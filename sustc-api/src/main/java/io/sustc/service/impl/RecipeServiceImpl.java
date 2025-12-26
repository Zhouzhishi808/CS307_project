package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import io.sustc.util.PasswordUtil;
import io.sustc.util.PermissionUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;

@Service
@Slf4j
public class RecipeServiceImpl implements RecipeService {
    @Autowired
    private PermissionUtils permissionUtils;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Cacheable(value = "recipeNames", key = "#id")
    public String getNameFromID(long id) {
        if (id <= 0) {
            return null;
        }

        try {
            String sql = "SELECT Name FROM recipes WHERE RecipeId = ?";
            return jdbcTemplate.queryForObject(sql, String.class, id);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            log.error("Error getting recipe name by ID {}: {}", id, e.getMessage(), e);
            return null;
        }
    }

    @Override
    @Cacheable(value = "recipes", key = "#recipeId")
    public RecipeRecord getRecipeById(long recipeId) {
        // 1. 参数验证
        if (recipeId <= 0) {
            throw new IllegalArgumentException("Recipe ID must be positive");
        }

        try {
            // 2. 使用优化视图，一次查询获取所有数据包括食材，消除N+1查询
            String sql = "SELECT r.*, u.AuthorName FROM v_recipe_full_info r " +
                    "LEFT JOIN users u ON r.AuthorId = u.AuthorId " +
                    "WHERE r.RecipeId = ?";
            RecipeRecord recipe = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                return mapResultSetToRecipeRecordFromView(rs);
            }, recipeId);

            return recipe;

        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            log.error("Error getting recipe by ID {}: {}", recipeId, e.getMessage(), e);
            throw new RuntimeException("Failed to get recipe by ID: " + e.getMessage(), e);
        }
    }

    // 重写 mapResultSetToRecipeRecord 方法以处理所有字段
    private RecipeRecord mapResultSetToRecipeRecord(ResultSet rs) throws SQLException {
        return mapResultSetToRecipeRecord(rs, true);
    }

    // 重载方法，可以控制是否查询食材
    private RecipeRecord mapResultSetToRecipeRecord(ResultSet rs, boolean fetchIngredients) throws SQLException {
        RecipeRecord recipe = new RecipeRecord();

        // 设置基本字段
        recipe.setRecipeId(rs.getLong("RecipeId"));
        recipe.setName(rs.getString("Name"));
        recipe.setAuthorId(rs.getLong("AuthorId"));

        // 尝试获取 AuthorName（如果查询中包含了 JOIN users）
        try {
            recipe.setAuthorName(rs.getString("AuthorName"));
        } catch (SQLException e) {
            // 如果查询中没有 AuthorName 字段，设置为 null
            recipe.setAuthorName(null);
        }

        recipe.setCookTime(rs.getString("CookTime"));
        recipe.setPrepTime(rs.getString("PrepTime"));
        recipe.setTotalTime(rs.getString("TotalTime"));
        recipe.setDatePublished(rs.getTimestamp("DatePublished"));
        recipe.setDescription(rs.getString("Description"));
        recipe.setRecipeCategory(rs.getString("RecipeCategory"));

        // 处理评分（可能为null）
        Float aggregatedRating = getNullableFloat(rs, "AggregatedRating");
        recipe.setAggregatedRating(aggregatedRating != null ? aggregatedRating : 0.0f);
        recipe.setReviewCount(rs.getInt("ReviewCount"));

        // 处理营养信息（可能为null）
        recipe.setCalories(getSafeFloat(rs, "Calories"));
        recipe.setFatContent(getSafeFloat(rs, "FatContent"));
        recipe.setSaturatedFatContent(getSafeFloat(rs, "SaturatedFatContent"));
        recipe.setCholesterolContent(getSafeFloat(rs, "CholesterolContent"));
        recipe.setSodiumContent(getSafeFloat(rs, "SodiumContent"));
        recipe.setCarbohydrateContent(getSafeFloat(rs, "CarbohydrateContent"));
        recipe.setFiberContent(getSafeFloat(rs, "FiberContent"));
        recipe.setSugarContent(getSafeFloat(rs, "SugarContent"));
        recipe.setProteinContent(getSafeFloat(rs, "ProteinContent"));

        // 处理servings（存储为VARCHAR，需要转换为int）
        String servingsStr = rs.getString("RecipeServings");
        int servings = 0;
        if (servingsStr != null && !servingsStr.trim().isEmpty()) {
            try {
                servings = Integer.parseInt(servingsStr.trim());
            } catch (NumberFormatException e) {
                servings = 0;
            }
        }
        recipe.setRecipeServings(servings);

        recipe.setRecipeYield(rs.getString("RecipeYield"));

        // 如果需要，获取并设置食材列表
        if (fetchIngredients) {
            long recipeId = rs.getLong("RecipeId");
            String[] ingredients = getRecipeIngredientsArray(recipeId);
            recipe.setRecipeIngredientParts(ingredients);
        }

        return recipe;
    }

    // 辅助方法：安全获取可能为null的float值
    // 辅助方法：安全获取可能为null的Float值（包装类型）
    private Float getNullableFloat(ResultSet rs, String columnName) throws SQLException {
        float value = rs.getFloat(columnName);
        return rs.wasNull() ? null : value;
    }

    // 辅助方法：安全获取可能为null的float值，转换为原始float类型（不可为null）
    private float getSafeFloat(ResultSet rs, String columnName) throws SQLException {
        Float value = getNullableFloat(rs, columnName);
        return value != null ? value : 0.0f;
    }

    // 辅助方法：安全获取可能为null的Integer值
    private Integer getNullableInt(ResultSet rs, String columnName) throws SQLException {
        int value = rs.getInt(columnName);
        return rs.wasNull() ? null : value;
    }

    // 辅助方法：安全获取可能为null的String值
    private String getNullableString(ResultSet rs, String columnName) throws SQLException {
        String value = rs.getString(columnName);
        return rs.wasNull() ? null : value;
    }

    // 辅助方法：从视图中映射食谱记录（已包含食材数组）
    private RecipeRecord mapResultSetToRecipeRecordFromView(ResultSet rs) throws SQLException {
        RecipeRecord recipe = new RecipeRecord();

        // 设置基本字段
        recipe.setRecipeId(rs.getLong("RecipeId"));
        recipe.setName(rs.getString("Name"));
        recipe.setAuthorId(rs.getLong("AuthorId"));
        recipe.setAuthorName(rs.getString("AuthorName"));

        recipe.setCookTime(rs.getString("CookTime"));
        recipe.setPrepTime(rs.getString("PrepTime"));
        recipe.setTotalTime(rs.getString("TotalTime"));
        recipe.setDatePublished(rs.getTimestamp("DatePublished"));
        recipe.setDescription(rs.getString("Description"));
        recipe.setRecipeCategory(rs.getString("RecipeCategory"));

        // 处理评分（可能为null）
        Float aggregatedRating = getNullableFloat(rs, "AggregatedRating");
        recipe.setAggregatedRating(aggregatedRating != null ? aggregatedRating : 0.0f);
        recipe.setReviewCount(rs.getInt("ReviewCount"));

        // 处理营养信息（可能为null）
        recipe.setCalories(getSafeFloat(rs, "Calories"));
        recipe.setFatContent(getSafeFloat(rs, "FatContent"));
        recipe.setSaturatedFatContent(getSafeFloat(rs, "SaturatedFatContent"));
        recipe.setCholesterolContent(getSafeFloat(rs, "CholesterolContent"));
        recipe.setSodiumContent(getSafeFloat(rs, "SodiumContent"));
        recipe.setCarbohydrateContent(getSafeFloat(rs, "CarbohydrateContent"));
        recipe.setFiberContent(getSafeFloat(rs, "FiberContent"));
        recipe.setSugarContent(getSafeFloat(rs, "SugarContent"));
        recipe.setProteinContent(getSafeFloat(rs, "ProteinContent"));

        // 处理servings（存储为VARCHAR，需要转换为int）
        String servingsStr = rs.getString("RecipeServings");
        int servings = 0;
        if (servingsStr != null && !servingsStr.trim().isEmpty()) {
            try {
                servings = Integer.parseInt(servingsStr.trim());
            } catch (NumberFormatException e) {
                servings = 0;
            }
        }
        recipe.setRecipeServings(servings);

        recipe.setRecipeYield(rs.getString("RecipeYield"));

        // 从视图中直接获取食材数组（已排序）
        Array ingredientArray = rs.getArray("RecipeIngredientParts");
        if (ingredientArray != null) {
            String[] ingredients = (String[]) ingredientArray.getArray();
            recipe.setRecipeIngredientParts(ingredients);
        } else {
            recipe.setRecipeIngredientParts(new String[0]);
        }

        return recipe;
    }
    
    // 保留原方法以兼容其他地方的调用
    private String[] getRecipeIngredientsArray(long recipeId) {
        String sql = "SELECT IngredientPart FROM recipe_ingredients WHERE RecipeId = ? ORDER BY LOWER(IngredientPart) COLLATE \"C\"";

        try {
            List<String> ingredientsList = jdbcTemplate.queryForList(sql, String.class, recipeId);
            if (ingredientsList == null) {
                return new String[0];
            }
            return ingredientsList.toArray(new String[0]);
        } catch (Exception e) {
            log.debug("Error getting ingredients for recipe {}: {}", recipeId, e.getMessage());
            return new String[0];
        }
    }

    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating, Integer page, Integer size, String sort) {
        // 验证分页参数
        if (page == null || page < 1) {
            throw new IllegalArgumentException("Page must be >= 1");
        }
        if (size == null || size <= 0) {
            throw new IllegalArgumentException("Size must be > 0");
        }

        int validPage = page;
        int validSize = size;

        // 构建查询条件和参数
        List<Object> params = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder(" WHERE 1=1");

        // 关键字搜索（在名称和描述中模糊匹配）
        if (keyword != null && !keyword.trim().isEmpty()) {
            // 使用ILIKE实现大小写不敏感的模糊匹配（PostgreSQL特性）
            whereClause.append(" AND (r.Name ILIKE ? OR r.Description ILIKE ?)");
            String likeKeyword = "%" + keyword.trim() + "%";
            params.add(likeKeyword);
            params.add(likeKeyword);
        }

        // 分类过滤
        if (category != null && !category.trim().isEmpty()) {
            whereClause.append(" AND r.RecipeCategory = ?");
            params.add(category.trim());
        }

        // 最低评分过滤
        if (minRating != null) {
            whereClause.append(" AND r.AggregatedRating IS NOT NULL AND r.AggregatedRating >= ?");
            params.add(minRating);
        }

        // 构建排序子句
        String orderByClause = buildOrderByClause(sort);

        // 查询总记录数（使用视图以保持一致性）
        String countSql = "SELECT COUNT(*) FROM v_recipe_full_info r" + whereClause.toString();
        Long total = jdbcTemplate.queryForObject(countSql, Long.class, params.toArray());
        if (total == null) total = 0L;

        // 如果总记录数为0，直接返回空结果
        if (total == 0) {
            return PageResult.<RecipeRecord>builder()
                    .items(new ArrayList<>())
                    .page(validPage)
                    .size(validSize)
                    .total(0L)
                    .build();
        }

        // 添加分页参数
        List<Object> queryParams = new ArrayList<>(params);
        queryParams.add(validSize);
        queryParams.add((validPage - 1) * validSize);

        // 执行查询 - 使用优化的视图查询
        String optimizedQuerySql = "SELECT r.*, u.AuthorName FROM v_recipe_full_info r " +
                "LEFT JOIN users u ON r.AuthorId = u.AuthorId " +
                whereClause.toString() +
                " " + orderByClause + " LIMIT ? OFFSET ?";
        
        List<RecipeRecord> recipes = jdbcTemplate.query(optimizedQuerySql, queryParams.toArray(), (rs, rowNum) ->
            mapResultSetToRecipeRecordFromView(rs)  // 使用视图版本的映射方法
        );

        return PageResult.<RecipeRecord>builder()
                .items(recipes)
                .page(validPage)
                .size(validSize)
                .total(total)
                .build();
    }

    // 辅助方法：构建排序子句
    private String buildOrderByClause(String sort) {
        if (sort == null) {
            return "ORDER BY r.DatePublished DESC, r.RecipeId DESC";
        }

        switch (sort.toLowerCase()) {
            case "rating_desc":
                return "ORDER BY r.AggregatedRating DESC NULLS LAST, r.RecipeId DESC";
            case "date_desc":
                return "ORDER BY r.DatePublished DESC, r.RecipeId DESC";
            case "date_asc":
                return "ORDER BY r.DatePublished ASC, r.RecipeId DESC";
            case "calories_asc":
                return "ORDER BY r.Calories ASC NULLS LAST, r.RecipeId DESC";
            case "calories_desc":
                return "ORDER BY r.Calories DESC NULLS LAST, r.RecipeId DESC";
            case "id_desc":
                return "ORDER BY r.RecipeId DESC";
            case "id_asc":
                return "ORDER BY r.RecipeId ASC";
            default:
                log.warn("Invalid sort parameter '{}', using default 'date_desc'", sort);
                return "ORDER BY r.DatePublished DESC, r.RecipeId DESC";
        }
    }

    @Override
    @Transactional
    @CacheEvict(value = {"recipes", "recipeNames", "recipeSearch"}, allEntries = true)
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        // 1. 验证权限
        if (auth == null) {
            throw new SecurityException("Invalid or inactive user");
        }
        if (!validateAuthAndPermission(auth)) {
            throw new SecurityException("Invalid or inactive user");
        }
        if(permissionUtils.validateUser(auth)==-1L){
            throw new SecurityException("Invalid or inactive user");
        }
        // 2. 验证食谱数据
        validateRecipeData(dto);

        // 3. 生成新的RecipeId
        long newRecipeId = generateNewRecipeId();

        // 4. 插入食谱基本信息
        insertRecipe(newRecipeId, dto, auth.getAuthorId());

        // 5. 插入食材信息（如果存在）
        if (dto.getRecipeIngredientParts() != null && dto.getRecipeIngredientParts().length > 0) {
            insertRecipeIngredients(newRecipeId, dto.getRecipeIngredientParts());
        }

        log.info("Recipe created successfully: ID={}, Name={}, Author={}",
                newRecipeId, dto.getName(), auth.getAuthorId());

        return newRecipeId;
    }

    // 辅助方法：验证用户权限
    private boolean validateAuthAndPermission(AuthInfo auth) {
        if (auth == null || auth.getAuthorId() <= 0) {
            return false;
        }

        String sql = "SELECT COUNT(*) FROM users WHERE AuthorId = ? AND IsDeleted = false";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, auth.getAuthorId());
            return count != null && count > 0;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    // 辅助方法：验证食谱数据
    private void validateRecipeData(RecipeRecord dto) {
        if (dto == null) {
            throw new IllegalArgumentException("Recipe data cannot be null");
        }

        if (dto.getName() == null || dto.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("Recipe name cannot be null or empty");
        }
    }

    // 辅助方法：生成新的RecipeId
    private long generateNewRecipeId() {
        String sql = "SELECT COALESCE(MAX(RecipeId), 0) FROM recipes";
        try {
            Long maxId = jdbcTemplate.queryForObject(sql, Long.class);
            return (maxId != null ? maxId : 0) + 1;
        } catch (Exception e) {
            return 1L;
        }
    }

    // 辅助方法：插入食谱基本信息
    private void insertRecipe(long recipeId, RecipeRecord dto, long authorId) {
        String sql = "INSERT INTO recipes (RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, " +
                "DatePublished, Description, RecipeCategory, AggregatedRating, ReviewCount, " +
                "Calories, FatContent, SaturatedFatContent, CholesterolContent, " +
                "SodiumContent, CarbohydrateContent, FiberContent, SugarContent, " +
                "ProteinContent, RecipeServings, RecipeYield) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        // 处理AggregatedRating：如果 > 0 使用该值，否则设置为null（参考DatabaseServiceImpl的逻辑）
        Float aggregatedRating = null;
        if (dto.getAggregatedRating() > 0) {
            aggregatedRating = dto.getAggregatedRating();
        }

        // 处理营养字段：如果值 <= 0，设置为null（与DatabaseServiceImpl的导入逻辑保持一致）
        Float calories = dto.getCalories() > 0 ? dto.getCalories() : null;
        Float fatContent = dto.getFatContent() > 0 ? dto.getFatContent() : null;
        Float saturatedFatContent = dto.getSaturatedFatContent() > 0 ? dto.getSaturatedFatContent() : null;
        Float cholesterolContent = dto.getCholesterolContent() > 0 ? dto.getCholesterolContent() : null;
        Float sodiumContent = dto.getSodiumContent() > 0 ? dto.getSodiumContent() : null;
        Float carbohydrateContent = dto.getCarbohydrateContent() > 0 ? dto.getCarbohydrateContent() : null;
        Float fiberContent = dto.getFiberContent() > 0 ? dto.getFiberContent() : null;
        Float sugarContent = dto.getSugarContent() > 0 ? dto.getSugarContent() : null;
        Float proteinContent = dto.getProteinContent() > 0 ? dto.getProteinContent() : null;

        jdbcTemplate.update(sql,
                recipeId,
                dto.getName(),
                authorId,
                dto.getCookTime(),
                dto.getPrepTime(),
                dto.getTotalTime(),
                dto.getDatePublished(),
                dto.getDescription(),
                dto.getRecipeCategory(),
                aggregatedRating,  // 使用处理后的值
                dto.getReviewCount(),
                calories,
                fatContent,
                saturatedFatContent,
                cholesterolContent,
                sodiumContent,
                carbohydrateContent,
                fiberContent,
                sugarContent,
                proteinContent,
                dto.getRecipeServings() > 0 ? String.valueOf(dto.getRecipeServings()) : null,
                dto.getRecipeYield()
        );
    }

    // 辅助方法：插入食材信息
    private void insertRecipeIngredients(long recipeId, String[] ingredientParts) {
        String sql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?)";

        // 使用Set去重，避免违反主键约束
        // 注意：不要trim()，保留原始的空格
        Set<String> uniqueIngredients = new HashSet<>();
        for (String ingredient : ingredientParts) {
            if (ingredient != null && !ingredient.isEmpty()) {
                uniqueIngredients.add(ingredient);
            }
        }

        List<Object[]> batchArgs = new ArrayList<>();
        for (String ingredient : uniqueIngredients) {
            batchArgs.add(new Object[]{recipeId, ingredient});
        }

        if (!batchArgs.isEmpty()) {
            try {
                jdbcTemplate.batchUpdate(sql, batchArgs);
            } catch (Exception e) {
                log.error("Error inserting recipe ingredients for recipe {}: {}", recipeId, e.getMessage());
                throw new RuntimeException("Failed to insert recipe ingredients", e);
            }
        }
    }

    @Override
    @Transactional
    @CacheEvict(value = {"recipes", "recipeNames", "recipeSearch"}, allEntries = true)
    public void deleteRecipe(long recipeId, AuthInfo auth) {
        // 1. 验证权限
        if (!validateAuthAndPermission(auth)) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 2. 验证食谱存在且用户是作者
        validateRecipeOwnership(recipeId, auth.getAuthorId());

        // 3. 删除相关数据
        // 3.1 删除评论点赞
        deleteReviewLikesForRecipe(recipeId);

        // 3.2 删除评论
        deleteReviewsForRecipe(recipeId);

        // 3.3 删除食材
        deleteRecipeIngredients(recipeId);

        // 3.4 删除食谱
        deleteRecipeRecord(recipeId);

        log.info("Recipe deleted successfully: ID={}, Author={}", recipeId, auth.getAuthorId());
    }

    // 辅助方法：验证食谱所有权
    private void validateRecipeOwnership(long recipeId, long userId) {
        String sql = "SELECT AuthorId FROM recipes WHERE RecipeId = ?";
        try {
            Long authorId = jdbcTemplate.queryForObject(sql, Long.class, recipeId);
            if (authorId == null) {
                throw new IllegalArgumentException("Recipe does not exist");
            }
            if (authorId != userId) {
                throw new SecurityException("User is not the author of this recipe");
            }
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe does not exist");
        }
    }

    // 辅助方法：删除食谱的评论点赞
    private void deleteReviewLikesForRecipe(long recipeId) {
        String sql = "DELETE FROM review_likes WHERE ReviewId IN (SELECT ReviewId FROM reviews WHERE RecipeId = ?)";
        jdbcTemplate.update(sql, recipeId);
    }

    // 辅助方法：删除食谱的评论
    private void deleteReviewsForRecipe(long recipeId) {
        String sql = "DELETE FROM reviews WHERE RecipeId = ?";
        jdbcTemplate.update(sql, recipeId);
    }

    // 辅助方法：删除食谱的食材
    private void deleteRecipeIngredients(long recipeId) {
        String sql = "DELETE FROM recipe_ingredients WHERE RecipeId = ?";
        jdbcTemplate.update(sql, recipeId);
    }

    // 辅助方法：删除食谱记录
    private void deleteRecipeRecord(long recipeId) {
        String sql = "DELETE FROM recipes WHERE RecipeId = ?";
        jdbcTemplate.update(sql, recipeId);
    }

    @Override
    @Transactional
    public void updateTimes(AuthInfo auth, long recipeId, String cookTimeIso, String prepTimeIso) {
        // Verify authentication and permission
        if (!validateAuthAndPermission(auth)) {
            throw new SecurityException("Invalid or inactive user");
        }
        if(permissionUtils.validateUser(auth)==-1L){
            throw new SecurityException("Invalid or inactive user");
        }

        // Verify recipe ownership
        validateRecipeOwnership(recipeId, auth.getAuthorId());

        // Parse and validate durations
        Duration cookDuration = null;
        Duration prepDuration = null;

        if (cookTimeIso != null) {
            try {
                cookDuration = Duration.parse(cookTimeIso);
                if (cookDuration.isNegative()) {
                    throw new IllegalArgumentException("Negative duration");
                }
                // Check for overflow (Duration can be very large, but we need to ensure it's reasonable)
                if (cookDuration.getSeconds() > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Duration overflow");
                }
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid ISO 8601 duration format");
            }
        }

        if (prepTimeIso != null) {
            try {
                prepDuration = Duration.parse(prepTimeIso);
                if (prepDuration.isNegative()) {
                    throw new IllegalArgumentException("Negative duration");
                }
                // Check for overflow
                if (prepDuration.getSeconds() > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Duration overflow");
                }
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid ISO 8601 duration format");
            }
        }

        // Get existing times if needed
        String currentCookTime = null;
        String currentPrepTime = null;
        
        if (cookTimeIso == null || prepTimeIso == null) {
            String sql = "SELECT CookTime, PrepTime FROM recipes WHERE RecipeId = ?";
            try {
                Map<String, Object> result = jdbcTemplate.queryForMap(sql, recipeId);
                currentCookTime = (String) result.get("cooktime");
                currentPrepTime = (String) result.get("preptime");
            } catch (EmptyResultDataAccessException e) {
                // Recipe doesn't exist, but this should have been caught by validateRecipeOwnership
                throw new IllegalArgumentException("Recipe not found");
            }
        }

        // Calculate total time
        String finalCookTime = cookTimeIso != null ? cookTimeIso : currentCookTime;
        String finalPrepTime = prepTimeIso != null ? prepTimeIso : currentPrepTime;
        
        Duration totalDuration = Duration.ZERO;
        if (finalCookTime != null) {
            try {
                totalDuration = totalDuration.plus(Duration.parse(finalCookTime));
            } catch (DateTimeParseException e) {
                // Existing data might be invalid, treat as zero
            }
        }
        if (finalPrepTime != null) {
            try {
                totalDuration = totalDuration.plus(Duration.parse(finalPrepTime));
            } catch (DateTimeParseException e) {
                // Existing data might be invalid, treat as zero
            }
        }

        // Update database
        String updateSql = "UPDATE recipes SET CookTime = ?, PrepTime = ?, TotalTime = ? WHERE RecipeId = ?";
        jdbcTemplate.update(updateSql, finalCookTime, finalPrepTime, totalDuration.toString(), recipeId);
    }

    // 辅助方法：解析时间字符串
    private Duration parseDuration(String timeIso) {
        if (timeIso == null) {
            return Duration.ZERO;
        }
        try {
            return Duration.parse(timeIso);
        } catch (DateTimeParseException e) {
            return Duration.ZERO;
        }
    }

    // 辅助方法：计算总时间
    private Duration calculateTotalDuration(Duration cookDuration, Duration prepDuration) {
        if (cookDuration == null) cookDuration = Duration.ZERO;
        if (prepDuration == null) prepDuration = Duration.ZERO;
        return cookDuration.plus(prepDuration);
    }

    // 辅助方法：格式化时间为ISO 8601
    private String formatDuration(Duration duration) {
        if (duration == null || duration.isZero()) {
            return null;
        }
        return duration.toString();
    }

    // 辅助方法：更新食谱时间
    private void updateRecipeTimes(long recipeId, String cookTime, String prepTime, String totalTime) {
        String sql = "UPDATE recipes SET CookTime = ?, PrepTime = ?, TotalTime = ? WHERE RecipeId = ?";
        jdbcTemplate.update(sql, cookTime, prepTime, totalTime, recipeId);
    }

    @Override
    public Map<String, Object> getClosestCaloriePair() {
        String sql = """
            SELECT r1.RecipeId AS RecipeA, r2.RecipeId AS RecipeB,
                   r1.Calories AS CaloriesA, r2.Calories AS CaloriesB,
                   ABS(r1.Calories - r2.Calories) AS Difference
            FROM recipes r1
            JOIN recipes r2 ON r1.RecipeId < r2.RecipeId
            WHERE r1.Calories IS NOT NULL AND r2.Calories IS NOT NULL
            ORDER BY Difference ASC, r1.RecipeId ASC, r2.RecipeId ASC
            LIMIT 1
            """;

        try {
            return jdbcTemplate.query(sql, rs -> {
                if (!rs.next()) {
                    return null;
                }
                Map<String, Object> result = new HashMap<>();
                result.put("RecipeA", rs.getLong("RecipeA"));
                result.put("RecipeB", rs.getLong("RecipeB"));
                result.put("CaloriesA", rs.getDouble("CaloriesA"));
                result.put("CaloriesB", rs.getDouble("CaloriesB"));
                result.put("Difference", rs.getDouble("Difference"));
                return result;
            });
        } catch (Exception e) {
            log.error("Error computing closest calorie pair: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public List<Map<String, Object>> getTop3MostComplexRecipesByIngredients() {
        String sql = """
            SELECT r.RecipeId, r.Name, COUNT(ri.IngredientPart) AS IngredientCount
            FROM recipe_ingredients ri
            JOIN recipes r ON r.RecipeId = ri.RecipeId
            GROUP BY r.RecipeId, r.Name
            ORDER BY IngredientCount DESC, r.RecipeId ASC
            LIMIT 3
            """;

        return jdbcTemplate.query(sql, (rs, i) -> {
            Map<String, Object> row = new HashMap<>();
            row.put("RecipeId", rs.getLong("RecipeId"));
            row.put("Name", rs.getString("Name"));
            row.put("IngredientCount", rs.getInt("IngredientCount"));
            return row;
        });
    }

}
