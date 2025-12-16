package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import io.sustc.util.PasswordUtil;
import io.sustc.util.PermissionUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
    private JdbcTemplate jdbcTemplate;

    @Override
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
    public RecipeRecord getRecipeById(long recipeId) {
        // 1. 参数验证
        if (recipeId <= 0) {
            throw new IllegalArgumentException("Recipe ID must be positive");
        }

        try {
            // 2. 查询食谱基本信息
            String sql = "SELECT * FROM recipes WHERE RecipeId = ?";
            RecipeRecord recipe = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                return mapResultSetToRecipeRecord(rs);
            }, recipeId);

            if (recipe == null) {
                return null;
            }

            // 3. 查询并设置食材列表
            String[] ingredients = getRecipeIngredientsArray(recipeId);
            recipe.setRecipeIngredientParts(ingredients);

            return recipe;

        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            log.error("Error getting recipe by ID {}: {}", recipeId, e.getMessage(), e);
            throw new RuntimeException("Failed to get recipe by ID: " + e.getMessage(), e);
        }
    }

    // 辅助方法：将 ResultSet 映射到 RecipeRecord
    private RecipeRecord mapResultSetToRecipeRecord(ResultSet rs) throws SQLException {
        RecipeRecord recipe = new RecipeRecord();

        // 设置基本字段
        recipe.setRecipeId(rs.getLong("RecipeId"));
        recipe.setName(rs.getString("Name"));
        recipe.setAuthorId(rs.getLong("AuthorId"));
        recipe.setCookTime(rs.getString("CookTime"));
        recipe.setPrepTime(rs.getString("PrepTime"));
        recipe.setTotalTime(rs.getString("TotalTime"));
        recipe.setDatePublished(rs.getTimestamp("DatePublished"));
        recipe.setDescription(rs.getString("Description"));
        recipe.setRecipeCategory(rs.getString("RecipeCategory"));

        // 处理评分（可能为null）
        recipe.setAggregatedRating(getNullableFloat(rs, "AggregatedRating"));
        recipe.setReviewCount(rs.getInt("ReviewCount"));

        // 处理营养信息（可能为null）
        recipe.setCalories(getNullableFloat(rs, "Calories"));
        recipe.setFatContent(getNullableFloat(rs, "FatContent"));
        recipe.setSaturatedFatContent(getNullableFloat(rs, "SaturatedFatContent"));
        recipe.setCholesterolContent(getNullableFloat(rs, "CholesterolContent"));
        recipe.setSodiumContent(getNullableFloat(rs, "SodiumContent"));
        recipe.setCarbohydrateContent(getNullableFloat(rs, "CarbohydrateContent"));
        recipe.setFiberContent(getNullableFloat(rs, "FiberContent"));
        recipe.setSugarContent(getNullableFloat(rs, "SugarContent"));
        recipe.setProteinContent(getNullableFloat(rs, "ProteinContent"));

        recipe.setRecipeServings(rs.getInt("RecipeServings"));
        recipe.setRecipeYield(rs.getString("RecipeYield"));

        return recipe;
    }

    // 辅助方法：安全获取可能为null的float值
    private Float getNullableFloat(ResultSet rs, String columnName) throws SQLException {
        float value = rs.getFloat(columnName);
        return rs.wasNull() ? null : value;
    }

    // 辅助方法：获取食谱的食材数组
    private String[] getRecipeIngredientsArray(long recipeId) {
        String sql = "SELECT IngredientPart FROM recipe_ingredients WHERE RecipeId = ? ORDER BY IngredientPart";

        try {
            List<String> ingredientsList = jdbcTemplate.queryForList(sql, String.class, recipeId);
            return ingredientsList.toArray(new String[0]);
        } catch (Exception e) {
            log.debug("Error getting ingredients for recipe {}: {}", recipeId, e.getMessage());
            return new String[0];
        }
    }

    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating,
                                                  Integer page, Integer size, String sort) {
        // 参数验证和默认值设置
        int validPage = Math.max(1, page != null ? page : 1);
        int validSize = Math.max(1, Math.min(size != null ? size : 20, 200));

        // 构建查询条件和参数
        List<Object> params = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder(" WHERE 1=1");

        // 关键字搜索（在名称和描述中模糊匹配）
        if (keyword != null && !keyword.trim().isEmpty()) {
            whereClause.append(" AND (LOWER(Name) LIKE LOWER(?) OR LOWER(Description) LIKE LOWER(?))");
            String likeKeyword = "%" + keyword.trim() + "%";
            params.add(likeKeyword);
            params.add(likeKeyword);
        }

        // 分类过滤
        if (category != null && !category.trim().isEmpty()) {
            whereClause.append(" AND RecipeCategory = ?");
            params.add(category.trim());
        }

        // 最低评分过滤
        if (minRating != null && minRating > 0) {
            whereClause.append(" AND AggregatedRating >= ?");
            params.add(minRating);
        }

        // 构建排序子句
        String orderByClause = buildOrderByClause(sort);

        // 查询总记录数
        String countSql = "SELECT COUNT(*) FROM recipes" + whereClause.toString();
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

        // 构建分页查询
        String querySql = "SELECT * FROM recipes" + whereClause.toString() + " " + orderByClause + " LIMIT ? OFFSET ?";

        // 添加分页参数
        List<Object> queryParams = new ArrayList<>(params);
        queryParams.add(validSize);
        queryParams.add((validPage - 1) * validSize);

        // 执行查询
        List<RecipeRecord> recipes = jdbcTemplate.query(querySql, queryParams.toArray(), (rs, rowNum) -> {
            RecipeRecord recipe = mapResultSetToRecipeRecord(rs);

            // 获取并设置食材列表
            long recipeId = rs.getLong("RecipeId");
            String[] ingredients = getRecipeIngredientsArray(recipeId);
            recipe.setRecipeIngredientParts(ingredients);

            return recipe;
        });

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
            return "ORDER BY DatePublished DESC, RecipeId DESC";
        }

        switch (sort.toLowerCase()) {
            case "rating_desc":
                return "ORDER BY AggregatedRating DESC NULLS LAST, RecipeId DESC";
            case "date_desc":
                return "ORDER BY DatePublished DESC, RecipeId DESC";
            case "calories_asc":
                return "ORDER BY Calories ASC NULLS LAST, RecipeId ASC";
            default:
                log.warn("Invalid sort parameter '{}', using default 'date_desc'", sort);
                return "ORDER BY DatePublished DESC, RecipeId DESC";
        }
    }

    @Override
    @Transactional
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        // 1. 验证权限
        if (!validateAuthAndPermission(auth)) {
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

        // 验证分类（可选）
        if (dto.getRecipeCategory() != null && dto.getRecipeCategory().length() > 255) {
            throw new IllegalArgumentException("Recipe category is too long");
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
                dto.getAggregatedRating(),
                dto.getReviewCount(),
                dto.getCalories(),
                dto.getFatContent(),
                dto.getSaturatedFatContent(),
                dto.getCholesterolContent(),
                dto.getSodiumContent(),
                dto.getCarbohydrateContent(),
                dto.getFiberContent(),
                dto.getSugarContent(),
                dto.getProteinContent(),
                dto.getRecipeServings(),
                dto.getRecipeYield()
        );
    }

    // 辅助方法：插入食材信息
    private void insertRecipeIngredients(long recipeId, String[] ingredientParts) {
        String sql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?)";

        List<Object[]> batchArgs = new ArrayList<>();
        for (String ingredient : ingredientParts) {
            if (ingredient != null && !ingredient.trim().isEmpty()) {
                batchArgs.add(new Object[]{recipeId, ingredient.trim()});
            }
        }

        if (!batchArgs.isEmpty()) {
            jdbcTemplate.batchUpdate(sql, batchArgs);
        }
    }

    @Override
    @Transactional
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
        // 1. 验证权限
        if (!validateAuthAndPermission(auth)) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 2. 验证食谱存在且用户是作者
        validateRecipeOwnership(recipeId, auth.getAuthorId());

        // 3. 解析时间字符串
        Duration cookDuration = null;
        Duration prepDuration = null;

        if (cookTimeIso != null) {
            try {
                cookDuration = Duration.parse(cookTimeIso);
                if (cookDuration.isNegative()) {
                    throw new IllegalArgumentException("Cook time cannot be negative");
                }
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid cook time format. Expected ISO 8601 duration format", e);
            }
        }

        if (prepTimeIso != null) {
            try {
                prepDuration = Duration.parse(prepTimeIso);
                if (prepDuration.isNegative()) {
                    throw new IllegalArgumentException("Prep time cannot be negative");
                }
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid prep time format. Expected ISO 8601 duration format", e);
            }
        }

        // 4. 获取原始时间（如果新时间为null）
        String originalCookTime = null;
        String originalPrepTime = null;

        if (cookTimeIso == null || prepTimeIso == null) {
            String sql = "SELECT CookTime, PrepTime FROM recipes WHERE RecipeId = ?";
            Map<String, Object> result = jdbcTemplate.queryForMap(sql, recipeId);
            originalCookTime = (String) result.get("CookTime");
            originalPrepTime = (String) result.get("PrepTime");
        }

        // 5. 计算总时间
        Duration totalDuration = calculateTotalDuration(
                cookTimeIso != null ? cookDuration : parseDuration(originalCookTime),
                prepTimeIso != null ? prepDuration : parseDuration(originalPrepTime)
        );

        // 6. 更新数据库
        updateRecipeTimes(recipeId,
                cookTimeIso != null ? cookTimeIso : originalCookTime,
                prepTimeIso != null ? prepTimeIso : originalPrepTime,
                formatDuration(totalDuration));

        log.info("Recipe times updated: ID={}, CookTime={}, PrepTime={}, TotalTime={}",
                recipeId,
                cookTimeIso != null ? cookTimeIso : originalCookTime,
                prepTimeIso != null ? prepTimeIso : originalPrepTime,
                formatDuration(totalDuration));
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
            WITH ranked_recipes AS (
                SELECT 
                    RecipeId,
                    Calories,
                    LAG(RecipeId) OVER (ORDER BY Calories) AS prev_recipe_id,
                    LAG(Calories) OVER (ORDER BY Calories) AS prev_calories,
                    Calories - LAG(Calories) OVER (ORDER BY Calories) AS diff
                FROM recipes
                WHERE Calories IS NOT NULL
                ORDER BY Calories
            ),
            min_diff AS (
                SELECT 
                    RecipeId,
                    Calories,
                    prev_recipe_id,
                    prev_calories,
                    diff,
                    ROW_NUMBER() OVER (ORDER BY diff, prev_recipe_id, RecipeId) as rn
                FROM ranked_recipes
                WHERE diff IS NOT NULL
            )
            SELECT 
                prev_recipe_id AS RecipeA,
                RecipeId AS RecipeB,
                prev_calories AS CaloriesA,
                Calories AS CaloriesB,
                diff AS Difference
            FROM min_diff
            WHERE rn = 1
            """;

        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Map<String, Object> result = new HashMap<>();
                result.put("RecipeA", rs.getLong("RecipeA"));
                result.put("RecipeB", rs.getLong("RecipeB"));
                result.put("CaloriesA", rs.getDouble("CaloriesA"));
                result.put("CaloriesB", rs.getDouble("CaloriesB"));
                result.put("Difference", rs.getDouble("Difference"));
                return result;
            });
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<Map<String, Object>> getTop3MostComplexRecipesByIngredients() {
        String sql = """
            SELECT 
                r.RecipeId,
                r.Name,
                COUNT(ri.IngredientPart) AS IngredientCount
            FROM recipes r
            INNER JOIN recipe_ingredients ri ON r.RecipeId = ri.RecipeId
            GROUP BY r.RecipeId, r.Name
            ORDER BY IngredientCount DESC, r.RecipeId ASC
            LIMIT 3
            """;

        try {
            return jdbcTemplate.query(sql, (rs, rowNum) -> {
                Map<String, Object> result = new HashMap<>();
                result.put("RecipeId", rs.getLong("RecipeId"));
                result.put("Name", rs.getString("Name"));
                result.put("IngredientCount", rs.getInt("IngredientCount"));
                return result;
            });
        } catch (EmptyResultDataAccessException e) {
            return new ArrayList<>();
        }
    }

}