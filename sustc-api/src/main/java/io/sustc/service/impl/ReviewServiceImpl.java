package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.util.PermissionUtils;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

@Service
@Slf4j
public class ReviewServiceImpl implements ReviewService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PermissionUtils permissionUtils;

    @Autowired
    private UserService userService;


    @Override
    @Transactional
    @CacheEvict(value = "reviewLists", allEntries = true)
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        // 基础参数验证
        if (auth == null || rating < 1 || rating > 5 || review == null || review.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid parameters");
        }

        long userId = permissionUtils.validateUser(auth);
        if (userId == -1L) {
            throw new SecurityException("Invalid user");
        }

        if (!permissionUtils.recipeExists(recipeId)) {
            throw new IllegalArgumentException("Recipe does not exist");
        }

        long newReviewId = permissionUtils.generateNewId("reviews", "ReviewId");
        Timestamp now = Timestamp.from(Instant.now());
        
        jdbcTemplate.update(
            "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) VALUES (?, ?, ?, ?, ?, ?, ?)",
            newReviewId, recipeId, userId, rating, review.trim(), now, now);

        // 直接更新聚合评分，不需要返回完整记录
        jdbcTemplate.update(
            "UPDATE recipes SET AggregatedRating = (SELECT ROUND(AVG(Rating)::numeric, 2) FROM reviews WHERE RecipeId = ?), " +
            "ReviewCount = (SELECT COUNT(*) FROM reviews WHERE RecipeId = ?) WHERE RecipeId = ?",
            recipeId, recipeId, recipeId);

        return newReviewId;
    }


    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        if (auth == null || rating < 1 || rating > 5 || review == null || review.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid parameters");
        }

        // 先验证数据存在性，再验证权限
        if (!permissionUtils.reviewExists(reviewId)) {
            throw new IllegalArgumentException("Review does not exist");
        }

        if (!permissionUtils.reviewBelongsToRecipe(reviewId, recipeId)) {
            throw new IllegalArgumentException("Review does not belong to the specified recipe");
        }

        long userId = permissionUtils.validateUser(auth);
        if (userId == -1L) {
            throw new SecurityException("Invalid user");
        }

        if (!permissionUtils.isReviewAuthor(userId, reviewId)) {
            throw new SecurityException("Unauthorized to edit this review");
        }

        jdbcTemplate.update(
            "UPDATE reviews SET Rating = ?, Review = ?, DateModified = CURRENT_TIMESTAMP WHERE ReviewId = ?",
            rating, review.trim(), reviewId);

        // 直接更新聚合评分
        jdbcTemplate.update(
            "UPDATE recipes SET AggregatedRating = (SELECT ROUND(AVG(Rating)::numeric, 2) FROM reviews WHERE RecipeId = ?), " +
            "ReviewCount = (SELECT COUNT(*) FROM reviews WHERE RecipeId = ?) WHERE RecipeId = ?",
            recipeId, recipeId, recipeId);
    }


    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        if (auth == null || recipeId <= 0 || reviewId <= 0) {
            throw new IllegalArgumentException("Invalid parameters");
        }

        // 先验证数据存在性，再验证权限
        if (!permissionUtils.reviewExists(reviewId)) {
            throw new IllegalArgumentException("Review does not exist");
        }

        if (!permissionUtils.reviewBelongsToRecipe(reviewId, recipeId)) {
            throw new IllegalArgumentException("Review does not belong to the specified recipe");
        }

        long userId = permissionUtils.validateUser(auth);
        if (userId == -1L) {
            throw new SecurityException("Invalid user");
        }

        if (!permissionUtils.isReviewAuthor(userId, reviewId)) {
            throw new SecurityException("Unauthorized to delete this review");
        }

        // 删除评论的点赞和评论本身
        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId = ?", reviewId);
        jdbcTemplate.update("DELETE FROM reviews WHERE ReviewId = ?", reviewId);

        // 直接更新聚合评分
        jdbcTemplate.update(
            "UPDATE recipes SET AggregatedRating = (SELECT ROUND(AVG(Rating)::numeric, 2) FROM reviews WHERE RecipeId = ?), " +
            "ReviewCount = (SELECT COUNT(*) FROM reviews WHERE RecipeId = ?) WHERE RecipeId = ?",
            recipeId, recipeId, recipeId);
    }


    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        if (auth == null || reviewId <= 0) {
            throw new IllegalArgumentException("Invalid parameters");
        }

        long userId = permissionUtils.validateUser(auth);
        if (userId == -1L) {
            throw new SecurityException("Invalid user");
        }

        if (!permissionUtils.reviewExists(reviewId)) {
            throw new IllegalArgumentException("Review does not exist");
        }

        // 检查不能给自己的评论点赞
        String authorSql = "SELECT AuthorId FROM reviews WHERE ReviewId = ?";
        try {
            Long authorId = jdbcTemplate.queryForObject(authorSql, Long.class, reviewId);
            if (authorId != null && authorId.equals(userId)) {
                throw new SecurityException("Cannot like own review");
            }
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review does not exist");
        }

        // 检查是否已点赞，如果没有则插入
        if (!hasUserLikedReview(userId, reviewId)) {
            try {
                jdbcTemplate.update("INSERT INTO review_likes (AuthorId, ReviewId) VALUES (?, ?)", userId, reviewId);
            } catch (Exception e) {
                // 忽略重复键错误
                if (!e.getMessage().contains("duplicate key")) {
                    throw e;
                }
            }
        }

        return getReviewLikeCount(reviewId);
    }


    // 辅助方法：获取评论的点赞数
    private long getReviewLikeCount(long reviewId) {
        String sql = "SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?";
        try {
            Long count = jdbcTemplate.queryForObject(sql, Long.class, reviewId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        if (auth == null || reviewId <= 0) {
            throw new IllegalArgumentException("Invalid parameters");
        }

        long userId = permissionUtils.validateUser(auth);
        if (userId == -1L) {
            throw new SecurityException("Invalid user");
        }

        if (!permissionUtils.reviewExists(reviewId)) {
            throw new IllegalArgumentException("Review does not exist");
        }

        // 删除点赞记录
        jdbcTemplate.update("DELETE FROM review_likes WHERE AuthorId = ? AND ReviewId = ?", userId, reviewId);
        
        return getReviewLikeCount(reviewId);
    }

    // 工具方法：检查用户是否已经点赞
    private boolean hasUserLikedReview(long userId, long reviewId) {
        String sql = "SELECT COUNT(*) FROM review_likes WHERE AuthorId = ? AND ReviewId = ?";
        try {
            Long count = jdbcTemplate.queryForObject(sql, Long.class, userId, reviewId);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }



    @Cacheable(value = "reviewLists", key = "#recipeId + '_' + #page + '_' + #size + '_' + #sort")
    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
//        log.debug("Listing reviews for recipeId: {}, page: {}, size: {}, sort: {}",
//                recipeId, page, size, sort);

        try {
            // === 1. 参数验证 ===
            if (recipeId <= 0) {
                throw new IllegalArgumentException("Recipe ID must be positive");
            }

            if (page < 1) {
                throw new IllegalArgumentException("Page must be >= 1");
            }

            if (size <= 0) {
                throw new IllegalArgumentException("Size must be > 0");
            }

            // 确保size在合理范围内（1-200）
            int validSize = Math.max(1, Math.min(size, 200));
            int validPage = Math.max(1, page);
            int offset = (validPage - 1) * validSize;

            // === 2. 查询总记录数 ===
            String countSql = "SELECT COUNT(*) FROM reviews WHERE RecipeId = ?";
            Long total = jdbcTemplate.queryForObject(countSql, Long.class, recipeId);

            if (total == null || total == 0) {
                //log.debug("No reviews found for recipe {}", recipeId);
                return PageResult.<ReviewRecord>builder()
                        .items(new ArrayList<>())
                        .page(validPage)
                        .size(validSize)
                        .total(0L)
                        .build();
            }

            // === 3. 构建排序和查询语句 ===
            String orderByClause = buildOrderByClause(sort);
            String querySql = buildQuerySql(orderByClause);

            // === 4. 执行分页查询 ===
            List<ReviewRecord> reviews = executePaginatedQuery(
                    recipeId, querySql, validSize, offset
            );

            // === 5. 构建返回结果 ===
            PageResult<ReviewRecord> result = PageResult.<ReviewRecord>builder()
                    .items(reviews)
                    .page(validPage)
                    .size(validSize)
                    .total(total)
                    .build();

//            log.info("Found {} reviews for recipe {} (page {} of {})",
//                    total, recipeId, validPage, calculateTotalPages(total, validSize));

            return result;

        } catch (IllegalArgumentException e) {
            //log.warn("listByRecipe failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            //log.error("Unexpected error in listByRecipe: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to list reviews by recipe: " + e.getMessage(), e);
        }
    }

    // 辅助方法：构建排序子句
    private String buildOrderByClause(String sort) {
        if (sort == null) {
            return "ORDER BY r.DateModified DESC, r.ReviewId DESC";
        }

        switch (sort.toLowerCase()) {
            case "likes_desc":
                return "ORDER BY like_count DESC, r.DateModified DESC, r.ReviewId DESC";
            case "date_desc":
                return "ORDER BY r.DateModified DESC, r.ReviewId DESC";
            default:
                //log.warn("Invalid sort parameter '{}', using default 'date_desc'", sort);
                return "ORDER BY r.DateModified DESC, r.ReviewId DESC";
        }
    }

    // 辅助方法：构建优化的查询语句（使用视图）
    private String buildQuerySql(String orderByClause) {
        return "SELECT * FROM v_review_with_likes r " +
                "WHERE r.RecipeId = ? " +
                orderByClause + " " +
                "LIMIT ? OFFSET ?";
    }

    // 辅助方法：执行优化的分页查询（使用视图，消除N+1查询）
    private List<ReviewRecord> executePaginatedQuery(
            long recipeId, String sql, int limit, int offset
    ) {
        return jdbcTemplate.query(
                sql,
                new Object[]{recipeId, limit, offset},
                (rs, rowNum) -> {
                    ReviewRecord review = new ReviewRecord();

                    // 设置基本字段
                    review.setReviewId(rs.getLong("ReviewId"));
                    review.setRecipeId(rs.getLong("RecipeId"));
                    review.setAuthorId(rs.getLong("AuthorId"));
                    review.setAuthorName(rs.getString("AuthorName"));
                    review.setRating(rs.getInt("Rating"));
                    review.setReview(rs.getString("Review"));

                    // 设置时间戳
                    Timestamp dateSubmitted = rs.getTimestamp("DateSubmitted");
                    Timestamp dateModified = rs.getTimestamp("DateModified");

                    if (dateSubmitted != null) {
                        review.setDateSubmitted(dateSubmitted);
                    }

                    if (dateModified != null) {
                        review.setDateModified(dateModified);
                    }

                    // 从视图中直接获取点赞数组（已排序），消除N+1查询问题
                    Array likesArray = rs.getArray("likes_array");
                    if (likesArray != null) {
                        Long[] likeObjects = (Long[]) likesArray.getArray();
                        long[] likes = new long[likeObjects.length];
                        for (int i = 0; i < likeObjects.length; i++) {
                            likes[i] = likeObjects[i];
                        }
                        review.setLikes(likes);
                    } else {
                        review.setLikes(new long[0]);
                    }

                    return review;
                }
        );
    }

    // 保留原方法以兼容其他地方的调用（已由视图优化）
    private long[] getLikesForReview(long reviewId) {
        String sql = "SELECT AuthorId FROM review_likes WHERE ReviewId = ? ORDER BY AuthorId";

        try {
            List<Long> likesList = jdbcTemplate.queryForList(sql, Long.class, reviewId);

            if (likesList == null || likesList.isEmpty()) {
                return new long[0];
            }

            // 转换为 long[]
            long[] likesArray = new long[likesList.size()];
            for (int i = 0; i < likesList.size(); i++) {
                likesArray[i] = likesList.get(i);
            }
            return likesArray;
        } catch (Exception e) {
            //log.debug("Error getting likes for review {}: {}", reviewId, e.getMessage());
            return new long[0];
        }
    }

    // 辅助方法：计算总页数
    private int calculateTotalPages(long totalRecords, int pageSize) {
        return (int) Math.ceil((double) totalRecords / pageSize);
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        if (recipeId <= 0) {
            throw new IllegalArgumentException("Invalid recipe ID");
        }

        if (!permissionUtils.recipeExists(recipeId)) {
            throw new IllegalArgumentException("Recipe does not exist");
        }

        // 直接更新聚合评分
        jdbcTemplate.update(
            "UPDATE recipes SET AggregatedRating = (SELECT ROUND(AVG(Rating)::numeric, 2) FROM reviews WHERE RecipeId = ?), " +
            "ReviewCount = (SELECT COUNT(*) FROM reviews WHERE RecipeId = ?) WHERE RecipeId = ?",
            recipeId, recipeId, recipeId);

        // 获取并返回更新后的食谱记录
        return getUpdatedRecipeRecord(recipeId);
    }


    // 辅助方法：获取更新后的食谱记录
    private RecipeRecord getUpdatedRecipeRecord(long recipeId) {
        String sql = "SELECT * FROM recipes WHERE RecipeId = ?";

        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                RecipeRecord recipe = new RecipeRecord();
                recipe.setRecipeId(rs.getLong("RecipeId"));
                recipe.setName(rs.getString("Name"));
                recipe.setAuthorId(rs.getLong("AuthorId"));
                recipe.setCookTime(rs.getString("CookTime"));
                recipe.setPrepTime(rs.getString("PrepTime"));
                recipe.setTotalTime(rs.getString("TotalTime"));
                recipe.setDatePublished(rs.getTimestamp("DatePublished"));
                recipe.setDescription(rs.getString("Description"));
                recipe.setRecipeCategory(rs.getString("RecipeCategory"));

                // 处理可能为null的评分 - 修复这里！
                float aggregatedRating = rs.getFloat("AggregatedRating");
                if (rs.wasNull()) {
                    recipe.setAggregatedRating(0.0f);  // 如果为null，设置为0.0f
                } else {
                    recipe.setAggregatedRating(aggregatedRating);
                }

                recipe.setReviewCount(rs.getInt("ReviewCount"));
                recipe.setCalories(rs.getFloat("Calories"));
                recipe.setFatContent(rs.getFloat("FatContent"));
                recipe.setSaturatedFatContent(rs.getFloat("SaturatedFatContent"));
                recipe.setCholesterolContent(rs.getFloat("CholesterolContent"));
                recipe.setSodiumContent(rs.getFloat("SodiumContent"));
                recipe.setCarbohydrateContent(rs.getFloat("CarbohydrateContent"));
                recipe.setFiberContent(rs.getFloat("FiberContent"));
                recipe.setSugarContent(rs.getFloat("SugarContent"));
                recipe.setProteinContent(rs.getFloat("ProteinContent"));
                recipe.setRecipeServings(rs.getInt("RecipeServings"));
                recipe.setRecipeYield(rs.getString("RecipeYield"));

                // 获取食材列表
                List<String> ingredients = getRecipeIngredients(recipeId);
                recipe.setRecipeIngredientParts(ingredients.toArray(new String[0]));

                return recipe;
            }, recipeId);

        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe with ID " + recipeId + " does not exist");
        }
    }

    // 辅助方法：获取食谱的食材列表
    private List<String> getRecipeIngredients(long recipeId) {
        String sql = "SELECT IngredientPart FROM recipe_ingredients WHERE RecipeId = ? ORDER BY IngredientPart";

        try {
            return jdbcTemplate.queryForList(sql, String.class, recipeId);
        } catch (Exception e) {
            //log.warn("Failed to get ingredients for recipe {}: {}", recipeId, e.getMessage());
            return new ArrayList<>();
        }
    }


}
