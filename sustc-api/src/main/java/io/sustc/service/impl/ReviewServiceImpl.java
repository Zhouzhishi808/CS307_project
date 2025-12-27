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
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        try {
            validateAddReviewParameters(auth, rating, review);
            long userId = permissionUtils.validateUser(auth);
            if (userId == -1L) {
                throw new SecurityException("Invalid or inactive user");
            }
            
            // 小数据集优化：使用数据库序列生成ID，避免额外查询
            String insertSql = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                    "VALUES (COALESCE((SELECT MAX(ReviewId) FROM reviews), 0) + 1, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) " +
                    "RETURNING ReviewId";
            
            try {
                Long newReviewId = jdbcTemplate.queryForObject(insertSql, Long.class, 
                        recipeId, userId, rating, review.trim());
                
                if (newReviewId == null) {
                    throw new RuntimeException("Failed to insert review - no ID returned");
                }
                
                // 小数据集优化：合并rating更新到单个SQL操作
                updateRecipeRatingOptimized(recipeId);
                return newReviewId;
                
            } catch (org.springframework.dao.DataIntegrityViolationException e) {
                if (e.getMessage() != null && e.getMessage().toLowerCase().contains("foreign key")) {
                    throw new IllegalArgumentException("Recipe does not exist");
                }
                throw new RuntimeException("Database constraint violation: " + e.getMessage(), e);
            }
        } catch (SecurityException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to add review: " + e.getMessage(), e);
        }
    }

    private void validateAddReviewParameters(AuthInfo auth, int rating, String review) {
        if (auth == null || auth.getAuthorId() <= 0) {
            throw new IllegalArgumentException("Authentication info is required");
        }
        if (auth.getPassword() == null || auth.getPassword().trim().isEmpty()) {
            throw new IllegalArgumentException("Password is required for user validation");
        }
        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Rating must be between 1 and 5");
        }
        if (review == null || review.trim().isEmpty()) {
            throw new IllegalArgumentException("Review content cannot be null or empty");
        }
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        try {
            validateEditReviewParameters(auth, rating, review);
            long userId = permissionUtils.validateUser(auth);
            if (userId == -1L) {
                throw new SecurityException("Invalid user credentials or inactive account");
            }
            
            // 小数据集优化：合并权限检查和更新操作
            String updateSql = "UPDATE reviews SET Rating = ?, Review = ?, DateModified = CURRENT_TIMESTAMP " +
                              "WHERE ReviewId = ? AND RecipeId = ? AND AuthorId = ?";
            int affectedRows = jdbcTemplate.update(updateSql, rating, review.trim(), reviewId, recipeId, userId);
            
            if (affectedRows == 0) {
                // 检查具体失败原因
                String checkSql = "SELECT AuthorId, RecipeId FROM reviews WHERE ReviewId = ?";
                try {
                    ReviewInfo info = jdbcTemplate.queryForObject(checkSql, (rs, rowNum) -> 
                        new ReviewInfo(rs.getLong("AuthorId"), rs.getLong("RecipeId")), reviewId);
                    
                    if (info.getRecipeId() != recipeId) {
                        throw new IllegalArgumentException("Review does not belong to the specified recipe");
                    }
                    if (info.getAuthorId() != userId) {
                        throw new SecurityException("User is not authorized to edit this review");
                    }
                } catch (EmptyResultDataAccessException e) {
                    throw new IllegalArgumentException("Review does not exist");
                }
                throw new RuntimeException("Failed to update review");
            }
            
            updateRecipeRatingQuickly(recipeId);
        } catch (SecurityException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to edit review: " + e.getMessage(), e);
        }
    }

    private void validateEditReviewParameters(AuthInfo auth, int rating, String review) {
        if (auth == null || auth.getAuthorId() <= 0) {
            throw new IllegalArgumentException("Authentication info is required");
        }
        if (auth.getPassword() == null || auth.getPassword().isEmpty()) {
            throw new IllegalArgumentException("Password is required for user validation");
        }
        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Rating must be between 1 and 5");
        }
        if (review == null || review.trim().isEmpty()) {
            throw new IllegalArgumentException("Review content cannot be null or empty");
        }
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        try {
            validateDeleteReviewParameters(auth, recipeId, reviewId);
            long userId = authenticateUser(auth);
            validateDeleteReviewBusinessRules(userId, reviewId, recipeId);
            performReviewDeletion(reviewId);
            refreshRecipeRating(recipeId);
        } catch (SecurityException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete review: " + e.getMessage(), e);
        }
    }

    private long authenticateUser(AuthInfo auth) {
        long userId = permissionUtils.validateUser(auth);
        if (userId == -1L) {
            throw new SecurityException("Invalid user credentials or inactive account");
        }
        return userId;
    }

    private void validateDeleteReviewParameters(AuthInfo auth, long recipeId, long reviewId) {
        if (auth == null || auth.getAuthorId() <= 0) {
            throw new IllegalArgumentException("Authentication info is required");
        }
        if (auth.getPassword() == null || auth.getPassword().trim().isEmpty()) {
            throw new IllegalArgumentException("Password is required for user validation");
        }
        if (recipeId <= 0 || reviewId <= 0) {
            throw new IllegalArgumentException("Invalid recipe or review ID");
        }
    }

    private void validateDeleteReviewBusinessRules(long userId, long reviewId, long recipeId) {
        // 小数据集下的优化：合并权限检查为单个查询
        String sql = "SELECT AuthorId, RecipeId FROM reviews WHERE ReviewId = ?";
        try {
            ReviewInfo info = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> 
                new ReviewInfo(rs.getLong("AuthorId"), rs.getLong("RecipeId")), reviewId);
            
            if (info == null) {
                throw new IllegalArgumentException("Review with ID " + reviewId + " does not exist");
            }
            if (info.getRecipeId() != recipeId) {
                throw new IllegalArgumentException(
                        "Review with ID " + reviewId + " does not belong to recipe with ID " + recipeId
                );
            }
            if (info.getAuthorId() != userId) {
                throw new SecurityException(
                        "User with ID " + userId + " is not authorized to delete review with ID " + reviewId
                );
            }
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review with ID " + reviewId + " does not exist");
        }
    }


    private void performReviewDeletion(long reviewId) {
        try {
            deleteReviewLikes(reviewId);
            deleteReviewRecord(reviewId);
        } catch (Exception e) {
            throw new RuntimeException("Database error while deleting review: " + e.getMessage(), e);
        }
    }

    private void deleteReviewLikes(long reviewId) {
        String deleteLikesSql = "DELETE FROM review_likes WHERE ReviewId = ?";
        jdbcTemplate.update(deleteLikesSql, reviewId);
    }

    private void deleteReviewRecord(long reviewId) {
        String deleteReviewSql = "DELETE FROM reviews WHERE ReviewId = ?";
        int reviewsDeleted = jdbcTemplate.update(deleteReviewSql, reviewId);
        if (reviewsDeleted != 1) {
            throw new RuntimeException(
                    "Failed to delete review " + reviewId + " - unexpected row count: " + reviewsDeleted
            );
        }
    }

    private void refreshRecipeRating(long recipeId) {
        try {
            String updateSql =
                    "UPDATE recipes SET " +
                            "(AggregatedRating, ReviewCount) = (" +
                            "SELECT CASE WHEN COUNT(*) = 0 THEN NULL " +
                            "       ELSE ROUND(AVG(Rating)::numeric, 2) END, " +
                            "       COUNT(*)::INTEGER " +
                            "FROM reviews WHERE RecipeId = ?) " +
                            "WHERE RecipeId = ?";
            jdbcTemplate.update(updateSql, recipeId, recipeId);
        } catch (Exception e) {
            // 忽略错误，不影响主操作
        }
    }


    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        try {
            validateLikeReviewParameters(auth, reviewId);
            long userId = authenticateUser(auth);
            validateLikeReviewBusinessRules(userId, reviewId);
            return performLikeReview(userId, reviewId);
        } catch (SecurityException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to like review: " + e.getMessage(), e);
        }
    }

    private void validateLikeReviewParameters(AuthInfo auth, long reviewId) {
        if (auth == null || auth.getAuthorId() <= 0) {
            throw new IllegalArgumentException("Authentication info is required");
        }
        if (auth.getPassword() == null || auth.getPassword().trim().isEmpty()) {
            throw new IllegalArgumentException("Password is required for user validation");
        }
        if (reviewId <= 0) {
            throw new IllegalArgumentException("Invalid review ID");
        }
    }
    private void validateLikeReviewBusinessRules(long userId, long reviewId) {
        ReviewInfo reviewInfo = getReviewInfo(reviewId);
        if (reviewInfo == null) {
            throw new IllegalArgumentException("Review with ID " + reviewId + " does not exist");
        }
        if (reviewInfo.getAuthorId() == userId) {
            throw new SecurityException("User cannot like their own review");
        }
    }

    private static class ReviewInfo {
        private long authorId;
        private long recipeId;

        public ReviewInfo(long authorId, long recipeId) {
            this.authorId = authorId;
            this.recipeId = recipeId;
        }

        public long getAuthorId() { return authorId; }
        public long getRecipeId() { return recipeId; }
    }

    private ReviewInfo getReviewInfo(long reviewId) {
        String sql = "SELECT AuthorId, RecipeId FROM reviews WHERE ReviewId = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                long authorId = rs.getLong("AuthorId");
                long recipeId = rs.getLong("RecipeId");
                return new ReviewInfo(authorId, recipeId);
            }, reviewId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    private long performLikeReview(long userId, long reviewId) {
        if (hasUserLikedReview(userId, reviewId)) {
            return getReviewLikeCount(reviewId);
        }
        insertLike(userId, reviewId);
        return getReviewLikeCount(reviewId);
    }

    private void insertLike(long userId, long reviewId) {
        String sql = "INSERT INTO review_likes (AuthorId, ReviewId) VALUES (?, ?)";
        try {
            int rowsInserted = jdbcTemplate.update(sql, userId, reviewId);
            if (rowsInserted != 1) {
                throw new RuntimeException(
                        "Failed to insert like for review " + reviewId +
                                " by user " + userId + " - unexpected row count: " + rowsInserted
                );
            }
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("duplicate key")) {
                return;
            }
            throw new RuntimeException("Database error while inserting like: " + e.getMessage(), e);
        }
    }

    private long getReviewLikeCount(long reviewId) {
        String sql = "SELECT COUNT(*)::BIGINT FROM review_likes WHERE ReviewId = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, reviewId);
        } catch (Exception e) {
            return 0L;
        }
    }


    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        try {
            validateUnlikeReviewParameters(auth, reviewId);
            long userId = authenticateUser(auth);
            validateUnlikeReviewBusinessRules(reviewId);
            return processUnlike(userId, reviewId);
        } catch (SecurityException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to unlike review: " + e.getMessage(), e);
        }
    }

    private void validateUnlikeReviewParameters(AuthInfo auth, long reviewId) {
        if (auth == null || auth.getAuthorId() <= 0) {
            throw new IllegalArgumentException("Authentication info is required");
        }
        if (auth.getPassword() == null || auth.getPassword().trim().isEmpty()) {
            throw new IllegalArgumentException("Password is required for user validation");
        }
        if (reviewId <= 0) {
            throw new IllegalArgumentException("Invalid review ID");
        }
    }

    private void validateUnlikeReviewBusinessRules(long reviewId) {
        String sql = "SELECT EXISTS(SELECT 1 FROM reviews WHERE ReviewId = ?)";
        try {
            Boolean exists = jdbcTemplate.queryForObject(sql, Boolean.class, reviewId);
            if (exists == null || !exists) {
                throw new IllegalArgumentException("Review with ID " + reviewId + " does not exist");
            }
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                throw e;
            }
            throw new RuntimeException("Failed to validate review existence: " + e.getMessage(), e);
        }
    }


    private long processUnlike(long userId, long reviewId) {
        if (hasUserLikedReview(userId, reviewId)) {
            String deleteSql = "DELETE FROM review_likes WHERE AuthorId = ? AND ReviewId = ?";
            jdbcTemplate.update(deleteSql, userId, reviewId);
        }
        return getReviewLikeCount(reviewId);
    }

    private boolean hasUserLikedReview(long userId, long reviewId) {
        String sql = "SELECT EXISTS(SELECT 1 FROM review_likes WHERE AuthorId = ? AND ReviewId = ?)";
        try {
            return jdbcTemplate.queryForObject(sql, Boolean.class, userId, reviewId);
        } catch (Exception e) {
            return false;
        }
    }



    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        // 验证参数
        if (recipeId <= 0) {
            throw new IllegalArgumentException("Recipe ID must be > 0");
        }
        if (page < 1) {
            throw new IllegalArgumentException("Page must be >= 1");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be > 0");
        }
        
        // 计算偏移量
        int offset = (page - 1) * size;
        
        // 确定排序方式
        String orderBy = getOrderByClause(sort);
        
        // 查询总数
        String countSql = "SELECT COUNT(*) FROM reviews WHERE RecipeId = ?";
        Long total;
        try {
            total = jdbcTemplate.queryForObject(countSql, Long.class, recipeId);
            if (total == null) {
                total = 0L;
            }
        } catch (Exception e) {
            total = 0L;
        }
        
        if (total == 0) {
            return PageResult.<ReviewRecord>builder()
                    .items(new ArrayList<>())
                    .page(page)
                    .size(size)
                    .total(0L)
                    .build();
        }
        
        // 优化：一次查询获取所有数据包括likes
        String dataSql = "SELECT r.*, u.AuthorName, " +
                        "COALESCE((SELECT array_agg(rl.AuthorId ORDER BY rl.AuthorId) " +
                        "FROM review_likes rl WHERE rl.ReviewId = r.ReviewId), '{}') as likes " +
                        "FROM reviews r " +
                        "LEFT JOIN users u ON r.AuthorId = u.AuthorId " +
                        "WHERE r.RecipeId = ? " +
                        orderBy + " " +
                        "LIMIT ? OFFSET ?";
        
        List<ReviewRecord> reviews = jdbcTemplate.query(
            dataSql,
            new Object[]{recipeId, size, offset},
            (rs, rowNum) -> {
                ReviewRecord review = new ReviewRecord();
                review.setReviewId(rs.getLong("ReviewId"));
                review.setRecipeId(rs.getLong("RecipeId"));
                review.setAuthorId(rs.getLong("AuthorId"));
                
                String authorName = rs.getString("AuthorName");
                review.setAuthorName(authorName != null ? authorName : "Unknown User");
                
                review.setRating((float) rs.getInt("Rating"));
                review.setReview(rs.getString("Review"));
                
                Timestamp dateSubmitted = rs.getTimestamp("DateSubmitted");
                if (dateSubmitted != null) {
                    review.setDateSubmitted(dateSubmitted);
                }
                
                Timestamp dateModified = rs.getTimestamp("DateModified");
                if (dateModified != null) {
                    review.setDateModified(dateModified);
                }
                
                // 处理likes数组
                try {
                    Array likesArray = rs.getArray("likes");
                    if (likesArray != null) {
                        Long[] likesData = (Long[]) likesArray.getArray();
                        if (likesData != null) {
                            review.setLikes(Arrays.stream(likesData).mapToLong(Long::longValue).toArray());
                        } else {
                            review.setLikes(new long[0]);
                        }
                    } else {
                        review.setLikes(new long[0]);
                    }
                } catch (Exception e) {
                    review.setLikes(new long[0]);
                }
                
                return review;
            }
        );
        
        return PageResult.<ReviewRecord>builder()
                .items(reviews)
                .page(page)
                .size(size)
                .total(total)
                .build();
    }

    private String getOrderByClause(String sort) {
        if (sort == null || sort.trim().isEmpty()) {
            // 默认排序：使用ReviewId确保稳定性
            return "ORDER BY r.ReviewId DESC";
        } else if ("date_desc".equals(sort)) {
            return "ORDER BY r.DateModified DESC, r.ReviewId DESC";
        } else if ("likes_desc".equals(sort)) {
            return "ORDER BY (SELECT COUNT(*) FROM review_likes WHERE ReviewId = r.ReviewId) DESC, r.ReviewId DESC";
        } else {
            // 无效的sort参数，使用默认排序
            return "ORDER BY r.ReviewId DESC";
        }
    }



    private void updateRecipeRatingQuickly(long recipeId) {
        try {
            calculateRecipeRatingStats(recipeId);
        } catch (Exception e) {
            // 忽略错误，不影响主操作
        }
    }
    
    private void updateRecipeRatingOptimized(long recipeId) {
        try {
            // 小数据集优化：单个SQL直接更新rating统计
            String updateSql = "UPDATE recipes SET " +
                    "AggregatedRating = (SELECT ROUND(AVG(Rating)::numeric, 2) FROM reviews WHERE RecipeId = ?), " +
                    "ReviewCount = (SELECT COUNT(*)::INTEGER FROM reviews WHERE RecipeId = ?) " +
                    "WHERE RecipeId = ?";
            jdbcTemplate.update(updateSql, recipeId, recipeId, recipeId);
        } catch (Exception e) {
            // 忽略错误，不影响主操作
        }
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        try {
            if (recipeId <= 0) {
                throw new IllegalArgumentException("Invalid recipe ID");
            }
            if (!permissionUtils.recipeExists(recipeId)) {
                throw new IllegalArgumentException("Recipe with ID " + recipeId + " does not exist");
            }
            RatingStats ratingStats = calculateRecipeRatingStats(recipeId);
            return getUpdatedRecipeRecord(recipeId);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to refresh recipe rating: " + e.getMessage(), e);
        }
    }

    private static class RatingStats {
        private Double averageRating;
        private int reviewCount;

        public RatingStats(Double averageRating, int reviewCount) {
            this.averageRating = averageRating;
            this.reviewCount = reviewCount;
        }

        public Double getAverageRating() { return averageRating; }
        public int getReviewCount() { return reviewCount; }
    }

    private RatingStats calculateRecipeRatingStats(long recipeId) {
        String sql = """
            UPDATE recipes SET
                (AggregatedRating, ReviewCount) = (
                    SELECT CASE WHEN COUNT(*) = 0 THEN NULL 
                           ELSE ROUND(AVG(Rating)::numeric, 2) END,
                           COUNT(*)::INTEGER
                    FROM reviews WHERE RecipeId = ?
                )
            WHERE RecipeId = ?
            RETURNING AggregatedRating, ReviewCount
            """;

        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Double avgRating = rs.getObject("AggregatedRating", Double.class);
                int reviewCount = rs.getInt("ReviewCount");
                return new RatingStats(avgRating, reviewCount);
            }, recipeId, recipeId);
        } catch (Exception e) {
            String fallbackSql =
                    "SELECT " +
                            "  CASE WHEN COUNT(*) = 0 THEN NULL " +
                            "       ELSE ROUND(AVG(Rating)::numeric, 2) " +
                            "  END as avg_rating, " +
                            "  COUNT(*)::INTEGER as review_count " +
                            "FROM reviews " +
                            "WHERE RecipeId = ?";
            
            Map<String, Object> stats = jdbcTemplate.queryForMap(fallbackSql, recipeId);
            Double avgRating = (Double) stats.get("avg_rating");
            Integer reviewCount = (Integer) stats.get("review_count");
            return new RatingStats(avgRating, reviewCount != null ? reviewCount : 0);
        }
    }


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

                float aggregatedRating = rs.getFloat("AggregatedRating");
                if (rs.wasNull()) {
                    recipe.setAggregatedRating(0.0f);
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

                List<String> ingredients = getRecipeIngredients(recipeId);
                recipe.setRecipeIngredientParts(ingredients.toArray(new String[0]));
                return recipe;
            }, recipeId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe with ID " + recipeId + " does not exist");
        }
    }

    private List<String> getRecipeIngredients(long recipeId) {
        String sql = "SELECT IngredientPart FROM recipe_ingredients WHERE RecipeId = ? ORDER BY IngredientPart";
        try {
            return jdbcTemplate.queryForList(sql, String.class, recipeId);
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }
}
