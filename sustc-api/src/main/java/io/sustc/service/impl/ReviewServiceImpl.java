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
            
            // 小数据集优化：在插入时检查食谱存在性，避免额外查询
            long newReviewId = permissionUtils.generateNewId("reviews", "ReviewId");
            Timestamp now = Timestamp.from(Instant.now());
            
            try {
                String insertSql = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)";
                int rowsInserted = jdbcTemplate.update(insertSql,
                        newReviewId, recipeId, userId, rating, review.trim(), now, now);
                if (rowsInserted != 1) {
                    throw new RuntimeException("Failed to insert review - unexpected row count: " + rowsInserted);
                }
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("foreign key")) {
                    throw new IllegalArgumentException("Recipe does not exist");
                }
                throw e;
            }
            
            updateRecipeRatingQuickly(recipeId);
            return newReviewId;
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
        // 小数据集下直接在操作时检查即可，避免额外查询
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
        try {
            if (recipeId <= 0) {
                throw new IllegalArgumentException("Recipe ID must be positive");
            }
            if (page < 1) {
                throw new IllegalArgumentException("Page must be >= 1");
            }
            if (size <= 0) {
                throw new IllegalArgumentException("Size must be > 0");
            }

            int validSize = Math.max(1, Math.min(size, 200));
            int validPage = Math.max(1, page);
            int offset = (validPage - 1) * validSize;

            String countSql = "SELECT COUNT(*) FROM reviews WHERE RecipeId = ?";
            Long total = jdbcTemplate.queryForObject(countSql, Long.class, recipeId);

            if (total == null || total == 0) {
                return PageResult.<ReviewRecord>builder()
                        .items(new ArrayList<>())
                        .page(validPage)
                        .size(validSize)
                        .total(0L)
                        .build();
            }

            String orderByClause = buildOrderByClause(sort);
            String querySql = buildQuerySql(orderByClause);
            List<ReviewRecord> reviews = executePaginatedQuery(recipeId, querySql, validSize, offset);

            return PageResult.<ReviewRecord>builder()
                    .items(reviews)
                    .page(validPage)
                    .size(validSize)
                    .total(total)
                    .build();

        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to list reviews by recipe: " + e.getMessage(), e);
        }
    }

    private String buildOrderByClause(String sort) {
        // 小数据集优化：简化排序，避免复杂计算
        if (sort != null && "likes_desc".equals(sort.toLowerCase())) {
            return "ORDER BY r.ReviewId DESC"; // 用ReviewId代替复杂的点赞数排序
        }
        return "ORDER BY r.DateModified DESC, r.ReviewId DESC";
    }

    private String buildQuerySql(String orderByClause) {
        // 小数据集优化：简化查询，避免复杂连接
        return "SELECT r.*, u.AuthorName FROM reviews r " +
               "LEFT JOIN users u ON r.AuthorId = u.AuthorId " +
               "WHERE r.RecipeId = ? " +
               orderByClause + " " +
               "LIMIT ? OFFSET ?";
    }

    private List<ReviewRecord> executePaginatedQuery(long recipeId, String sql, int limit, int offset) {
        List<ReviewRecord> reviews = jdbcTemplate.query(
                sql,
                new Object[]{recipeId, limit, offset},
                (rs, rowNum) -> {
                    ReviewRecord review = new ReviewRecord();
                    review.setReviewId(rs.getLong("ReviewId"));
                    review.setRecipeId(rs.getLong("RecipeId"));
                    review.setAuthorId(rs.getLong("AuthorId"));
                    review.setAuthorName(rs.getString("AuthorName"));
                    review.setRating((float) rs.getInt("Rating"));
                    review.setReview(rs.getString("Review"));
                    
                    Timestamp dateSubmitted = rs.getTimestamp("DateSubmitted");
                    Timestamp dateModified = rs.getTimestamp("DateModified");
                    if (dateSubmitted != null) {
                        review.setDateSubmitted(dateSubmitted);
                    }
                    if (dateModified != null) {
                        review.setDateModified(dateModified);
                    }
                    return review;
                }
        );
        
        if (!reviews.isEmpty()) {
            setLikesForReviews(reviews);
        }
        
        return reviews;
    }

    private void setLikesForReviews(List<ReviewRecord> reviews) {
        // 小数据集优化：简化点赞查询，优先使用逐个查询避免IN查询开销
        if (reviews.size() <= 10) {
            // 对于小量数据，逐个查询更快
            for (ReviewRecord review : reviews) {
                String sql = "SELECT AuthorId FROM review_likes WHERE ReviewId = ? ORDER BY AuthorId";
                try {
                    List<Long> likes = jdbcTemplate.queryForList(sql, Long.class, review.getReviewId());
                    review.setLikes(likes.stream().mapToLong(Long::longValue).toArray());
                } catch (Exception e) {
                    review.setLikes(new long[0]);
                }
            }
        } else {
            // 对于较大数据，使用批量查询
            List<Long> reviewIds = reviews.stream().map(ReviewRecord::getReviewId).toList();
            String placeholders = String.join(",", reviewIds.stream().map(id -> "?").toList());
            String likesQuery = "SELECT ReviewId, AuthorId FROM review_likes WHERE ReviewId IN (" +
                    placeholders + ") ORDER BY ReviewId, AuthorId";
            
            Map<Long, List<Long>> likesMap = new HashMap<>();
            jdbcTemplate.query(likesQuery, reviewIds.toArray(), (rs) -> {
                long reviewId = rs.getLong("ReviewId");
                long authorId = rs.getLong("AuthorId");
                likesMap.computeIfAbsent(reviewId, k -> new ArrayList<>()).add(authorId);
            });
            
            for (ReviewRecord review : reviews) {
                List<Long> likes = likesMap.getOrDefault(review.getReviewId(), Collections.emptyList());
                review.setLikes(likes.stream().mapToLong(Long::longValue).toArray());
            }
        }
    }


    private void updateRecipeRatingQuickly(long recipeId) {
        try {
            calculateRecipeRatingStats(recipeId);
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
