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
    //插入新的评论，为其自动分配ID
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        try {
            log.debug("Starting addReview for recipeId: {}, rating: {}, user: {}",
                    recipeId, rating, auth != null ? auth.getAuthorId() : "null");

            validateAddReviewParameters(auth, rating, review);

            long userId = permissionUtils.validateUser(auth);
            if (userId == -1L) {
                throw new SecurityException("Invalid or inactive user");
            }

            if (!permissionUtils.recipeExists(recipeId)) {
                throw new IllegalArgumentException("Recipe does not exist");
            }

            if (permissionUtils.hasUserReviewedRecipe(userId, recipeId)) {
                throw new IllegalArgumentException("User has already reviewed this recipe");
            }

            long newReviewId = permissionUtils.generateNewId("reviews", "ReviewId");

            Timestamp now = Timestamp.from(Instant.now());
            String insertSql = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";

            int rowsInserted = jdbcTemplate.update(insertSql,
                    newReviewId, recipeId, userId, rating, review.trim(), now, now);

            if (rowsInserted != 1) {
                throw new RuntimeException("Failed to insert review - unexpected row count: " + rowsInserted);
            }

            refreshRecipeAggregatedRating(recipeId);

            log.info("Successfully added review {} for recipe {} by user {}",
                    newReviewId, recipeId, userId);

            return newReviewId;

        } catch (SecurityException | IllegalArgumentException e) {
            // 重新抛出这些已知的异常
            log.warn("addReview failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            // 捕获所有其他异常
            log.error("Unexpected error in addReview: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to add review: " + e.getMessage(), e);
        }
    }

    private void validateAddReviewParameters(AuthInfo auth, int rating, String review) {
        // 验证auth参数
        if (permissionUtils.validateUser(auth) == -1) {
            throw new IllegalArgumentException("Authentication info is required");
        }

        // 验证评分范围
        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Rating must be between 1 and 5");
        }

        // 验证评论内容
        if (review == null) {
            throw new IllegalArgumentException("Review content cannot be null");
        }

        String trimmedReview = review.trim();
        if (trimmedReview.isEmpty()) {
            throw new IllegalArgumentException("Review content cannot be empty");
        }
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        log.debug("Starting editReview for reviewId: {}, recipeId: {}, user: {}",
                reviewId, recipeId, auth != null ? auth.getAuthorId() : "null");

        try {
            // === 1. 基础参数验证（符合接口规范）===
            validateEditReviewParameters(auth, rating, review);

            // === 2. 用户认证 ===
            long userId = permissionUtils.validateUser(auth);
            if (userId == -1L) {
                throw new SecurityException("Invalid user credentials or inactive account");
            }

            // === 3. 使用工具类进行权限验证 ===
            // 验证评论是否存在
            if (!permissionUtils.reviewExists(reviewId)) {
                throw new IllegalArgumentException("Review does not exist");
            }

            // 验证评论是否属于指定的食谱
            if (!permissionUtils.reviewBelongsToRecipe(reviewId, recipeId)) {
                throw new IllegalArgumentException("Review does not belong to the specified recipe");
            }

            // 验证用户是否为评论作者
            if (!permissionUtils.isReviewAuthor(userId, reviewId)) {
                throw new SecurityException("User is not authorized to edit this review");
            }

            // === 4. 执行评论更新 ===
            String updateSql = "UPDATE reviews SET Rating = ?, Review = ?, DateModified = CURRENT_TIMESTAMP WHERE ReviewId = ?";
            int affectedRows = jdbcTemplate.update(updateSql, rating, review.trim(), reviewId);

            if (affectedRows != 1) {
                throw new RuntimeException("Failed to update review - unexpected row count: " + affectedRows);
            }

            refreshRecipeAggregatedRating(recipeId);

            // === 5. 记录日志 ===
            log.info("User {} edited review {} for recipe {}", userId, reviewId, recipeId);

        } catch (SecurityException | IllegalArgumentException e) {
            // 重新抛出这些已知的异常
            log.warn("editReview failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            // 捕获所有其他异常
            log.error("Unexpected error in editReview: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to edit review: " + e.getMessage(), e);
        }
    }

    private void validateEditReviewParameters(AuthInfo auth, int rating, String review) {
        if (auth == null) {
            throw new IllegalArgumentException("Authentication info is required");
        }

        if (auth.getAuthorId() <= 0) {
            throw new IllegalArgumentException("Invalid user ID");
        }

        if (auth.getPassword() == null || auth.getPassword().isEmpty()) {
            throw new IllegalArgumentException("Password is required for user validation");
        }

        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Rating must be between 1 and 5");
        }

        if (review == null) {
            throw new IllegalArgumentException("Review content cannot be null");
        }

        String trimmedReview = review.trim();
        if (trimmedReview.isEmpty()) {
            throw new IllegalArgumentException("Review content cannot be empty");
        }

    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        final String METHOD_NAME = "deleteReview";
        log.debug("[{}] Starting - reviewId: {}, recipeId: {}, user: {}",
                METHOD_NAME, reviewId, recipeId, auth != null ? auth.getAuthorId() : "null");

        try {
            // === 1. 参数验证 ===
            validateDeleteReviewParameters(auth, recipeId, reviewId);

            // === 2. 用户认证 ===
            long userId = authenticateUser(auth);

            // === 3. 业务规则验证 ===
            validateDeleteReviewBusinessRules(userId, reviewId, recipeId);

            // === 4. 获取评论信息（用于日志） ===
            Map<String, Object> reviewInfo = getReviewInfoForLogging(reviewId);

            // === 5. 执行删除操作 ===
            performReviewDeletion(reviewId);

            // === 6. 刷新食谱评分 ===
            refreshRecipeRating(recipeId);

            // === 7. 记录成功日志 ===
            logSuccessfulDeletion(reviewId, recipeId, userId, reviewInfo);

            log.debug("[{}] Completed successfully", METHOD_NAME);

        } catch (SecurityException | IllegalArgumentException e) {
            log.warn("[{}] Failed due to business rule violation: {}", METHOD_NAME, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("[{}] Failed unexpectedly: {}", METHOD_NAME, e.getMessage(), e);
            throw new RuntimeException("Failed to delete review: " + e.getMessage(), e);
        }
    }

    private long authenticateUser(AuthInfo auth) {
        // 使用 PermissionUtils.validateUser 来验证用户是否存在且活跃
        long userId = permissionUtils.validateUser(auth);
        if (userId == -1L) {
            throw new SecurityException("Invalid user credentials or inactive account");
        }
        return userId;
    }

    // 辅助方法：验证删除评论的参数
    private void validateDeleteReviewParameters(AuthInfo auth, long recipeId, long reviewId) {
        if (auth == null) {
            throw new IllegalArgumentException("Authentication info is required");
        }

        if (auth.getAuthorId() <= 0) {
            throw new IllegalArgumentException("Invalid user ID");
        }

        if (auth.getPassword() == null || auth.getPassword().trim().isEmpty()) {
            throw new IllegalArgumentException("Password is required for user validation");
        }

        if (recipeId <= 0) {
            throw new IllegalArgumentException("Invalid recipe ID");
        }

        if (reviewId <= 0) {
            throw new IllegalArgumentException("Invalid review ID");
        }
    }

    // 辅助方法：验证删除评论的业务规则
    private void validateDeleteReviewBusinessRules(long userId, long reviewId, long recipeId) {
        // 验证评论是否存在
        if (!permissionUtils.reviewExists(reviewId)) {
            throw new IllegalArgumentException("Review with ID " + reviewId + " does not exist");
        }

        // 验证评论是否属于指定的食谱
        if (!permissionUtils.reviewBelongsToRecipe(reviewId, recipeId)) {
            throw new IllegalArgumentException(
                    "Review with ID " + reviewId + " does not belong to recipe with ID " + recipeId
            );
        }

        // 验证用户是否为评论作者
        if (!permissionUtils.isReviewAuthor(userId, reviewId)) {
            throw new SecurityException(
                    "User with ID " + userId + " is not authorized to delete review with ID " + reviewId
            );
        }
    }

    // 辅助方法：获取评论信息用于日志记录
    private Map<String, Object> getReviewInfoForLogging(long reviewId) {
        try {
            String sql = "SELECT RecipeId, AuthorId, Rating, Review FROM reviews WHERE ReviewId = ?";
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Map<String, Object> info = new HashMap<>();
                info.put("recipeId", rs.getLong("RecipeId"));
                info.put("authorId", rs.getLong("AuthorId"));
                info.put("rating", rs.getInt("Rating"));
                info.put("review", rs.getString("Review"));
                return info;
            }, reviewId);
        } catch (EmptyResultDataAccessException e) {
            log.warn("Could not retrieve review info for review {} (it may have been deleted)", reviewId);
            return null;
        }
    }

    // 辅助方法：执行评论删除操作
    private void performReviewDeletion(long reviewId) {
        try {
            // 1. 先删除评论的所有点赞（因为外键约束）
            deleteReviewLikes(reviewId);

            // 2. 删除评论本身
            deleteReviewRecord(reviewId);

            log.debug("Successfully deleted review {} and all its likes", reviewId);

        } catch (Exception e) {
            log.error("Database error when deleting review {}: {}", reviewId, e.getMessage(), e);
            throw new RuntimeException("Database error while deleting review: " + e.getMessage(), e);
        }
    }

    // 辅助方法：删除评论的所有点赞
    private void deleteReviewLikes(long reviewId) {
        String deleteLikesSql = "DELETE FROM review_likes WHERE ReviewId = ?";
        int likesDeleted = jdbcTemplate.update(deleteLikesSql, reviewId);

        if (likesDeleted > 0) {
            log.debug("Deleted {} likes for review {}", likesDeleted, reviewId);
        }
    }

    // 辅助方法：删除评论记录
    private void deleteReviewRecord(long reviewId) {
        String deleteReviewSql = "DELETE FROM reviews WHERE ReviewId = ?";
        int reviewsDeleted = jdbcTemplate.update(deleteReviewSql, reviewId);

        if (reviewsDeleted != 1) {
            throw new RuntimeException(
                    "Failed to delete review " + reviewId + " - unexpected row count: " + reviewsDeleted
            );
        }
    }

    // 辅助方法：刷新食谱评分
    private void refreshRecipeRating(long recipeId) {
        try {
            // 直接使用 SQL 更新，确保评分正确
            String updateSql =
                    "UPDATE recipes SET " +
                            "AggregatedRating = (SELECT ROUND(AVG(Rating)::numeric, 2) FROM reviews WHERE RecipeId = ?), " +
                            "ReviewCount = (SELECT COUNT(*) FROM reviews WHERE RecipeId = ?) " +
                            "WHERE RecipeId = ?";

            int updated = jdbcTemplate.update(updateSql, recipeId, recipeId, recipeId);

            if (updated == 1) {
                log.debug("Successfully refreshed recipe rating for recipe {}", recipeId);
            } else {
                log.warn("Recipe {} not found or already deleted", recipeId);
            }
        } catch (Exception e) {
            log.error("Failed to refresh recipe rating for recipe {}: {}", recipeId, e.getMessage(), e);
            // 这里不抛出异常，因为删除操作已经成功
            // 评分刷新失败不应该影响删除操作的成功
        }
    }

    // 辅助方法：记录成功的删除操作
    private void logSuccessfulDeletion(long reviewId, long recipeId, long userId, Map<String, Object> reviewInfo) {
        if (reviewInfo != null) {
            log.info("User {} deleted review {} (rating: {}) for recipe {}",
                    userId, reviewId, reviewInfo.get("rating"), recipeId);
        } else {
            log.info("User {} deleted review {} for recipe {}", userId, reviewId, recipeId);
        }
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        final String METHOD_NAME = "likeReview";
        log.debug("[{}] Starting - reviewId: {}, user: {}",
                METHOD_NAME, reviewId, auth != null ? auth.getAuthorId() : "null");

        try {
            // === 1. 参数验证 ===
            validateLikeReviewParameters(auth, reviewId);

            // === 2. 用户认证 ===
            long userId = authenticateActiveUser(auth);

            // === 3. 业务规则验证 ===
            validateLikeReviewBusinessRules(userId, reviewId);

            // === 4. 执行点赞操作 ===
            long totalLikes = performLikeReview(userId, reviewId);

            // === 5. 记录成功日志 ===
            logSuccessfulLike(reviewId, userId, totalLikes);

            log.debug("[{}] Completed successfully, total likes: {}", METHOD_NAME, totalLikes);
            return totalLikes;

        } catch (SecurityException | IllegalArgumentException e) {
            log.warn("[{}] Failed due to business rule violation: {}", METHOD_NAME, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("[{}] Failed unexpectedly: {}", METHOD_NAME, e.getMessage(), e);
            throw new RuntimeException("Failed to like review: " + e.getMessage(), e);
        }
    }

    // 辅助方法：验证点赞评论的参数
    private void validateLikeReviewParameters(AuthInfo auth, long reviewId) {
        if (auth == null) {
            throw new IllegalArgumentException("Authentication info is required");
        }

        if (auth.getAuthorId() <= 0) {
            throw new IllegalArgumentException("Invalid user ID");
        }

        if (auth.getPassword() == null || auth.getPassword().trim().isEmpty()) {
            throw new IllegalArgumentException("Password is required for user validation");
        }

        if (reviewId <= 0) {
            throw new IllegalArgumentException("Invalid review ID");
        }
    }


    // 辅助方法：验证点赞评论的业务规则
    private void validateLikeReviewBusinessRules(long userId, long reviewId) {
        // 验证评论是否存在
        if (!permissionUtils.reviewExists(reviewId)) {
            throw new IllegalArgumentException("Review with ID " + reviewId + " does not exist");
        }

        // 获取评论信息，验证用户不是评论作者
        ReviewInfo reviewInfo = getReviewInfo(reviewId);
        if (reviewInfo == null) {
            throw new IllegalArgumentException("Review with ID " + reviewId + " does not exist");
        }

        // 验证用户不能给自己的评论点赞
        if (reviewInfo.getAuthorId() == userId) {
            throw new SecurityException("User cannot like their own review");
        }
    }

    // 内部类：评论信息
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

    // 辅助方法：获取评论信息
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

    // 辅助方法：执行点赞操作
    private long performLikeReview(long userId, long reviewId) {
        // 检查是否已经点赞
        if (hasUserLikedReview(userId, reviewId)) {
            // 如果已经点赞，直接返回当前点赞数（幂等操作）
            long currentLikes = getReviewLikeCount(reviewId);
            log.debug("User {} has already liked review {}, returning current like count: {}",
                    userId, reviewId, currentLikes);
            return currentLikes;
        }

        // 插入点赞记录
        insertLike(userId, reviewId);

        // 返回新的总点赞数
        return getReviewLikeCount(reviewId);
    }

    // 辅助方法：插入点赞记录
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

            log.debug("Successfully inserted like for review {} by user {}", reviewId, userId);

        } catch (Exception e) {
            // 如果是唯一约束冲突（用户已经点赞），忽略这个异常
            if (e.getMessage() != null && e.getMessage().contains("duplicate key")) {
                log.debug("User {} has already liked review {}, ignoring duplicate", userId, reviewId);
                return;
            }

            log.error("Database error when inserting like for review {} by user {}: {}",
                    reviewId, userId, e.getMessage(), e);
            throw new RuntimeException("Database error while inserting like: " + e.getMessage(), e);
        }
    }

    // 辅助方法：获取评论的点赞数
    private long getReviewLikeCount(long reviewId) {
        String sql = "SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?";

        try {
            Long count = jdbcTemplate.queryForObject(sql, Long.class, reviewId);
            return count != null ? count : 0;
        } catch (Exception e) {
            log.error("Error getting like count for review {}: {}", reviewId, e.getMessage());
            return 0;
        }
    }

    // 辅助方法：记录成功的点赞操作
    private void logSuccessfulLike(long reviewId, long userId, long totalLikes) {
        log.info("User {} liked review {}. Total likes: {}", userId, reviewId, totalLikes);
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        log.debug("Starting unlikeReview for reviewId: {}, user: {}",
                reviewId, auth != null ? auth.getAuthorId() : "null");

        try {
            // === 1. 参数验证 ===
            if (auth == null) {
                throw new IllegalArgumentException("Authentication info is required");
            }

            if (reviewId <= 0) {
                throw new IllegalArgumentException("Invalid review ID");
            }

            // === 2. 用户认证 ===
            long userId = authenticateActiveUser(auth);

            // === 3. 验证评论存在性 ===
            if (!permissionUtils.reviewExists(reviewId)) {
                throw new IllegalArgumentException("Review does not exist");
            }

            // === 4. 执行取消点赞（使用已有的工具方法） ===
            long totalLikes = processUnlike(userId, reviewId);

            // === 5. 记录日志 ===
            log.info("User {} unliked review {}. Total likes: {}", userId, reviewId, totalLikes);

            return totalLikes;

        } catch (SecurityException | IllegalArgumentException e) {
            log.warn("unlikeReview failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error in unlikeReview: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to unlike review: " + e.getMessage(), e);
        }
    }

    // 辅助方法：处理取消点赞的核心逻辑
    private long processUnlike(long userId, long reviewId) {
        // 检查用户是否已经点赞
        if (hasUserLikedReview(userId, reviewId)) {
            // 删除点赞记录
            String deleteSql = "DELETE FROM review_likes WHERE AuthorId = ? AND ReviewId = ?";
            jdbcTemplate.update(deleteSql, userId, reviewId);
            log.debug("User {} removed like from review {}", userId, reviewId);
        } else {
            log.debug("User {} had not liked review {}, no action needed", userId, reviewId);
        }

        // 返回当前点赞总数
        return getReviewLikeCount(reviewId);
    }

    // 工具方法：检查用户是否已经点赞（如果类中没有，需要添加）
    private boolean hasUserLikedReview(long userId, long reviewId) {
        String sql = "SELECT COUNT(*) FROM review_likes WHERE AuthorId = ? AND ReviewId = ?";

        try {
            Long count = jdbcTemplate.queryForObject(sql, Long.class, userId, reviewId);
            return count != null && count > 0;
        } catch (Exception e) {
            log.error("Error checking if user {} liked review {}: {}", userId, reviewId, e.getMessage());
            return false;
        }
    }


    @Override
    @Cacheable(value = "reviewLists", key = "#recipeId + '_' + #page + '_' + #size + '_' + #sort")
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        log.debug("Listing reviews for recipeId: {}, page: {}, size: {}, sort: {}",
                recipeId, page, size, sort);

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
                log.debug("No reviews found for recipe {}", recipeId);
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

            log.info("Found {} reviews for recipe {} (page {} of {})",
                    total, recipeId, validPage, calculateTotalPages(total, validSize));

            return result;

        } catch (IllegalArgumentException e) {
            log.warn("listByRecipe failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error in listByRecipe: {}", e.getMessage(), e);
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
                log.warn("Invalid sort parameter '{}', using default 'date_desc'", sort);
                return "ORDER BY r.DateModified DESC, r.ReviewId DESC";
        }
    }

    // 辅助方法：构建查询语句
    private String buildQuerySql(String orderByClause) {
        return "SELECT r.*, u.AuthorName, " +
                "COALESCE(COUNT(rl.ReviewId), 0) as like_count " +
                "FROM reviews r " +
                "LEFT JOIN users u ON r.AuthorId = u.AuthorId " +
                "LEFT JOIN review_likes rl ON r.ReviewId = rl.ReviewId " +
                "WHERE r.RecipeId = ? " +
                "GROUP BY r.ReviewId, u.AuthorName " +
                orderByClause + " " +
                "LIMIT ? OFFSET ?";
    }

    // 辅助方法：执行分页查询
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

                    // 获取点赞用户列表
                    review.setLikes(getLikesForReview(review.getReviewId()));

                    return review;
                }
        );
    }

    // 辅助方法：获取评论的点赞用户列表
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
            log.debug("Error getting likes for review {}: {}", reviewId, e.getMessage());
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
        final String METHOD_NAME = "refreshRecipeAggregatedRating";
        log.debug("[{}] Starting for recipeId: {}", METHOD_NAME, recipeId);

        try {
            // === 1. 验证参数 ===
            if (recipeId <= 0) {
                throw new IllegalArgumentException("Invalid recipe ID");
            }

            // === 2. 验证食谱是否存在 ===
            if (!permissionUtils.recipeExists(recipeId)) {
                throw new IllegalArgumentException("Recipe with ID " + recipeId + " does not exist");
            }

            // === 3. 计算新的评分和评论数 ===
            RatingStats ratingStats = calculateRecipeRatingStats(recipeId);

            // === 4. 更新食谱的评分和评论数 ===
            updateRecipeRating(recipeId, ratingStats);

            // === 5. 获取并返回更新后的食谱记录 ===
            RecipeRecord updatedRecipe = getUpdatedRecipeRecord(recipeId);

            log.info("[{}] Successfully refreshed rating for recipe {}: rating={}, reviewCount={}",
                    METHOD_NAME, recipeId, ratingStats.getAverageRating(), ratingStats.getReviewCount());

            return updatedRecipe;

        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed: {}", METHOD_NAME, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("[{}] Failed unexpectedly: {}", METHOD_NAME, e.getMessage(), e);
            throw new RuntimeException("Failed to refresh recipe rating: " + e.getMessage(), e);
        }
    }

    // 内部类：用于存储评分统计信息
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

    // 辅助方法：计算食谱的评分统计
    private RatingStats calculateRecipeRatingStats(long recipeId) {
        // 在 SQL 中直接转换为 DOUBLE PRECISION 和 INTEGER
        String sql =
                "SELECT " +
                        "  CASE WHEN COUNT(*) = 0 THEN NULL " +
                        "       ELSE ROUND(AVG(Rating)::numeric, 2)::DOUBLE PRECISION " +
                        "  END as avg_rating, " +
                        "  COUNT(*)::INTEGER as review_count " +
                        "FROM reviews " +
                        "WHERE RecipeId = ?";

        try {
            Map<String, Object> stats = jdbcTemplate.queryForMap(sql, recipeId);

            // 现在应该是正确的类型
            Double avgRating = (Double) stats.get("avg_rating");
            Integer reviewCount = (Integer) stats.get("review_count");

            return new RatingStats(avgRating, reviewCount != null ? reviewCount : 0);

        } catch (EmptyResultDataAccessException e) {
            // 没有评论，评分和评论数都为0
            log.debug("No reviews found for recipe {}, setting rating to null and count to 0", recipeId);
            return new RatingStats(null, 0);
        }
    }

    // 辅助方法：更新食谱的评分和评论数
    private void updateRecipeRating(long recipeId, RatingStats ratingStats) {
        String updateSql = "UPDATE recipes SET AggregatedRating = ?, ReviewCount = ? WHERE RecipeId = ?";

        int rowsAffected = jdbcTemplate.update(
                updateSql,
                ratingStats.getAverageRating(),
                ratingStats.getReviewCount(),
                recipeId
        );

        if (rowsAffected != 1) {
            throw new RuntimeException(
                    "Failed to update recipe rating for recipe " + recipeId +
                            " - unexpected row count: " + rowsAffected
            );
        }
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
            log.warn("Failed to get ingredients for recipe {}: {}", recipeId, e.getMessage());
            return new ArrayList<>();
        }
    }

    private long authenticateActiveUser(AuthInfo auth) {
        // 支持明文或加盐存储的密码校验，保证密码错误触发 SecurityException
        if (auth == null || auth.getPassword() == null || auth.getPassword().trim().isEmpty()) {
            throw new IllegalArgumentException("Authentication info is required");
        }
        String sql = "SELECT Password FROM users WHERE AuthorId = ? AND IsDeleted = false";
        try {
            String stored = jdbcTemplate.queryForObject(sql, String.class, auth.getAuthorId());
            if (stored == null) {
                throw new SecurityException("Invalid user credentials or inactive account");
            }
            boolean ok = stored.equals(auth.getPassword()) || io.sustc.util.PasswordUtil.verifyPassword(auth.getPassword(), stored);
            if (ok) {
                return auth.getAuthorId();
            }
            throw new SecurityException("Invalid user credentials or inactive account");
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("Invalid user credentials or inactive account");
        }
    }

}
