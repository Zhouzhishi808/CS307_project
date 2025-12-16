package io.sustc.util;

import io.sustc.dto.*;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.format.DateTimeParseException;

@Component
@Slf4j
public class PermissionUtils {
    @Autowired
    private static JdbcTemplate jdbcTemplate;

    //检验用户是否存在
    public static long validateUser(AuthInfo auth) {
        if (auth == null || auth.getAuthorId() <= 0) {
            log.warn("Invalid auth info: {}", auth);
            return -1L;
        }

        String sql = "SELECT COUNT(*) FROM users WHERE AuthorId = ? AND IsDeleted = false";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, auth.getAuthorId());
            if (count != null && count > 0) {
                return auth.getAuthorId();
            }
        } catch (EmptyResultDataAccessException e) {
            // 用户不存在
        }
        log.warn("User {} not found or inactive", auth.getAuthorId());
        return -1L;
    }

    public boolean isRecipeAuthor(long userId, long recipeId) {
        String sql = "SELECT COUNT(*) FROM recipes WHERE RecipeId = ? AND AuthorId = ?";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, recipeId, userId);
            return count != null && count > 0;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    public static boolean recipeExists(long recipeId) {
        String sql = "SELECT COUNT(*) FROM recipes WHERE RecipeId = ?";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, recipeId);
            return count != null && count > 0;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    public boolean isReviewAuthor(long userId, long reviewId) {
        String sql = "SELECT COUNT(*) FROM reviews WHERE ReviewId = ? AND AuthorId = ?";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, reviewId, userId);
            return count != null && count > 0;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    public boolean reviewBelongsToRecipe(long reviewId, long recipeId) {
        String sql = "SELECT COUNT(*) FROM reviews WHERE ReviewId = ? AND RecipeId = ?";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, reviewId, recipeId);
            return count != null && count > 0;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    public boolean reviewExists(long reviewId) {
        String sql = "SELECT COUNT(*) FROM reviews WHERE ReviewId = ?";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, reviewId);
            return count != null && count > 0;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    public static boolean hasUserReviewedRecipe(long userId, long recipeId) {
        String sql = "SELECT COUNT(*) FROM reviews WHERE AuthorId = ? AND RecipeId = ?";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, userId, recipeId);
            return count != null && count > 0;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    public boolean hasUserLikedReview(long userId, long reviewId) {
        String sql = "SELECT COUNT(*) FROM review_likes WHERE AuthorId = ? AND ReviewId = ?";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, userId, reviewId);
            return count != null && count > 0;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    public long getReviewLikeCount(long reviewId) {
        String sql = "SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?";
        try {
            Long count = jdbcTemplate.queryForObject(sql, Long.class, reviewId);
            return count != null ? count : 0;
        } catch (EmptyResultDataAccessException e) {
            return 0;
        }
    }

    public long getRecipeReviewCount(long recipeId) {
        String sql = "SELECT COUNT(*) FROM reviews WHERE RecipeId = ?";
        try {
            Long count = jdbcTemplate.queryForObject(sql, Long.class, recipeId);
            return count != null ? count : 0;
        } catch (EmptyResultDataAccessException e) {
            return 0;
        }
    }

    public static long generateNewId(String tableName, String idColumn) {
        String sql = String.format("SELECT COALESCE(MAX(%s), 0) FROM %s", idColumn, tableName);
        try {
            Long maxId = jdbcTemplate.queryForObject(sql, Long.class);
            return (maxId != null ? maxId : 0) + 1;
        } catch (EmptyResultDataAccessException e) {
            return 1L;
        }
    }


}
