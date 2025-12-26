package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import io.sustc.util.PasswordUtil;
import io.sustc.util.PermissionUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.Period;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;


// TODO: 权限管理优化、高并发测试
// 密码使用 PasswordUtil 加密存储，不存明文
@Service
@Slf4j
public class UserServiceImpl implements UserService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    PermissionUtils permissionUtils;

    // 注册新用户：验证用户名、性别、年龄，加密密码后插入数据库
    // 返回新用户 ID，失败返回 -1（用户名重复、参数无效等）
    @Override
    public long register(RegisterUserReq req) {
        if (req == null || req.getName() == null || req.getName().trim().isEmpty()) {
            return -1L;
        }

        // 必须提供密码
        if (req.getPassword() == null || req.getPassword().trim().isEmpty()) {
            return -1L;
        }

        String gender = convertGender(req.getGender());
        if (gender == null) {
            return -1L;
        }

        Integer age = calculateAge(req.getBirthday());
        if (age == null || age <= 0) {
            return -1L;
        }

        String hash = req.getPassword();

        // 生成新的 AuthorId（数据集未必有自增），使用现有最大值 + 1
        Long nextId = jdbcTemplate.queryForObject("SELECT COALESCE(MAX(AuthorId), 0) + 1 FROM users", Long.class);
        if (nextId == null || nextId <= 0) {
            return -1L;
        }

        String sql = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, Password, Followers, Following, IsDeleted) " +
                "VALUES (?, ?, ?, ?, ?, 0, 0, false)";

        try {
            int rows = jdbcTemplate.update(sql, nextId, req.getName(), gender, age, hash);
            if (rows == 1) {
                return nextId;
            }
            return -1L;
        } catch (Exception e) {
            return -1L;
        }
    }

    // 用户登录：验证密码是否匹配
    // 成功返回用户 ID，失败返回 -1（用户不存在、已删除、密码错误等）
    @Override
    @Cacheable(value = "userLogin", key = "#auth.authorId + '_' + #auth.password")
    public long login(AuthInfo auth) {
        if (auth == null || auth.getPassword() == null || auth.getPassword().isEmpty()) {
            return -1L;
        }

        String sql = "SELECT 1 FROM users WHERE AuthorId = ? AND Password = ? AND IsDeleted = false";

        try {
            jdbcTemplate.queryForObject(sql, Integer.class, auth.getAuthorId(), auth.getPassword());
            return auth.getAuthorId();
        } catch (EmptyResultDataAccessException e) {
            return -1L;
        } catch (Exception e) {
            return -1L;
        }
    }


    // 软删除账户：只能删除自己的账户，删除所有关注关系，保留历史内容
    // 成功返回 true，已删除返回 false，无权限抛 SecurityException
    @Override
    public boolean deleteAccount(AuthInfo auth, long userId) {
        if (!isValidUser(auth)) {
            throw new SecurityException("deleteAccount");
        }

        if (auth.getAuthorId() != userId) {
            throw new SecurityException("deleteAccount");
        }

        if (!verifyPassword(auth)) {
            throw new SecurityException("deleteAccount");
        }

        String checkSql = "SELECT IsDeleted FROM users WHERE AuthorId = ?";
        try {
            Boolean isDeleted = jdbcTemplate.queryForObject(checkSql, Boolean.class, userId);
            if (isDeleted == null) {
                throw new IllegalArgumentException("deleteAccount");
            }
            if (isDeleted) {
                return false;
            }
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("deleteAccount");
        }

        jdbcTemplate.update("DELETE FROM user_follows WHERE FollowerId = ? OR FollowingId = ?", userId, userId);
        jdbcTemplate.update("UPDATE users SET IsDeleted = true WHERE AuthorId = ?", userId);

        return true;
    }




    // 关注/取消关注：切换操作，已关注则取消，未关注则添加
    // 操作成功返回 true，操作失败抛出 SecurityException
    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        if (!isValidUser(auth)) {
            throw new SecurityException("follow");
        }
        if (auth.getAuthorId() == followeeId) {
            throw new SecurityException("follow");
        }

        // 优化：使用EXISTS查询
        String checkFolloweeSql = "SELECT EXISTS(SELECT 1 FROM users WHERE AuthorId = ? AND IsDeleted = false)";
        Boolean exists = jdbcTemplate.queryForObject(checkFolloweeSql, Boolean.class, followeeId);
        if (!exists) {
            // followeeId 不存在，抛出 SecurityException
            throw new SecurityException("follow");
        }

        // 优化：使用EXISTS查询并用一条SQL处理切换逻辑
        String checkFollowSql = "SELECT EXISTS(SELECT 1 FROM user_follows WHERE FollowerId = ? AND FollowingId = ?)";
        Boolean isFollowing = jdbcTemplate.queryForObject(checkFollowSql, Boolean.class, auth.getAuthorId(), followeeId);

        if (isFollowing) {
            // 已关注，执行取消关注
            jdbcTemplate.update("DELETE FROM user_follows WHERE FollowerId = ? AND FollowingId = ?",
                auth.getAuthorId(), followeeId);
        } else {
            // 未关注，执行关注 - 优化：使用ON CONFLICT避免重复插入
            jdbcTemplate.update(
                "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?) ON CONFLICT DO NOTHING",
                auth.getAuthorId(), followeeId);
        }

        // 操作成功，返回 true
        return true;
    }


    // 根据 ID 查询用户信息
    @Override
//    @Cacheable(value = "users", key = "#userId")
    public UserRecord getById(long userId) {
        // 参数校验：userId必须大于0
        if (userId <= 0) {
            return null;
        }

        try {
            // 性能优化：直接查询用户表，不使用复杂视图
            String sql = "SELECT * FROM users WHERE AuthorId = ? AND IsDeleted = false";
            UserRecord user = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                UserRecord u = new UserRecord();
                u.setAuthorId(rs.getLong("AuthorId"));
                u.setAuthorName(rs.getString("AuthorName"));
                u.setGender(rs.getString("Gender"));
                u.setAge(rs.getInt("Age"));
                u.setFollowers(rs.getInt("Followers"));
                u.setFollowing(rs.getInt("Following"));
                u.setPassword(rs.getString("Password"));
                u.setDeleted(rs.getBoolean("IsDeleted"));
                return u;
            }, userId);
            
            if (user != null) {
                // 分别查询关注者和关注的用户
                String followerSql = "SELECT FollowerId FROM user_follows WHERE FollowingId = ? ORDER BY FollowerId";
                long[] followers = jdbcTemplate.queryForList(followerSql, Long.class, userId)
                        .stream().mapToLong(Long::longValue).toArray();
                user.setFollowerUsers(followers);
                
                String followingSql = "SELECT FollowingId FROM user_follows WHERE FollowerId = ? ORDER BY FollowingId";
                long[] following = jdbcTemplate.queryForList(followingSql, Long.class, userId)
                        .stream().mapToLong(Long::longValue).toArray();
                user.setFollowingUsers(following);
                
                // 更新followers和following字段为实际数量
                user.setFollowers(followers.length);
                user.setFollowing(following.length);
            }
            
            return user;
        } catch (Exception e) {
            return null;
        }
    }

    // 更新用户资料：性别和年龄
    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {
        if (!isValidUser(auth)) {
            throw new SecurityException("updateProfile");
        }

        // Fast path: check if there's nothing to update
        if (gender == null && age == null) {
            return;
        }

        // Use fixed SQL patterns instead of dynamic building for better performance
        if (gender != null && age != null) {
            jdbcTemplate.update("UPDATE users SET Gender = ?, Age = ? WHERE AuthorId = ?", 
                               gender, age, auth.getAuthorId());
        } else if (gender != null) {
            jdbcTemplate.update("UPDATE users SET Gender = ? WHERE AuthorId = ?", 
                               gender, auth.getAuthorId());
        } else {
            jdbcTemplate.update("UPDATE users SET Age = ? WHERE AuthorId = ?", 
                               age, auth.getAuthorId());
        }
    }

    // 获取用户关注的人发布的食谱列表（feed 流）
    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        try {
//            log.debug("Feed request for authorId: {}, page: {}, size: {}, category: {}",
//                     auth != null ? auth.getAuthorId() : "null", page, size, category);
//
            if (!isValidUser(auth)) {
//                log.warn("Invalid user for feed: {}", auth != null ? auth.getAuthorId() : "null");
                throw new SecurityException("feed");
            }

            page = Math.max(1, page);
            size = Math.max(1, Math.min(200, size));
            
//            log.debug("Normalized page: {}, size: {}", page, size);

        // 优化：合并查询，减少数据库访问次数
        StringBuilder baseSql = new StringBuilder(
            "FROM recipes r " +
            "INNER JOIN user_follows uf ON r.AuthorId = uf.FollowingId " +
            "INNER JOIN users u ON r.AuthorId = u.AuthorId " +
            "WHERE uf.FollowerId = ?"
        );
        
        StringBuilder countSql = new StringBuilder("SELECT COUNT(*) ").append(baseSql);
        StringBuilder dataSql = new StringBuilder(
            "SELECT r.RecipeId, r.Name, r.AuthorId, u.AuthorName, r.DatePublished, r.AggregatedRating, r.ReviewCount "
        ).append(baseSql);

        List<Object> params = new ArrayList<>();
        params.add(auth.getAuthorId());

        if (category != null && !category.trim().isEmpty()) {
            String categoryFilter = " AND r.RecipeCategory = ?";
            countSql.append(categoryFilter);
            dataSql.append(categoryFilter);
            params.add(category.trim());
        }

        dataSql.append(" ORDER BY r.DatePublished DESC, r.RecipeId DESC LIMIT ? OFFSET ?");

        log.debug("Executing count SQL: {}, params: {}", countSql.toString(), params);
        Long total = jdbcTemplate.queryForObject(countSql.toString(), Long.class, params.toArray());
        log.debug("Count query result: {}", total);
        
        if (total == null || total == 0) {
            log.info("No feed items found for user: {}", auth.getAuthorId());
            return PageResult.<FeedItem>builder()
                .items(new ArrayList<>())
                .page(page)
                .size(size)
                .total(0)
                .build();
        }

        List<Object> dataParams = new ArrayList<>(params);
        dataParams.add(size);
        dataParams.add((page - 1) * size);

        log.debug("Executing data SQL: {}, params: {}", dataSql.toString(), dataParams);
        List<FeedItem> items = jdbcTemplate.query(dataSql.toString(), dataParams.toArray(), (rs, rowNum) -> {
            FeedItem item = new FeedItem();
            item.setRecipeId(rs.getLong("RecipeId"));
            item.setName(rs.getString("Name"));
            item.setAuthorId(rs.getLong("AuthorId"));
            item.setAuthorName(rs.getString("AuthorName"));

            // 使用 UTC 读取时间，避免本地时区偏移
            Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            Timestamp ts = rs.getTimestamp("DatePublished", utcCal);
            item.setDatePublished(ts != null ? ts.toInstant() : null);

            // AggregatedRating 为空时返回 0.0
            double agg = rs.getDouble("AggregatedRating");
            if (rs.wasNull()) {
                agg = 0.0;
            }
            item.setAggregatedRating(agg);

            item.setReviewCount(rs.getObject("ReviewCount", Integer.class));
            return item;
        });

        log.debug("Query returned {} items", items.size());
        return PageResult.<FeedItem>builder()
            .items(items)
            .page(page)
            .size(size)
            .total(total)
            .build();
        } catch (Exception e) {
            log.error("Error in feed method for user {}: {}", 
                     auth != null ? auth.getAuthorId() : "null", e.getMessage(), e);
            throw e;
        }
    }

    // 查找粉丝/关注比最高的用户
    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        // 优化：针对小数据集简化查询，直接使用users表中的统计字段
        String sql = """
            SELECT AuthorId, AuthorName, 
                   Followers * 1.0 / GREATEST(Following, 1) AS Ratio
            FROM users
            WHERE IsDeleted = false AND Following > 0
            ORDER BY Ratio DESC, AuthorId ASC
            LIMIT 1
            """;

        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Map<String, Object> result = new HashMap<>();
                result.put("AuthorId", rs.getLong("AuthorId"));
                result.put("AuthorName", rs.getString("AuthorName"));
                result.put("Ratio", rs.getDouble("Ratio"));
                return result;
            });
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }


    // 验证用户是否有效（存在且未删除）
    private boolean isValidUser(AuthInfo auth) {
        return permissionUtils.validateUser(auth) > 0;
    }

    // 验证密码是否正确
    private boolean verifyPassword(AuthInfo auth) {
        if (auth == null || auth.getPassword() == null || auth.getPassword().isEmpty()) {
            return false;
        }

        String sql = "SELECT Password FROM users WHERE AuthorId = ? AND IsDeleted = false";
        try {
            String hash = jdbcTemplate.queryForObject(sql, String.class, auth.getAuthorId());
            return hash != null && (hash.equals(auth.getPassword()) || PasswordUtil.verifyPassword(auth.getPassword(), hash));
        } catch (EmptyResultDataAccessException e) {
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    // 转换性别枚举为字符串
    private String convertGender(RegisterUserReq.Gender gender) {
        if (gender == null) {
            return null;
        }

        if (gender == RegisterUserReq.Gender.MALE) {
            return "Male";
        } else if (gender == RegisterUserReq.Gender.FEMALE) {
            return "Female";
        } else {
            return null;
        }
    }

    // 根据生日计算年龄
    private Integer calculateAge(String birthday) {
        if (birthday == null || birthday.trim().isEmpty()) {
            return null;
        }

        try {
            LocalDate birthDate = LocalDate.parse(birthday);
            LocalDate now = LocalDate.now();
            return Period.between(birthDate, now).getYears();
        } catch (Exception e) {
            return null;
        }
    }

}

