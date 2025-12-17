package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import io.sustc.util.PasswordUtil;
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
import java.util.*;


// TODO: 权限管理优化、高并发测试
// 密码使用 PasswordUtil 加密存储，不存明文
@Service
@Slf4j
public class UserServiceImpl implements UserService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    // 注册新用户：验证用户名、性别、年龄，加密密码后插入数据库
    // 返回新用户 ID，失败返回 -1（用户名重复、参数无效等）
    @Override
    public long register(RegisterUserReq req) {
        if (req == null || req.getName() == null || req.getName().trim().isEmpty()) {
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

        String hash = PasswordUtil.hashPassword(req.getPassword());
        String sql = "INSERT INTO users (AuthorName, Gender, Age, Password, Followers, Following, IsDeleted) " +
                "VALUES (?, ?, ?, ?, 0, 0, false)";

        KeyHolder keyHolder = new GeneratedKeyHolder();

        try {
            int rows = jdbcTemplate.update(connection -> {
                PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, req.getName());
                ps.setString(2, gender);
                ps.setInt(3, age);
                ps.setString(4, hash);
                return ps;
            }, keyHolder);

            if (rows > 0 && keyHolder.getKey() != null) {
                return keyHolder.getKey().longValue();
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

        String sql = "SELECT Password FROM users WHERE AuthorId = ? AND IsDeleted = false";

        try {
            String hash = jdbcTemplate.queryForObject(sql, String.class, auth.getAuthorId());

            if (hash != null && PasswordUtil.verifyPassword(auth.getPassword(), hash)) {
                return auth.getAuthorId();
            }
            return -1L;
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
    // 操作后关注中返回 true，取消关注返回 false
    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        if (!isValidUser(auth)) {
            throw new SecurityException("follow");
        }

        if (auth.getAuthorId() == followeeId) {
            throw new SecurityException("follow");
        }

        String checkFolloweeSql = "SELECT COUNT(*) FROM users WHERE AuthorId = ? AND IsDeleted = false";
        Integer count = jdbcTemplate.queryForObject(checkFolloweeSql, Integer.class, followeeId);
        if (count == null || count == 0) {
            return false;
        }

        String checkFollowSql = "SELECT COUNT(*) FROM user_follows WHERE FollowerId = ? AND FollowingId = ?";
        Integer followCount = jdbcTemplate.queryForObject(checkFollowSql, Integer.class, auth.getAuthorId(), followeeId);

        if (followCount != null && followCount > 0) {
            jdbcTemplate.update("DELETE FROM user_follows WHERE FollowerId = ? AND FollowingId = ?",
                auth.getAuthorId(), followeeId);
            return false;
        } else {
            jdbcTemplate.update("INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)",
                auth.getAuthorId(), followeeId);
            return true;
        }
    }


    // 根据 ID 查询用户信息
    @Override
    @Cacheable(value = "users", key = "#userId")
    public UserRecord getById(long userId) {
        try {
            String sql = "SELECT * FROM public.users WHERE AuthorId = ?";
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                UserRecord user = new UserRecord();
                user.setAuthorId(rs.getLong("AuthorId"));
                user.setAuthorName(rs.getString("AuthorName"));
                user.setGender(rs.getString("Gender"));
                user.setAge(rs.getInt("Age"));
                user.setFollowers(rs.getInt("Followers"));
                user.setFollowing(rs.getInt("Following"));
                user.setPassword(rs.getString("Password"));
                user.setDeleted(rs.getBoolean("IsDeleted"));
                return user;
            }, userId);
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

        if (!verifyPassword(auth)) {
            throw new SecurityException("updateProfile");
        }

        if (gender != null && !gender.equals("Male") && !gender.equals("Female")) {
            throw new IllegalArgumentException("updateProfile");
        }

        if (age != null && age <= 0) {
            throw new IllegalArgumentException("updateProfile");
        }

        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("UPDATE users SET ");
        boolean hasUpdate = false;

        if (gender != null) {
            sql.append("Gender = ?");
            params.add(gender);
            hasUpdate = true;
        }

        if (age != null) {
            if (hasUpdate) {
                sql.append(", ");
            }
            sql.append("Age = ?");
            params.add(age);
            hasUpdate = true;
        }

        if (!hasUpdate) {
            return;
        }

        sql.append(" WHERE AuthorId = ?");
        params.add(auth.getAuthorId());

        jdbcTemplate.update(sql.toString(), params.toArray());
    }

    // 获取用户关注的人发布的食谱列表（feed 流）
    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        try {
            log.debug("Feed request for authorId: {}, page: {}, size: {}, category: {}", 
                     auth != null ? auth.getAuthorId() : "null", page, size, category);
            
            if (!isValidUser(auth)) {
                log.warn("Invalid user for feed: {}", auth != null ? auth.getAuthorId() : "null");
                throw new SecurityException("feed");
            }

            page = Math.max(1, page);
            size = Math.max(1, Math.min(200, size));
            
            log.debug("Normalized page: {}, size: {}", page, size);

        StringBuilder countSql = new StringBuilder(
            "SELECT COUNT(*) FROM recipes r " +
            "INNER JOIN user_follows uf ON r.AuthorId = uf.FollowingId " +
            "WHERE uf.FollowerId = ?"
        );

        StringBuilder dataSql = new StringBuilder(
            "SELECT r.RecipeId, r.Name, r.AuthorId, u.AuthorName, r.DatePublished, r.AggregatedRating, r.ReviewCount " +
            "FROM recipes r " +
            "INNER JOIN user_follows uf ON r.AuthorId = uf.FollowingId " +
            "INNER JOIN users u ON r.AuthorId = u.AuthorId " +
            "WHERE uf.FollowerId = ?"
        );

        List<Object> params = new ArrayList<>();
        params.add(auth.getAuthorId());

        if (category != null) {
            countSql.append(" AND r.RecipeCategory = ?");
            dataSql.append(" AND r.RecipeCategory = ?");
            params.add(category);
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
            Timestamp ts = rs.getTimestamp("DatePublished");
            item.setDatePublished(ts != null ? ts.toInstant() : null);
            BigDecimal rating = rs.getBigDecimal("AggregatedRating");
            item.setAggregatedRating(rating != null ? rating.doubleValue() : null);
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
        String sql = "SELECT u.AuthorId, u.AuthorName, " +
                "COALESCE(follower.cnt, 0) AS FollowerCount, " +
                "COALESCE(following.cnt, 0) AS FollowingCount, " +
                "COALESCE(follower.cnt, 0) * 1.0 / COALESCE(following.cnt, 1) AS Ratio " +
                "FROM users u " +
                "LEFT JOIN (SELECT FollowingId, COUNT(*) AS cnt FROM user_follows GROUP BY FollowingId) follower " +
                "ON u.AuthorId = follower.FollowingId " +
                "LEFT JOIN (SELECT FollowerId, COUNT(*) AS cnt FROM user_follows GROUP BY FollowerId) following " +
                "ON u.AuthorId = following.FollowerId " +
                "WHERE u.IsDeleted = false AND COALESCE(following.cnt, 0) > 0 " +
                "ORDER BY Ratio DESC, u.AuthorId ASC LIMIT 1";

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
        if (auth == null || auth.getAuthorId() <= 0) {
            return false;
        }

        String sql = "SELECT IsDeleted FROM users WHERE AuthorId = ?";
        try {
            Boolean isDeleted = jdbcTemplate.queryForObject(sql, Boolean.class, auth.getAuthorId());
            return isDeleted != null && !isDeleted;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    // 验证密码是否正确
    private boolean verifyPassword(AuthInfo auth) {
        if (auth == null || auth.getPassword() == null || auth.getPassword().isEmpty()) {
            return false;
        }

        String sql = "SELECT Password FROM users WHERE AuthorId = ? AND IsDeleted = false";
        try {
            String hash = jdbcTemplate.queryForObject(sql, String.class, auth.getAuthorId());
            return hash != null && PasswordUtil.verifyPassword(auth.getPassword(), hash);
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