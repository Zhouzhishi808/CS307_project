package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UserServiceImpl implements UserService {

    @Autowired
    private JdbcTemplate jdbcTemplate;




//            ### 功能描述
//- 这是一个在系统中注册新用户的方法。
//
//            ### 必填字段
//- 在 `req` 中，以下字段是必需的：
//            - `authorName`（用户名）
//            - `gender`（性别）
//            - `age`（年龄）
//
//            ### 返回值
//- **成功时返回新用户的 `authorId`**。
//            - **注册失败时返回 `-1`**。
//
//            ### 注册失败的特殊情况（返回 `-1`）
//            - `authorName` 为 `null` 或为空。
//            - `gender` 为 `null`、为空或不在 `{“Male”, “Female”}` 中。
//            - `age` 为 `null` 或不是一个有效的正整数。
//            - 已存在一个具有相同 `authorName` 的用户。
//
//            ### 其他说明
//- 如果出现上述任何特殊情况，该方法必须返回 `-1`。
    @Override
    public long register(RegisterUserReq req) {
        // 基本验证
        if (req == null || req.getName() == null || req.getName().trim().isEmpty()) {
            return -1L;
        }

        // Gender转换
        String gender = convertGender(req.getGender());
        if (gender == null) {
            return -1L;
        }

        // 计算年龄
        Integer age = calculateAge(req.getBirthday());
        if (age == null || age <= 0) {
            return -1L;
        }

        // 检查用户名是否存在
        String checkSql = "SELECT COUNT(*) FROM users WHERE AuthorName = ?";
        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, req.getName());
        if (count != null && count > 0) {
            return -1L;
        }

        // 插入新用户
        String insertSql = "INSERT INTO users (AuthorName, Gender, Age, Password, Followers, Following, IsDeleted) " +
                "VALUES (?, ?, ?, ?, 0, 0, false)";

        KeyHolder keyHolder = new GeneratedKeyHolder();

        try {
            int rows = jdbcTemplate.update(connection -> {
                PreparedStatement ps = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, req.getName());
                ps.setString(2, gender);
                ps.setInt(3, age);
                ps.setString(4, req.getPassword());
                return ps;
            }, keyHolder);

            if (rows > 0 && keyHolder.getKey() != null) {
                return keyHolder.getKey().longValue();
            }
            return -1L;
        } catch (Exception e) {
            log.error("Register failed: {}", e.getMessage());
            return -1L;
        }
    }


//            ### 功能描述
//- 这是一个基于密码的用户登录认证方法。
//            ### 登录要求和规则
//- **仅允许基于密码的登录**。
//            - **`auth` 中的 `authorId` 必须指向一个已存在的用户**。
//            - **目标用户必须是活跃的（未被软删除）**。
//            - **`auth` 中的密码必须与用户存储的密码哈希值匹配**。
//            ### 返回值
//- **成功时返回认证用户的 `authorId`**。
//            - **认证失败时返回 `-1`**。
//            ### 认证失败的特殊情况（返回 `-1`）
//            - `auth` 为 `null`。
//            - `auth.authorId` 无效（用户不存在）。
//            - 用户被软删除（非活跃）。
//            - `auth.password` 为 `null` 或为空。
//            - 密码与用户存储的密码哈希值不匹配。
//
//            ### 异常处理
//- 登录失败时不抛出异常，而是返回 `-1`。
//
//            ### 其他说明
//- 该项目中的其他 API 不要求严格的密码验证。
    @Override
    public long login(AuthInfo auth) {  //fixme:**`auth` 中的密码必须与用户存储的密码哈希值匹配**。
        if (auth == null || auth.getPassword() == null || auth.getPassword().isEmpty()) {
            return -1L;
        }
        String sql = "SELECT AuthorId FROM users WHERE AuthorId = ? AND Password = ? AND IsDeleted = false";

        try {
            Long userId = jdbcTemplate.queryForObject(sql, Long.class, auth.getAuthorId(), auth.getPassword());
            return userId != null ? userId : -1L;
        } catch (EmptyResultDataAccessException e) {
            return -1L;
        } catch (Exception e) {
            log.error("Login failed: {}", e.getMessage());
            return -1L;
        }
    }


//    ### 功能描述
//    这是一个对用户账户进行软删除的方法。
//
//            ### 授权规则
//- 用户可以删除自己的账户。
//            - 已软删除或非活跃用户不能执行此操作。
//
//            ### 一致性规则
//- 指定的 `userId` 必须存在且指向一个活跃用户。
//
//            ### 软删除后的条件
//- 用户被标记为非活跃（例如，`isDeleted = true`）。
//            - 被删除的用户不能再进行认证或执行任何操作。
//            - 用户创建的现有内容（如食谱、评论、点赞等）应保留，但被视为属于非活跃用户。
//            - 涉及该用户的所有关注关系必须被移除：
//            - 该用户不再关注任何人（清空其关注列表）。
//            - 没有其他用户关注该用户（从所有关注者列表中移除该用户）。
//
//            ### 参数
//- `auth`：操作者的身份验证信息。
//            - `userId`：要软删除的用户账户的ID。
//
//            ### 返回值
//- 如果账户成功被软删除，返回 `true`。
//            - 如果账户已经是非活跃状态，返回 `false`。
//
//            ### 异常
//- 如果 `auth` 无效、非活跃，或者操作者无权删除账户，抛出 `SecurityException`。
//            - 如果目标用户不存在，抛出 `IllegalArgumentException`。
    @Override
    public boolean deleteAccount(AuthInfo auth, long userId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }




//### 功能描述
//    这是一个用于关注或取消关注用户的操作。
//
//            ### 操作逻辑
//- 这是一个切换操作：
//            - 如果当前用户尚未关注 `followeeId`，则执行关注操作。
//            - 如果已经关注了，则取消关注。
//
//            ### 参数
//- `auth`：关注者的身份验证信息。
//            - `followeeId`：要被关注或取消关注的用户ID。
//
//            ### 返回值
//- 如果操作后关注状态变为“关注中”，返回 `true`。
//            - 如果操作失败，或者关注状态变为“未关注”，返回 `false`。
//
//            ### 特殊情况（返回 `false`）
//            - `auth` 无效。
//            - 指定的 `followeeId` 不存在对应用户。
//            - 用户尝试关注自己。
//
//            ### 异常
//- 如果 `auth` 无效、用户非活跃，或者用户尝试关注自己，抛出 `SecurityException`。
    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    @Override
    public UserRecord getById(long userId) {
        // 占位。
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

    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        throw new UnsupportedOperationException("Not implemented yet");
    }


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