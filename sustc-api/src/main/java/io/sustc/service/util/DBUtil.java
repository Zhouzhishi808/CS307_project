package io.sustc.service.util;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Component
public class DBUtil {
    private static DataSource springDataSource;

    @Autowired
    public void setDataSource(DataSource dataSource) {
        DBUtil.springDataSource = dataSource;
    }

    public static Connection getAdminConnection() throws SQLException {
        return springDataSource.getConnection();
    }

    public static Connection getWriterConnection() throws SQLException {
        return springDataSource.getConnection();
    }

    public static Connection getReaderConnection() throws SQLException {
        return springDataSource.getConnection();
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // Log error but don't throw
                e.printStackTrace();
            }
        }
    }

    public static JdbcTemplate getAdminJdbcTemplate() {
        return new JdbcTemplate(springDataSource);
    }

    public static JdbcTemplate getWriterJdbcTemplate() {
        return new JdbcTemplate(springDataSource);
    }

    public static JdbcTemplate getReaderJdbcTemplate() {
        return new JdbcTemplate(springDataSource);
    }

    public static void shutdown() {
        // Spring会自动管理DataSource生命周期，无需手动关闭
        // 保留此方法以兼容现有调用，但内部为空
    }
}
