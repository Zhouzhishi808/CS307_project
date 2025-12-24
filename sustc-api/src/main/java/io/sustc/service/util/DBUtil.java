package io.sustc.service.util;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtil {
    // Database connection parameters
    private static final String DB_HOST = System.getProperty("db.host", "localhost");
    private static final String DB_PORT = System.getProperty("db.port", "5432");
    private static final String DB_NAME = System.getProperty("db.name", "postgres");
    private static final String DB_URL = String.format("jdbc:postgresql://%s:%s/%s", DB_HOST, DB_PORT, DB_NAME);
    
    // Admin credentials (for DDL operations like drop/import)
    private static final String ADMIN_USER = System.getProperty("db.user", "postgres");
    private static final String ADMIN_PASSWORD = System.getProperty("db.password", "Fs921012");
    
    // Writer credentials (for DML operations like insert/update/delete)
    private static final String WRITER_USER = System.getProperty("db.writer.user", "sustc_writer");
    private static final String WRITER_PASSWORD = System.getProperty("db.writer.password", "123456");
    
    // Reader credentials (for read-only operations)
    private static final String READER_USER = System.getProperty("db.reader.user", "sustc_reader");
    private static final String READER_PASSWORD = System.getProperty("db.reader.password", "123456");
    
    // Unified HikariCP DataSource for all operations
    private static HikariDataSource unifiedDataSource;
    
    static {
        try {
            Class.forName("org.postgresql.Driver");
            initializeUnifiedDataSource();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load PostgreSQL driver", e);
        }
    }
    
    private static void initializeUnifiedDataSource() {
        // Initialize unified connection pool with large capacity for high concurrency
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        config.setUsername(ADMIN_USER);  // Use admin credentials for unified access
        config.setPassword(ADMIN_PASSWORD);
        config.setMaximumPoolSize(3000);  // Support 2048+ concurrent users
        config.setMinimumIdle(500);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        config.setLeakDetectionThreshold(60000);
        config.setPoolName("UnifiedPool");
        unifiedDataSource = new HikariDataSource(config);
    }
    

    public static Connection getAdminConnection() throws SQLException {
        return unifiedDataSource.getConnection();
    }
    

    public static Connection getWriterConnection() throws SQLException {
        return unifiedDataSource.getConnection();
    }

    public static Connection getReaderConnection() throws SQLException {
        return unifiedDataSource.getConnection();
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
        return new JdbcTemplate(unifiedDataSource);
    }

    public static JdbcTemplate getWriterJdbcTemplate() {
        return new JdbcTemplate(unifiedDataSource);
    }

    public static JdbcTemplate getReaderJdbcTemplate() {
        return new JdbcTemplate(unifiedDataSource);
    }
    
    public static void shutdown() {
        if (unifiedDataSource != null) {
            unifiedDataSource.close();
        }
    }
}