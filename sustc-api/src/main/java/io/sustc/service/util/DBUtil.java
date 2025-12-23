package io.sustc.service.util;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
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
    private static final String ADMIN_USER = System.getProperty("db.user", "sustc");
    private static final String ADMIN_PASSWORD = System.getProperty("db.password", "sustc");
    
    // Writer credentials (for DML operations like insert/update/delete)
    private static final String WRITER_USER = System.getProperty("db.writer.user", "sustc_writer");
    private static final String WRITER_PASSWORD = System.getProperty("db.writer.password", "123456");
    
    // Reader credentials (for read-only operations)
    private static final String READER_USER = System.getProperty("db.reader.user", "sustc_reader");
    private static final String READER_PASSWORD = System.getProperty("db.reader.password", "123456");
    
    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load PostgreSQL driver", e);
        }
    }
    

    public static Connection getAdminConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, ADMIN_USER, ADMIN_PASSWORD);
    }
    

    public static Connection getWriterConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, WRITER_USER, WRITER_PASSWORD);
    }

    public static Connection getReaderConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, READER_USER, READER_PASSWORD);
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
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(DB_URL);
        dataSource.setUsername(ADMIN_USER);
        dataSource.setPassword(ADMIN_PASSWORD);
        return new JdbcTemplate(dataSource);
    }

    public static JdbcTemplate getWriterJdbcTemplate() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(DB_URL);
        dataSource.setUsername(WRITER_USER);
        dataSource.setPassword(WRITER_PASSWORD);
        return new JdbcTemplate(dataSource);
    }

    public static JdbcTemplate getReaderJdbcTemplate() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(DB_URL);
        dataSource.setUsername(READER_USER);
        dataSource.setPassword(READER_PASSWORD);
        return new JdbcTemplate(dataSource);
    }
}