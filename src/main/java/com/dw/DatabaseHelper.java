package com.dw;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseHelper implements AutoCloseable {
    private Connection connection;
    private final String dbUrl;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 2000;

    public DatabaseHelper(String dbUrl) throws SQLException {
        this.dbUrl = dbUrl;
        this.connection = getConnection();
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl);
    }

    public List<FileConfig> getPendingFileConfigs() throws SQLException {
        return getFileConfigsByStatus("NP");
    }

    public List<FileConfig> getFileConfigsByStatus(String status) throws SQLException {
        String query = """
            SELECT config_key, config_value
            FROM dw_configurations
            WHERE config_group = 'FILE_PATH' AND is_active = 1 AND file_status = ?
        """;
        List<FileConfig> fileConfigs = new ArrayList<>();

        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, status);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String configKey = rs.getString("config_key");
                    String configValue = rs.getString("config_value");
                    fileConfigs.add(new FileConfig(configKey, configValue));
                }
            }
        }
        return fileConfigs;
    }

    public void updateFileStatus(String configKey, String status) throws SQLException {
        String query = """
        UPDATE dbo.dw_configurations  -- Thêm schema dbo
        SET file_status = ?, last_modified = GETDATE()
        WHERE config_key = ?
    """;
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, status);
            stmt.setString(2, configKey);
            stmt.executeUpdate();
        }
    }


    public void executeProcedureWithRetry(String procedureName, ErrorLogger errorLogger, String configKey) throws SQLException, InterruptedException {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try (CallableStatement stmt = connection.prepareCall("{CALL " + "db_warehouse.dbo." + procedureName + "}")) {
                stmt.execute();
                return; // Thành công, thoát khỏi phương thức
            } catch (SQLException e) {
                // Nếu đạt số lần retry tối đa, ghi log lỗi và ném ngoại lệ
                if (attempt == MAX_RETRIES) {
                    errorLogger.logError(
                            "Transform",
                            "ExecutionFailed",
                            "Failed to execute procedure " + procedureName + ": " + e.getMessage(),
                            configKey,
                            0
                    );
                    throw e; // Quăng lại ngoại lệ khi hết retry
                }

                // Ghi log retry
                errorLogger.logError(
                        "Transform",
                        "Retry",
                        "Attempt " + attempt + " to execute procedure " + procedureName + " failed: " + e.getMessage(),
                        configKey,
                        0
                );

                // Tạm dừng trước khi thử lại
                Thread.sleep(RETRY_DELAY_MS);
            }
        }
    }


    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}


