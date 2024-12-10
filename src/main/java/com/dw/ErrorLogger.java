package com.dw;

import java.sql.*;

public class ErrorLogger implements AutoCloseable {
    private final Connection connection;

    public ErrorLogger(String dbUrl) throws SQLException {
        this.connection = DriverManager.getConnection(dbUrl);
    }

    public void logError(String phase, String errorType, String errorMessage, String sourceFile, int rowNumber) {
        String storedProc = "{call staging.dbo.sp_insert_error_log(?, ?, ?, ?, ?)}";
        try (CallableStatement stmt = connection.prepareCall(storedProc)) {
            stmt.setString(1, phase);
            stmt.setString(2, errorType);
            stmt.setString(3, errorMessage);
            stmt.setString(4, sourceFile);
            stmt.setInt(5, rowNumber);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to log error to database", e);
        }
    }


    public void logSummary(String filePath, int totalRows, int successfulRows, int failedRows, long processingTime) {
        String query = """
        INSERT INTO staging.dbo.processing_summary
        (FileName, TotalRows, SuccessfulRows, FailedRows, ProcessingTime, ProcessedDate)
        VALUES (?, ?, ?, ?, ?, GETDATE())
    """;
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, filePath);
            stmt.setInt(2, totalRows);
            stmt.setInt(3, successfulRows);
            stmt.setInt(4, failedRows);
            stmt.setLong(5, processingTime);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to log processing summary", e);
        }
    }


    @Override
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}


