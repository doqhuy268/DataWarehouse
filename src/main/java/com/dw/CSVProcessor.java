package com.dw;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CSVProcessor implements AutoCloseable {
    private Connection connection;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 2000;
    private final ErrorLogger errorLogger;

    public CSVProcessor(String dbUrl, ErrorLogger errorLogger) throws SQLException {
        this.errorLogger = errorLogger;
        try {
            this.connection = DriverManager.getConnection(dbUrl);
        } catch (SQLException e) {
            errorLogger.logError("Connection", "CONNECTION_ERROR", "Database connection failed: " + e.getMessage(), "N/A", 0);
            throw e;
        }
    }

    public void loadCSVToDailyMobile(String filePath) throws Exception {
        long startTime = System.currentTimeMillis();
        updateFileStatus(filePath, "EX");

        int rowNumber = 0;
        int successfulRows = 0;
        int failedRows = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean isFirstLine = true;

            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }

                rowNumber++;
                String[] values = line.split(",");

                try {
                    insertToStaging(values, filePath, rowNumber);
                    successfulRows++;
                } catch (Exception e) {
                    failedRows++;
                    errorLogger.logError("Transform", "ROW_PROCESSING_ERROR", "Error processing row: " + e.getMessage(), filePath, rowNumber);
                }
            }

            runStoredProcedures(filePath);

            long endTime = System.currentTimeMillis();
            errorLogger.logSummary(filePath, rowNumber, successfulRows, failedRows, endTime - startTime);
        } catch (Exception e) {
            updateFileStatus(filePath, "ER");
            errorLogger.logError("Extract", "FILE_READ_ERROR", "Error reading file: " + e.getMessage(), filePath, 0);
            throw e;
        }
    }

    private void insertToStaging(String[] values, String filePath, int rowNumber) throws SQLException {
        String query = """
        INSERT INTO daily_mobile 
        (raw_name, raw_brand, raw_model, raw_battery_capacity, raw_screen_size, 
         raw_touchscreen, raw_resolution_x, raw_resolution_y, raw_processor, 
         raw_ram, raw_internal_storage, raw_rear_camera, raw_front_camera, 
         raw_operating_system, raw_price, source_file_name, row_number)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, values[0]);   // raw_name
            stmt.setString(2, values[1]);   // raw_brand
            stmt.setString(3, values[2]);   // raw_model
            stmt.setString(4, values[3]);   // raw_battery_capacity
            stmt.setString(5, values[4]);   // raw_screen_size
            stmt.setBoolean(6, values[5].equalsIgnoreCase("Yes")); // raw_touchscreen
            stmt.setString(7, values[6]);   // raw_resolution_x
            stmt.setString(8, values[7]);   // raw_resolution_y
            stmt.setString(9, values[8]);   // raw_processor
            stmt.setString(10, values[9]);  // raw_ram
            stmt.setString(11, values[10]); // raw_internal_storage
            stmt.setString(12, values[11]); // raw_rear_camera
            stmt.setString(13, values[12]); // raw_front_camera
            stmt.setString(14, values[13]); // raw_operating_system
            stmt.setString(15, values[14]); // raw_price
            stmt.setString(16, filePath);   // source_file_name
            stmt.setInt(17, rowNumber);     // row_number

            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new SQLException("Database insertion error: " + e.getMessage());
        }
    }

    private void runStoredProcedures(String filePath) throws Exception {
        String[] procedures = {
                "UpdateDailyMobileStatus",
                "sp_filter_valid_daily_mobile",
                "sp_merge_valid_to_staging"
        };

        try {
            connection.setAutoCommit(false);

            for (String procedure : procedures) {
                retryProcedure(procedure, filePath);
            }

            connection.commit();
            updateFileStatus(filePath, "TR");
        } catch (Exception e) {
            connection.rollback();
            updateFileStatus(filePath, "ER");
            errorLogger.logError("Transform", "STORED_PROCEDURE_ERROR", "Stored procedures execution failed: " + e.getMessage(), filePath, 0);
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }


    private void retryProcedure(String procedureName, String filePath) throws Exception {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                try (CallableStatement stmt = connection.prepareCall("{CALL " + procedureName + "}")) {
                    stmt.execute();
                }
                return;
            } catch (SQLException e) {
                errorLogger.logError("Transform", "PROCEDURE_EXECUTION_ERROR",
                        "Error executing " + procedureName + " on attempt " + attempt + ": " + e.getMessage(), filePath, 0);

                if (attempt == MAX_RETRIES) {
                    throw e;
                }

                Thread.sleep(RETRY_DELAY_MS);
            }
        }
    }


    private void updateFileStatus(String filePath, String status) {
        String query = "{call db_control.dbo.UpdateFileStatus(?, ?)}";
        try (PreparedStatement stmt = connection.prepareCall(query)) {
            stmt.setString(1, filePath);
            stmt.setString(2, status);
            stmt.execute();
        } catch (SQLException e) {
            System.err.println("Failed to update file status: " + e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
