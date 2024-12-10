package com.dw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

public class MainApp {
    private static final Logger logger = LoggerFactory.getLogger(MainApp.class);
    private static final String STATUS_EXTRACTED = "EX";
    private static final String STATUS_TRANSFORMED = "TR";
    private static final String STATUS_LOADED = "LD";
    private static final String STATUS_ERROR = "ER";

    public static void main(String[] args) {
        try {
            // Load configuration
            String configFilePath = System.getProperty("user.dir") + "/db-config.properties";
            DatabaseConfig.loadFromFile(configFilePath);

            // Database connections
            String controlDbUrl = DatabaseConfig.getControlDbUrl();
            String warehouseDbUrl = DatabaseConfig.getWarehouseDbUrl();
            String stagingDbUrl = DatabaseConfig.getStagingDbUrl();

            // Error logger
            ErrorLogger errorLogger = new ErrorLogger(stagingDbUrl);

            // Use try-with-resources to ensure proper resource management
            try (DatabaseHelper dbHelper = new DatabaseHelper(controlDbUrl)) {
                // Get list of pending files
                List<FileConfig> pendingFiles = dbHelper.getPendingFileConfigs();

                if (pendingFiles.isEmpty()) {
                    logger.info("No pending files to process.");
                    return;
                }

                // Process each file
                for (FileConfig fileConfig : pendingFiles) {
                    processFile(dbHelper, fileConfig, stagingDbUrl, warehouseDbUrl, errorLogger);
                }
            }
        } catch (Exception e) {
            logger.error("Critical error in main application", e);
        }
    }

    private static void processFile(DatabaseHelper dbHelper, FileConfig fileConfig,
                                    String stagingDbUrl, String warehouseDbUrl, ErrorLogger errorLogger) {
        String filePath = fileConfig.getConfigValue();
        String configKey = fileConfig.getConfigKey();

        logger.info("Starting to process file: {}", filePath);

        try {
            validateFile(filePath, configKey, dbHelper, errorLogger);
            updateFileStatus(dbHelper, configKey, STATUS_EXTRACTED);

            extractData(filePath, stagingDbUrl, configKey, dbHelper, errorLogger);
            updateFileStatus(dbHelper, configKey, STATUS_TRANSFORMED);

            loadToFactTable(warehouseDbUrl, configKey, dbHelper, errorLogger);
            updateFileStatus(dbHelper, configKey, STATUS_LOADED);

            logger.info("File processed successfully: {}", filePath);
        } catch (Exception e) {
            handleError(e, dbHelper, errorLogger, configKey, filePath);
        } finally {
            logger.info("Finished processing file: {}", filePath);
        }
    }

    private static void validateFile(String filePath, String configKey, DatabaseHelper dbHelper, ErrorLogger errorLogger) throws Exception {
        File file = new File(filePath);
        if (!file.exists() || !file.canRead()) {
            String errorMessage = "File not found or unreadable: " + filePath;
            errorLogger.logError("Validation", "FileError", errorMessage, filePath, 0);
            throw new Exception(errorMessage);
        }
    }

    private static void extractData(String filePath, String stagingDbUrl, String configKey,
                                    DatabaseHelper dbHelper, ErrorLogger errorLogger) throws Exception {
        try (CSVProcessor csvProcessor = new CSVProcessor(stagingDbUrl, errorLogger)) {
            csvProcessor.loadCSVToDailyMobile(filePath);
        } catch (Exception e) {
            String errorMessage = "Error during extraction: " + e.getMessage();
            errorLogger.logError("Extraction", "DataError", errorMessage, filePath, 0);
            throw e;
        }
    }

    private static void loadToFactTable(String warehouseDbUrl, String configKey,
                                        DatabaseHelper dbHelper, ErrorLogger errorLogger) throws SQLException, InterruptedException {
        try {
            dbHelper.executeProcedureWithRetry("sp_load_data_to_fact", errorLogger, configKey);
        } catch (SQLException e) {
            String errorMsg = "Failed to load data to Fact table: " + e.getMessage();
            errorLogger.logError("LoadToFact", "SQLExecutionError", errorMsg, configKey, 0);
            throw e;
        }
    }


    private static void updateFileStatus(DatabaseHelper dbHelper, String configKey, String status) throws SQLException {
        dbHelper.updateFileStatus(configKey, status);
    }

    private static void handleError(Exception e, DatabaseHelper dbHelper, ErrorLogger errorLogger,
                                    String configKey, String filePath) {
        String errorMessage = "Error processing file: " + filePath + ". Error: " + e.getMessage();
        logger.error(errorMessage, e);

        try {
            errorLogger.logError("Processing", "CriticalError", errorMessage, filePath, 0);
            dbHelper.updateFileStatus(configKey, STATUS_ERROR);
        } catch (SQLException logException) {
            logger.error("Failed to log error to database", logException);
        }
    }
}