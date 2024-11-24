package thu3.ca2.nhom3;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Properties;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Properties;

public class DataWarehouseETL {

	private static final String CONFIG_FILE = "config.properties";
	private static String stagingDb;

	public static void main(String[] args) {
		loadConfigurations();
		ETLControlManager controlManager = new ETLControlManager();
		int totalRecordsProcessed = 0;

		try (Connection conn = DriverManager.getConnection(stagingDb)) {
			// Bắt đầu công việc ETL
			controlManager.startJob("Mobile_Data_ETL");

			// Bước 1: Load CSV vào staging
			totalRecordsProcessed = loadCSVToStaging(controlManager);

			// Bước 2: Kiểm tra chất lượng dữ liệu và xóa bản ghi không hợp lệ
			int invalidRecords = validateDataQuality("staging_mobile", conn, controlManager);
			System.out.println("Total invalid records found and removed: " + invalidRecords);

			// Bước 3: Chuyển đổi và load vào warehouse
			transformAndLoadToWarehouse(controlManager);

			// Kết thúc công việc ETL
			controlManager.endJob("COMPLETED", totalRecordsProcessed, null);

		} catch (Exception e) {
			handleJobFailure(controlManager, totalRecordsProcessed, e);
		} finally {
			controlManager.close();
		}
	}

	private static void loadConfigurations() {
		try (InputStream input = new FileInputStream(CONFIG_FILE)) {
			Properties props = new Properties();
			props.load(input);
			stagingDb = props.getProperty("staging.db.url");
		} catch (IOException e) {
			throw new RuntimeException("Failed to load configurations.", e);
		}
	}

	private static int loadCSVToStaging(ETLControlManager controlManager) throws SQLException {
		int recordsLoaded = 0;
		try {
			CSVLoader loader = new CSVLoader();
			recordsLoaded = loader.loadCSVToStaging();
			LocalDateTime startTime = LocalDateTime.now();
			LocalDateTime endTime = LocalDateTime.now();
			controlManager.logStep("CSV_TO_STAGING", startTime, endTime, "COMPLETED", recordsLoaded, null);
		} catch (Exception e) {
			LocalDateTime startTime = LocalDateTime.now();
			LocalDateTime endTime = LocalDateTime.now();
			controlManager.logStep("CSV_TO_STAGING", startTime, endTime, "FAILED", 0, e.getMessage());
			throw e;
		}
		return recordsLoaded;
	}

	private static int validateDataQuality(String tableName, Connection conn, ETLControlManager controlManager)
			throws SQLException {
		DataValidator validator = new DataValidator();
		int nullCount = validator.checkNullValues(tableName, conn);
		int rangeCount = validator.checkRangeValues(tableName, conn);
		int duplicateCount = validator.checkDuplicates(tableName, conn);

		// Log kết quả kiểm tra
		if (nullCount > 0) {
			controlManager.logDataQualityCheck("STAGING_DATA_VALIDATION", tableName, nullCount, "NULL values found");
			removeInvalidRecords(tableName, "NULL_CHECK", conn);
		}
		if (rangeCount > 0) {
			controlManager.logDataQualityCheck("STAGING_DATA_VALIDATION", tableName, rangeCount, "Out of range values found");
			removeInvalidRecords(tableName, "RANGE_CHECK", conn);
		}
		if (duplicateCount > 0) {
			controlManager.logDataQualityCheck("STAGING_DATA_VALIDATION", tableName, duplicateCount, "Duplicate records found");
			removeInvalidRecords(tableName, "DUPLICATE_CHECK", conn);
		}

		return nullCount + rangeCount + duplicateCount;
	}


	private static void removeInvalidRecords(String tableName, String reason, Connection conn) throws SQLException {
		String deleteSQL = switch (reason) {
			case "NULL_CHECK" -> """
            DELETE FROM %s
            WHERE name IS NULL OR brand IS NULL OR price IS NULL
        """.formatted(tableName); // Các cột bắt buộc không được NULL
			case "RANGE_CHECK" -> """
            DELETE FROM %s
            WHERE battery_capacity <= 0 OR price < 0 OR screen_size <= 0
        """.formatted(tableName); // Điều kiện giá trị hợp lệ
			case "DUPLICATE_CHECK" -> """
            DELETE FROM %s
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM %s
                GROUP BY name, brand, model, price
            )
        """.formatted(tableName, tableName); // Loại bỏ các bản ghi trùng
			default -> throw new IllegalArgumentException("Invalid reason for removing records.");
		};

		try (Statement stmt = conn.createStatement()) {
			int rowsDeleted = stmt.executeUpdate(deleteSQL);
			System.out.println("Removed " + rowsDeleted + " invalid records due to " + reason);
		}
	}


	private static void transformAndLoadToWarehouse(ETLControlManager controlManager) throws SQLException {
		try {
			DataTransformer transformer = new DataTransformer();
			int recordsTransformed = transformer.transformData();
			LocalDateTime startTime = LocalDateTime.now();
			LocalDateTime endTime = LocalDateTime.now();
			controlManager.logStep("TRANSFORM_TO_WAREHOUSE", startTime, endTime, "COMPLETED", recordsTransformed, null);
		} catch (Exception e) {
			LocalDateTime startTime = LocalDateTime.now();
			LocalDateTime endTime = LocalDateTime.now();
			controlManager.logStep("TRANSFORM_TO_WAREHOUSE", startTime, endTime, "FAILED", 0, e.getMessage());
			throw e;
		}
	}

	private static void handleJobFailure(ETLControlManager controlManager, int totalRecordsProcessed, Exception e) {
		try {
			LocalDateTime endJobTime = LocalDateTime.now();
			controlManager.endJob("FAILED", totalRecordsProcessed, e.getMessage());
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		e.printStackTrace();
	}
}
