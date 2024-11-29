package thu3.ca2.nhom3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.Collectors;

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
			throws SQLException, JsonProcessingException {
		DataValidator validator = new DataValidator(conn);
		int totalIssues = 0;

		if (validator.isValidateNull()) {
			int nullCount = validator.checkNullValues(tableName);
			if (nullCount > 0) {
				controlManager.logDataQualityCheck("STAGING_DATA_VALIDATION", tableName, nullCount, "NULL values found");
				removeInvalidRecords(tableName, "NULL_CHECK", conn);
				totalIssues += nullCount;
			}
		}

		if (validator.isValidateRange()) {
			int rangeCount = validator.checkRangeValues(tableName);
			if (rangeCount > 0) {
				controlManager.logDataQualityCheck("STAGING_DATA_VALIDATION", tableName, rangeCount, "Out of range values found");
				removeInvalidRecords(tableName, "RANGE_CHECK", conn);
				totalIssues += rangeCount;
			}
		}

		if (validator.isValidateDuplicate()) {
			int duplicateCount = validator.checkDuplicates(tableName);
			if (duplicateCount > 0) {
				controlManager.logDataQualityCheck("STAGING_DATA_VALIDATION", tableName, duplicateCount, "Duplicate records found");
				removeInvalidRecords(tableName, "DUPLICATE_CHECK", conn);
				totalIssues += duplicateCount;
			}
		}

		return totalIssues;
	}


	private static void removeInvalidRecords(String tableName, String reason, Connection conn) throws SQLException, JsonProcessingException {
		DataValidator validator = new DataValidator(conn);
		String selectInvalidSQL;
		String deleteSQL;

		switch (reason) {
			case "NULL_CHECK" -> {
				List<String> nullColumns = validator.getNullColumns();
				if (nullColumns.isEmpty()) {
					System.out.println("No NULL_CHECK columns defined in configuration. Skipping removal.");
					return;
				}
				String nullConditions = nullColumns.stream()
						.map(column -> column + " IS NULL")
						.collect(Collectors.joining(" OR "));
				selectInvalidSQL = "SELECT * FROM " + tableName + " WHERE " + nullConditions;
				deleteSQL = "DELETE FROM " + tableName + " WHERE " + nullConditions;
			}
			case "RANGE_CHECK" -> {
				List<String> rangeColumns = validator.getRangeColumns();
				if (rangeColumns.isEmpty()) {
					System.out.println("No RANGE_CHECK columns defined in configuration. Skipping removal.");
					return;
				}
				String rangeConditions = rangeColumns.stream()
						.map(column -> column + " < 0")
						.collect(Collectors.joining(" OR "));
				selectInvalidSQL = "SELECT * FROM " + tableName + " WHERE " + rangeConditions;
				deleteSQL = "DELETE FROM " + tableName + " WHERE " + rangeConditions;
			}
			case "DUPLICATE_CHECK" -> {
				List<String> duplicateColumns = validator.getDuplicateColumns();
				if (duplicateColumns.isEmpty()) {
					System.out.println("No DUPLICATE_CHECK columns defined in configuration. Skipping removal.");
					return;
				}
				String groupByColumns = String.join(", ", duplicateColumns);
				selectInvalidSQL = String.format("""
                SELECT * 
                FROM %s 
                WHERE id NOT IN (
                    SELECT MIN(id) 
                    FROM %s 
                    GROUP BY %s
                )
            """, tableName, tableName, groupByColumns);
				deleteSQL = String.format("""
                DELETE FROM %s 
                WHERE id NOT IN (
                    SELECT MIN(id) 
                    FROM %s 
                    GROUP BY %s
                )
            """, tableName, tableName, groupByColumns);
			}
			default -> throw new IllegalArgumentException("Invalid reason for removing records.");
		}

		// Di chuyển dữ liệu không hợp lệ sang bảng invalid_records
		try (Statement selectStmt = conn.createStatement();
			 Statement deleteStmt = conn.createStatement();
			 ResultSet rs = selectStmt.executeQuery(selectInvalidSQL)) {
			while (rs.next()) {
				String invalidRecord = extractRecordAsJson(rs); // Chuyển bản ghi thành JSON
				String insertSQL = "INSERT INTO invalid_records (table_name, invalid_record, reason) VALUES (?, ?, ?)";
				try (PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {
					insertStmt.setString(1, tableName);
					insertStmt.setString(2, invalidRecord);
					insertStmt.setString(3, reason);
					insertStmt.executeUpdate();
				}
			}
			// Xóa bản ghi không hợp lệ khỏi bảng gốc
			int rowsDeleted = deleteStmt.executeUpdate(deleteSQL);
			System.out.println("Moved and removed " + rowsDeleted + " invalid records due to " + reason);
		}
	}

	// Helper: Chuyển ResultSet thành chuỗi JSON
	private static String extractRecordAsJson(ResultSet rs) throws SQLException, JsonProcessingException {
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();
		Map<String, Object> recordMap = new HashMap<>();
		for (int i = 1; i <= columnCount; i++) {
			recordMap.put(metaData.getColumnName(i), rs.getObject(i));
		}
		return new ObjectMapper().writeValueAsString(recordMap); // Dùng thư viện Jackson để chuyển sang JSON
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
			controlManager.endJob("FAILED", totalRecordsProcessed, e.getMessage());
		} catch (RuntimeException ex) {
			System.err.println("Failed to log job failure: " + ex.getMessage());
			ex.printStackTrace();
		}
		e.printStackTrace();
	}
}

		
