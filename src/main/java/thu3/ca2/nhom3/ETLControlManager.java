package thu3.ca2.nhom3;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Properties;

public class ETLControlManager {
	private static final String CONFIG_FILE = "config.properties";
	private static String controlDbUrl;
	private Connection controlConn;
	private int currentJobId;

	// Constructor
	public ETLControlManager() {
		loadConfigurations();
		initializeConnection();
	}

	// Load configurations from the properties file
	private static void loadConfigurations() {
		try (InputStream input = new FileInputStream(CONFIG_FILE)) {
			Properties props = new Properties();
			props.load(input);
			controlDbUrl = props.getProperty("control.db.url");
		} catch (IOException e) {
			throw new RuntimeException("Failed to load configurations from " + CONFIG_FILE, e);
		}
	}

	// Initialize connection to the control database
	private void initializeConnection() {
		try {
			controlConn = DriverManager.getConnection(controlDbUrl);
		} catch (SQLException e) {
			throw new RuntimeException("Failed to connect to the control database: " + controlDbUrl, e);
		}
	}

	// Start an ETL job and record its metadata
	public void startJob(String jobName) {
		String sql = """
            INSERT INTO etl_job_log (job_name, start_time, status) 
            VALUES (?, ?, ?)
        """;
		try (PreparedStatement pstmt = controlConn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			pstmt.setString(1, jobName);
			pstmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
			pstmt.setString(3, "RUNNING");
			pstmt.executeUpdate();

			try (ResultSet rs = pstmt.getGeneratedKeys()) {
				if (rs.next()) {
					currentJobId = rs.getInt(1);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Failed to start ETL job: " + jobName, e);
		}
	}

	// Log an individual ETL step
	public void logStep(String stepName, LocalDateTime startTime, LocalDateTime endTime, String status,
						int recordsProcessed, String errorMessage) {
		String sql = """
            INSERT INTO etl_step_log 
            (job_id, step_name, start_time, end_time, status, records_processed, error_message) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """;
		try (PreparedStatement pstmt = controlConn.prepareStatement(sql)) {
			pstmt.setInt(1, currentJobId);
			pstmt.setString(2, stepName);
			pstmt.setTimestamp(3, Timestamp.valueOf(startTime));
			pstmt.setTimestamp(4, Timestamp.valueOf(endTime));
			pstmt.setString(5, status);
			pstmt.setInt(6, recordsProcessed);
			pstmt.setString(7, errorMessage);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Failed to log ETL step: " + stepName, e);
		}
	}

	// Log a data quality check
	public void logDataQualityCheck(String checkName, String tableName, int failedRecords, String errorDetails) {
		String sql = """
            INSERT INTO data_quality_log 
            (job_id, check_name, check_time, table_name, failed_records, error_details) 
            VALUES (?, ?, ?, ?, ?, ?)
        """;
		try (PreparedStatement pstmt = controlConn.prepareStatement(sql)) {
			pstmt.setInt(1, currentJobId);
			pstmt.setString(2, checkName);
			pstmt.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
			pstmt.setString(4, tableName);
			pstmt.setInt(5, failedRecords);
			pstmt.setString(6, errorDetails);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Failed to log data quality check: " + checkName, e);
		}
	}

	// End an ETL job with a status
	public void endJob(String status, int recordsProcessed, String errorMessage) {
		String sql = """
            UPDATE etl_job_log 
            SET end_time = ?, status = ?, records_processed = ?, error_message = ? 
            WHERE job_id = ?
        """;
		try (PreparedStatement pstmt = controlConn.prepareStatement(sql)) {
			pstmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
			pstmt.setString(2, status);
			pstmt.setInt(3, recordsProcessed);
			pstmt.setString(4, errorMessage);
			pstmt.setInt(5, currentJobId);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Failed to end ETL job with status: " + status, e);
		}
	}

	// Close the connection to the control database
	public void close() {
		try {
			if (controlConn != null && !controlConn.isClosed()) {
				controlConn.close();
			}
		} catch (SQLException e) {
			System.err.println("Failed to close control database connection: " + e.getMessage());
		}
	}
}
