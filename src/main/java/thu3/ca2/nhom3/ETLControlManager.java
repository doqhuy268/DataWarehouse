package thu3.ca2.nhom3;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Properties;

public class ETLControlManager {
	private static final String CONFIG_FILE = "config.properties";
	private static String controlDb;
	private Connection controlConn;
	private int currentJobId;

	public ETLControlManager() {
		loadConfigurations();
		try {
			controlConn = DriverManager.getConnection(controlDb);
		} catch (SQLException e) {
			throw new RuntimeException("Failed to connect to control database", e);
		}
	}

	private static void loadConfigurations() {
		try (InputStream input = new FileInputStream(CONFIG_FILE)) {
			Properties props = new Properties();
			props.load(input);
			controlDb = props.getProperty("control.db.url");
		} catch (IOException e) {
			throw new RuntimeException("Failed to load configurations.", e);
		}
	}

	public void startJob(String jobName) throws SQLException {
		String sql = "INSERT INTO etl_job_log (job_name, start_time, status) VALUES (?, ?, ?)";
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
		}
	}

	public void logStep(String stepName, LocalDateTime startTime, LocalDateTime endTime, String status,
			int recordsProcessed, String errorMessage) throws SQLException {
		String sql = "INSERT INTO etl_step_log (job_id, step_name, start_time, end_time, status, records_processed, error_message) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?)";
		try (PreparedStatement pstmt = controlConn.prepareStatement(sql)) {
			pstmt.setInt(1, currentJobId);
			pstmt.setString(2, stepName);
			pstmt.setTimestamp(3, Timestamp.valueOf(startTime));
			pstmt.setTimestamp(4, Timestamp.valueOf(endTime));
			pstmt.setString(5, status);
			pstmt.setInt(6, recordsProcessed);
			pstmt.setString(7, errorMessage);
			pstmt.executeUpdate();
		}
	}

	public void logDataQualityCheck(String checkName, String tableName, int failedRecords, String errorDetails)
			throws SQLException {
		String sql = "INSERT INTO data_quality_log (job_id, check_name, check_time, table_name, failed_records, error_details) "
				+ "VALUES (?, ?, ?, ?, ?, ?)";
		try (PreparedStatement pstmt = controlConn.prepareStatement(sql)) {
			pstmt.setInt(1, currentJobId);
			pstmt.setString(2, checkName);
			pstmt.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
			pstmt.setString(4, tableName);
			pstmt.setInt(5, failedRecords);
			pstmt.setString(6, errorDetails);
			pstmt.executeUpdate();
		}
	}

	public void endJob(String status, int recordsProcessed, String errorMessage) throws SQLException {
		String sql = "UPDATE etl_job_log SET end_time = ?, status = ?, records_processed = ?, error_message = ? WHERE job_id = ?";
		try (PreparedStatement pstmt = controlConn.prepareStatement(sql)) {
			pstmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
			pstmt.setString(2, status);
			pstmt.setInt(3, recordsProcessed);
			pstmt.setString(4, errorMessage);
			pstmt.setInt(5, currentJobId);
			pstmt.executeUpdate();
		}
	}

	public void close() {
		try {
			if (controlConn != null && !controlConn.isClosed()) {
				controlConn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
