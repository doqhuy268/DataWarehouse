package thu3.ca2.nhom3;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class CSVLoader {
	private static final String CONFIG_FILE = "config.properties";
	private String csvFilePath;
	private String stagingDbUrl;

	public CSVLoader() {
		loadConfigurations();
	}

	private void loadConfigurations() {
		Properties props = new Properties();
		try (FileReader reader = new FileReader(CONFIG_FILE)) {
			props.load(reader);
			csvFilePath = props.getProperty("csv.file.path");
			stagingDbUrl = props.getProperty("staging.db.url");
		} catch (IOException e) {
			throw new RuntimeException("Failed to load configuration file", e);
		}
	}

	public int loadCSVToStaging() {
		int recordsLoaded = 0;
		try (CSVReader reader = new CSVReader(new FileReader(csvFilePath));
			 Connection conn = DriverManager.getConnection(stagingDbUrl)) {

			List<String[]> records = reader.readAll();
			records.remove(0); // Bỏ qua hàng tiêu đề

			String sql = "INSERT INTO staging_mobile (" +
					"name, brand, model, battery_capacity, screen_size, touchscreen, resolution_x, resolution_y, " +
					"processor, ram, internal_storage, rear_camera, front_camera, operating_system, price) " +
					"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				int batchSize = 500;
				int batchCount = 0;

				for (String[] record : records) {
					try {
						pstmt.setString(1, record[0]); // name
						pstmt.setString(2, record[1]); // brand
						pstmt.setString(3, record[2]); // model
						pstmt.setInt(4, Integer.parseInt(record[3])); // battery_capacity
						pstmt.setDouble(5, Double.parseDouble(record[4])); // screen_size
						pstmt.setBoolean(6, Boolean.parseBoolean(record[5])); // touchscreen
						pstmt.setInt(7, Integer.parseInt(record[6])); // resolution_x
						pstmt.setInt(8, Integer.parseInt(record[7])); // resolution_y
						pstmt.setString(9, record[8]); // processor
						pstmt.setInt(10, Integer.parseInt(record[9])); // ram
						pstmt.setInt(11, Integer.parseInt(record[10])); // internal_storage
						pstmt.setString(12, record[11]); // rear_camera
						pstmt.setString(13, record[12]); // front_camera
						pstmt.setString(14, record[13]); // operating_system
						pstmt.setDouble(15, Double.parseDouble(record[14])); // price

						pstmt.addBatch();
						recordsLoaded++;
						batchCount++;

						if (batchCount % batchSize == 0) {
							pstmt.executeBatch();
						}
					} catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
						System.err.println("Error parsing record: " + String.join(", ", record));
						e.printStackTrace();
					}
				}

				// Execute any remaining batch
				pstmt.executeBatch();
			}
		} catch (IOException | CsvException | SQLException e) {
			e.printStackTrace();
		}
		return recordsLoaded;
	}

	private int recordsProcessed = 0;

	public void loadCSV(String filePath, String tableName) throws Exception {
		try (BufferedReader br = new BufferedReader(new FileReader(filePath));
			 Connection connection = DriverManager.getConnection(stagingDbUrl)) {
			String line;
			String sql = "INSERT INTO " + tableName + " (id, name, brand, model, battery_capacity, screen_size, " +
					"touchscreen, resolution_x, resolution_y, processor, ram, internal_storage, rear_camera, " +
					"front_camera, operating_system, price, loaded_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

			PreparedStatement preparedStatement = connection.prepareStatement(sql);

			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				for (int i = 0; i < values.length; i++) {
					preparedStatement.setString(i + 1, values[i]);
				}
				preparedStatement.addBatch();
				recordsProcessed++;
			}

			preparedStatement.executeBatch();
		}
	}

	public int getRecordsProcessed() {
		return recordsProcessed;
	}
}

