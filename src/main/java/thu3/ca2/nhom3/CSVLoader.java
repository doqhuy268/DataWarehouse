package thu3.ca2.nhom3;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
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

			// Kiểm tra nếu dòng đã tồn tại
			String selectSql = "SELECT id, price FROM staging_mobile " +
					"WHERE name = ? AND brand = ? AND model = ?";

			// Chèn dữ liệu mới
			String insertSql = "INSERT INTO staging_mobile (" +
					"name, brand, model, battery_capacity, screen_size, touchscreen, resolution_x, resolution_y, " +
					"processor, ram, internal_storage, rear_camera, front_camera, operating_system, price, loaded_date) " +
					"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())";

			// Cập nhật giá nếu khác nhau
			String updateSql = "UPDATE staging_mobile SET price = ?, last_updated = GETDATE() WHERE id = ?";

			// Ghi log cập nhật giá
			String logSql = "INSERT INTO price_update_log (mobile_id, old_price, new_price, updated_at) " +
					"VALUES (?, ?, ?, GETDATE())";

			try (PreparedStatement selectStmt = conn.prepareStatement(selectSql);
				 PreparedStatement insertStmt = conn.prepareStatement(insertSql);
				 PreparedStatement updateStmt = conn.prepareStatement(updateSql);
				 PreparedStatement logStmt = conn.prepareStatement(logSql)) {

				int batchSize = 500;
				int batchCount = 0;

				for (String[] record : records) {
					try {
						String name = record[0];
						String brand = record[1];
						String model = record[2];
						double price = Double.parseDouble(record[14]);

						// Kiểm tra nếu dữ liệu đã tồn tại
						selectStmt.setString(1, name);
						selectStmt.setString(2, brand);
						selectStmt.setString(3, model);

						ResultSet rs = selectStmt.executeQuery();
						if (rs.next()) {
							int id = rs.getInt("id");
							double oldPrice = rs.getDouble("price");

							// Nếu giá thay đổi, cập nhật và ghi log
							if (oldPrice != price) {
								updateStmt.setDouble(1, price);
								updateStmt.setInt(2, id);
								updateStmt.addBatch();

								logStmt.setInt(1, id);
								logStmt.setDouble(2, oldPrice);
								logStmt.setDouble(3, price);
								logStmt.addBatch();

								batchCount++;
							}
						} else {
							// Thêm dữ liệu mới
							insertStmt.setString(1, name);
							insertStmt.setString(2, brand);
							insertStmt.setString(3, model);
							insertStmt.setInt(4, Integer.parseInt(record[3])); // battery_capacity
							insertStmt.setDouble(5, Double.parseDouble(record[4])); // screen_size
							insertStmt.setBoolean(6, Boolean.parseBoolean(record[5])); // touchscreen
							insertStmt.setInt(7, Integer.parseInt(record[6])); // resolution_x
							insertStmt.setInt(8, Integer.parseInt(record[7])); // resolution_y
							insertStmt.setString(9, record[8]); // processor
							insertStmt.setInt(10, Integer.parseInt(record[9])); // ram
							insertStmt.setInt(11, Integer.parseInt(record[10])); // internal_storage
							insertStmt.setString(12, record[11]); // rear_camera
							insertStmt.setString(13, record[12]); // front_camera
							insertStmt.setString(14, record[13]); // operating_system
							insertStmt.setDouble(15, price); // price

							insertStmt.addBatch();
							recordsLoaded++;
							batchCount++;
						}

						if (batchCount % batchSize == 0) {
							insertStmt.executeBatch();
							updateStmt.executeBatch();
							logStmt.executeBatch();
							batchCount = 0;
						}
					} catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
						System.err.println("Error parsing record: " + String.join(", ", record));
						e.printStackTrace();
					}
				}

				// Thực thi các batch còn lại
				insertStmt.executeBatch();
				updateStmt.executeBatch();
				logStmt.executeBatch();
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

