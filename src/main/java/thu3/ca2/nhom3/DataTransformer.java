package thu3.ca2.nhom3;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class DataTransformer {
    private String stagingDbUrl;
    private String warehouseDbUrl;

    public DataTransformer() {
        loadConfig();
    }

    private void loadConfig() {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            properties.load(fis);
            stagingDbUrl = properties.getProperty("staging.db.url");
            warehouseDbUrl = properties.getProperty("warehouse.db.url");
        } catch (IOException e) {
            System.err.println("Error loading config.properties: " + e.getMessage());
            throw new RuntimeException("Failed to load database configuration", e);
        }
    }

    public int transformData() {
        int recordsTransformed = 0;

        try (Connection stagingConn = DriverManager.getConnection(stagingDbUrl);
             Connection warehouseConn = DriverManager.getConnection(warehouseDbUrl)) {

            recordsTransformed += loadDimBrand(stagingConn, warehouseConn);
            recordsTransformed += loadDimModel(stagingConn, warehouseConn);
            recordsTransformed += loadDimSpecification(stagingConn, warehouseConn);
            recordsTransformed += loadDimProcessor(stagingConn, warehouseConn);
            recordsTransformed += loadDimCamera(stagingConn, warehouseConn);
            recordsTransformed += loadDimOS(stagingConn, warehouseConn);
            recordsTransformed += loadFactPhone(stagingConn, warehouseConn);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return recordsTransformed;
    }


    private int loadDimBrand(Connection stagingConn, Connection warehouseConn) throws SQLException {
        int recordsInserted = 0;
        String selectSQL = "SELECT DISTINCT brand FROM staging_mobile";
        String checkSQL = "SELECT 1 FROM DimBrand WHERE BrandName = ?";
        String insertSQL = "INSERT INTO DimBrand (BrandName) VALUES (?)";

        try (Statement stmt = stagingConn.createStatement();
             PreparedStatement checkStmt = warehouseConn.prepareStatement(checkSQL);
             PreparedStatement insertStmt = warehouseConn.prepareStatement(insertSQL)) {

            ResultSet rs = stmt.executeQuery(selectSQL);
            while (rs.next()) {
                String brand = rs.getString("brand");

                // Kiểm tra sự tồn tại
                checkStmt.setString(1, brand);
                try (ResultSet checkRs = checkStmt.executeQuery()) {
                    if (!checkRs.next()) { // Nếu không tồn tại
                        insertStmt.setString(1, brand);
                        recordsInserted += insertStmt.executeUpdate();
                    }
                }
            }
        }
        return recordsInserted;
    }


    private int loadDimModel(Connection stagingConn, Connection warehouseConn) throws SQLException {
        int recordsInserted = 0;
        String selectSQL = "SELECT DISTINCT model, brand FROM staging_mobile";
        String checkSQL = "SELECT 1 FROM DimModel WHERE ModelName = ? AND BrandKey = ?";
        String insertSQL = "INSERT INTO DimModel (ModelName, BrandKey) VALUES (?, ?)";

        try (Statement stmt = stagingConn.createStatement();
             PreparedStatement checkStmt = warehouseConn.prepareStatement(checkSQL);
             PreparedStatement insertStmt = warehouseConn.prepareStatement(insertSQL)) {

            ResultSet rs = stmt.executeQuery(selectSQL);
            while (rs.next()) {
                String model = rs.getString("model");
                int brandKey = getDimKey("DimBrand", "BrandName", rs.getString("brand"), warehouseConn);

                // Kiểm tra sự tồn tại
                checkStmt.setString(1, model);
                checkStmt.setInt(2, brandKey);
                try (ResultSet checkRs = checkStmt.executeQuery()) {
                    if (!checkRs.next()) { // Nếu không tồn tại
                        insertStmt.setString(1, model);
                        insertStmt.setInt(2, brandKey);
                        recordsInserted += insertStmt.executeUpdate();
                    }
                }
            }
        }
        return recordsInserted;
    }


    private int loadDimSpecification(Connection stagingConn, Connection warehouseConn) throws SQLException {
        int recordsInserted = 0;
        String selectSQL = "SELECT DISTINCT battery_capacity, screen_size, touchscreen, resolution_x, resolution_y, ram, internal_storage FROM staging_mobile";
        String checkSQL = "SELECT 1 FROM DimSpecification WHERE BatteryCapacity = ? AND ScreenSize = ? AND Touchscreen = ? AND ResolutionX = ? AND ResolutionY = ? AND RAM = ? AND InternalStorage = ?";
        String insertSQL = "INSERT INTO DimSpecification (BatteryCapacity, ScreenSize, Touchscreen, ResolutionX, ResolutionY, RAM, InternalStorage) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Statement stmt = stagingConn.createStatement();
             PreparedStatement checkStmt = warehouseConn.prepareStatement(checkSQL);
             PreparedStatement insertStmt = warehouseConn.prepareStatement(insertSQL)) {

            ResultSet rs = stmt.executeQuery(selectSQL);
            while (rs.next()) {
                checkStmt.setInt(1, rs.getInt("battery_capacity"));
                checkStmt.setDouble(2, rs.getDouble("screen_size"));
                checkStmt.setBoolean(3, rs.getBoolean("touchscreen"));
                checkStmt.setInt(4, rs.getInt("resolution_x"));
                checkStmt.setInt(5, rs.getInt("resolution_y"));
                checkStmt.setInt(6, rs.getInt("ram"));
                checkStmt.setInt(7, rs.getInt("internal_storage"));

                try (ResultSet checkRs = checkStmt.executeQuery()) {
                    if (!checkRs.next()) { // Nếu không tồn tại
                        insertStmt.setInt(1, rs.getInt("battery_capacity"));
                        insertStmt.setDouble(2, rs.getDouble("screen_size"));
                        insertStmt.setBoolean(3, rs.getBoolean("touchscreen"));
                        insertStmt.setInt(4, rs.getInt("resolution_x"));
                        insertStmt.setInt(5, rs.getInt("resolution_y"));
                        insertStmt.setInt(6, rs.getInt("ram"));
                        insertStmt.setInt(7, rs.getInt("internal_storage"));
                        recordsInserted += insertStmt.executeUpdate();
                    }
                }
            }
        }
        return recordsInserted;
    }



    private int loadFactPhone(Connection stagingConn, Connection warehouseConn) throws SQLException {
        int recordsInserted = 0;
        String selectSQL = "SELECT * FROM staging_mobile";
        String checkSQL = "SELECT PhoneKey, Price FROM FactPhone WHERE ModelKey = ? AND SpecKey = ? AND ProcessorKey = ? AND CameraKey = ? AND OSKey = ?";
        String insertSQL = "INSERT INTO FactPhone (ModelKey, SpecKey, ProcessorKey, CameraKey, OSKey, Price, CreatedDate, UpdatedDate) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), NULL)";
        String updateSQL = "UPDATE FactPhone SET Price = ?, UpdatedDate = GETDATE() WHERE PhoneKey = ?";

        try (Statement stmt = stagingConn.createStatement();
             PreparedStatement checkStmt = warehouseConn.prepareStatement(checkSQL);
             PreparedStatement insertStmt = warehouseConn.prepareStatement(insertSQL);
             PreparedStatement updateStmt = warehouseConn.prepareStatement(updateSQL)) {

            ResultSet rs = stmt.executeQuery(selectSQL);
            while (rs.next()) {
                int modelKey = getDimKey("DimModel", "ModelName", rs.getString("model"), warehouseConn);
                int specKey = getDimKey("DimSpecification", "ScreenSize", rs.getString("screen_size"), warehouseConn);
                int processorKey = getDimKey("DimProcessor", "ProcessorName", rs.getString("processor"), warehouseConn);
                int cameraKey = getDimKey("DimCamera", "RearCamera", rs.getString("rear_camera"), warehouseConn);
                int osKey = getDimKey("DimOS", "OSName", rs.getString("operating_system"), warehouseConn);

                double newPrice = rs.getDouble("price");

                // Kiểm tra bản ghi hiện tại
                checkStmt.setInt(1, modelKey);
                checkStmt.setInt(2, specKey);
                checkStmt.setInt(3, processorKey);
                checkStmt.setInt(4, cameraKey);
                checkStmt.setInt(5, osKey);

                try (ResultSet checkRs = checkStmt.executeQuery()) {
                    if (checkRs.next()) {
                        // Nếu đã tồn tại, kiểm tra giá
                        int phoneKey = checkRs.getInt("PhoneKey");
                        double oldPrice = checkRs.getDouble("Price");

                        if (oldPrice != newPrice) {
                            // Cập nhật giá nếu khác nhau
                            updateStmt.setDouble(1, newPrice);
                            updateStmt.setInt(2, phoneKey);
                            updateStmt.executeUpdate();

                            // Ghi log cập nhật giá
                            logPriceUpdate(warehouseConn, phoneKey, oldPrice, newPrice);
                        }
                    } else {
                        // Nếu chưa tồn tại, thêm mới
                        insertStmt.setInt(1, modelKey);
                        insertStmt.setInt(2, specKey);
                        insertStmt.setInt(3, processorKey);
                        insertStmt.setInt(4, cameraKey);
                        insertStmt.setInt(5, osKey);
                        insertStmt.setDouble(6, newPrice);
                        recordsInserted += insertStmt.executeUpdate();
                    }
                }
            }
        }
        return recordsInserted;
    }


    private int getFactKey(Connection warehouseConn, int modelKey, int specKey, int processorKey, int cameraKey, int osKey) throws SQLException {
        String query = "SELECT PhoneKey FROM FactPhone WHERE ModelKey = ? AND SpecKey = ? AND ProcessorKey = ? AND CameraKey = ? AND OSKey = ?";
        try (PreparedStatement pstmt = warehouseConn.prepareStatement(query)) {
            pstmt.setInt(1, modelKey);
            pstmt.setInt(2, specKey);
            pstmt.setInt(3, processorKey);
            pstmt.setInt(4, cameraKey);
            pstmt.setInt(5, osKey);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("PhoneKey"); // Sử dụng PhoneKey thay vì fact_id
                }
            }
        }
        return -1; // Trả về -1 nếu không tìm thấy
    }

    private void logPriceUpdate(Connection warehouseConn, int phoneKey, double oldPrice, double newPrice) throws SQLException {
        String logSQL = "INSERT INTO fact_price_update_log (fact_id, old_price, new_price, updated_at) VALUES (?, ?, ?, GETDATE())";
        try (PreparedStatement logStmt = warehouseConn.prepareStatement(logSQL)) {
            logStmt.setInt(1, phoneKey);
            logStmt.setDouble(2, oldPrice);
            logStmt.setDouble(3, newPrice);
            logStmt.executeUpdate();
        }
    }



    private int loadDimProcessor(Connection stagingConn, Connection warehouseConn) throws SQLException {
        int recordsInserted = 0;
        String selectSQL = "SELECT DISTINCT processor FROM staging_mobile";
        String checkSQL = "SELECT COUNT(*) FROM DimProcessor WHERE ProcessorName = ?";
        String insertSQL = "INSERT INTO DimProcessor (ProcessorName) VALUES (?)";

        try (Statement stmt = stagingConn.createStatement();
             PreparedStatement checkStmt = warehouseConn.prepareStatement(checkSQL);
             PreparedStatement insertStmt = warehouseConn.prepareStatement(insertSQL)) {

            ResultSet rs = stmt.executeQuery(selectSQL);
            while (rs.next()) {
                String processor = rs.getString("processor");

                // Check if the processor already exists
                checkStmt.setString(1, processor);
                ResultSet checkResult = checkStmt.executeQuery();
                checkResult.next();

                if (checkResult.getInt(1) == 0) { // If not exists, insert
                    insertStmt.setString(1, processor);
                    recordsInserted += insertStmt.executeUpdate();
                }
            }
        }
        return recordsInserted;
    }



    private int loadDimCamera(Connection stagingConn, Connection warehouseConn) throws SQLException {
        int recordsInserted = 0;
        String selectSQL = "SELECT DISTINCT rear_camera, front_camera FROM staging_mobile";
        String checkSQL = "SELECT COUNT(*) FROM DimCamera WHERE RearCamera = ? AND FrontCamera = ?";
        String insertSQL = "INSERT INTO DimCamera (RearCamera, FrontCamera) VALUES (?, ?)";

        try (Statement stmt = stagingConn.createStatement();
             PreparedStatement checkStmt = warehouseConn.prepareStatement(checkSQL);
             PreparedStatement insertStmt = warehouseConn.prepareStatement(insertSQL)) {

            ResultSet rs = stmt.executeQuery(selectSQL);
            while (rs.next()) {
                String rearCamera = rs.getString("rear_camera");
                String frontCamera = rs.getString("front_camera");

                // Check if the camera combination already exists
                checkStmt.setString(1, rearCamera);
                checkStmt.setString(2, frontCamera);
                ResultSet checkResult = checkStmt.executeQuery();
                checkResult.next();

                if (checkResult.getInt(1) == 0) { // If not exists, insert
                    insertStmt.setString(1, rearCamera);
                    insertStmt.setString(2, frontCamera);
                    recordsInserted += insertStmt.executeUpdate();
                }
            }
        }
        return recordsInserted;
    }



    private int loadDimOS(Connection stagingConn, Connection warehouseConn) throws SQLException {
        int recordsInserted = 0;
        String selectSQL = "SELECT DISTINCT operating_system FROM staging_mobile";
        String checkSQL = "SELECT COUNT(*) FROM DimOS WHERE OSName = ?";
        String insertSQL = "INSERT INTO DimOS (OSName) VALUES (?)";

        try (Statement stmt = stagingConn.createStatement();
             PreparedStatement checkStmt = warehouseConn.prepareStatement(checkSQL);
             PreparedStatement insertStmt = warehouseConn.prepareStatement(insertSQL)) {

            ResultSet rs = stmt.executeQuery(selectSQL);
            while (rs.next()) {
                String osName = rs.getString("operating_system");

                // Check if the OS already exists
                checkStmt.setString(1, osName);
                ResultSet checkResult = checkStmt.executeQuery();
                checkResult.next();

                if (checkResult.getInt(1) == 0) { // If not exists, insert
                    insertStmt.setString(1, osName);
                    recordsInserted += insertStmt.executeUpdate();
                }
            }
        }
        return recordsInserted;
    }




    private int getDimKey(String tableName, String columnName, String value, Connection conn) throws SQLException {
        String keyColumn = "";

        // Xác định tên khóa chính dựa trên tên bảng
        switch (tableName) {
            case "DimBrand":
                keyColumn = "BrandKey"; // Sử dụng tên cột đúng
                break;
            case "DimModel":
                keyColumn = "ModelKey";
                break;
            case "DimSpecification":
                keyColumn = "SpecKey";
                break;
            case "DimProcessor":
                keyColumn = "ProcessorKey";
                break;
            case "DimCamera":
                keyColumn = "CameraKey";
                break;
            case "DimOS":
                keyColumn = "OSKey";
                break;
            default:
                throw new SQLException("Invalid table name: " + tableName);
        }

        String sql = "SELECT " + keyColumn + " FROM " + tableName + " WHERE " + columnName + " = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, value);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return -1; // Trả về -1 nếu không tìm thấy
    }

}