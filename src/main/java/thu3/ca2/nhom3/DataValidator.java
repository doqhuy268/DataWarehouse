package thu3.ca2.nhom3;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DataValidator {

    public int checkNullValues(String tableName, Connection conn) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName + " WHERE name IS NULL OR brand IS NULL OR price IS NULL";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 0;
    }

    public int checkRangeValues(String tableName, Connection conn) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName + " WHERE battery_capacity <= 0 OR price < 0 OR screen_size <= 0";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 0;
    }

    public int checkDuplicates(String tableName, Connection conn) throws SQLException {
        int failedRecords = 0;
        String duplicateCheckSQL = "SELECT COUNT(*) AS duplicateCount " +
                "FROM " + tableName +
                " GROUP BY brand, model " +
                "HAVING COUNT(*) > 1";

        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(duplicateCheckSQL)) {
            while (rs.next()) {
                failedRecords += rs.getInt("duplicateCount");
            }
        }
        return failedRecords;
    }

}

