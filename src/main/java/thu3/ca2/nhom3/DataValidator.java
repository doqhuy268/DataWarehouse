package thu3.ca2.nhom3;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class DataValidator {
    private Connection connection;
    private List<String> nullColumns;
    private List<String> rangeColumns;
    private List<String> duplicateColumns;
    private boolean validateNull;
    private boolean validateRange;
    private boolean validateDuplicate;

    public DataValidator(Connection connection) {
        this.connection = connection;
        loadConfigurations();
    }

    private void loadConfigurations() {
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            Properties props = new Properties();
            props.load(fis);

            // Load column configurations
            nullColumns = Arrays.asList(props.getProperty("validation.null.columns", "").split(","));
            rangeColumns = Arrays.asList(props.getProperty("validation.range.columns", "").split(","));
            duplicateColumns = Arrays.asList(props.getProperty("validation.duplicate.columns", "").split(","));

            // Load rule toggles
            validateNull = Boolean.parseBoolean(props.getProperty("validation.null.enable", "true"));
            validateRange = Boolean.parseBoolean(props.getProperty("validation.range.enable", "true"));
            validateDuplicate = Boolean.parseBoolean(props.getProperty("validation.duplicate.enable", "true"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load validation configurations", e);
        }
    }

    public boolean isValidateNull() {
        return validateNull;
    }

    public boolean isValidateRange() {
        return validateRange;
    }

    public boolean isValidateDuplicate() {
        return validateDuplicate;
    }

    public List<String> getNullColumns() {
        return nullColumns;
    }

    public List<String> getRangeColumns() {
        return rangeColumns;
    }

    public List<String> getDuplicateColumns() {
        return duplicateColumns;
    }

    public int checkNullValues(String tableName) throws SQLException {
        int nullCount = 0;
        for (String column : nullColumns) {
            String sql = "SELECT COUNT(*) AS invalid_count FROM " + tableName + " WHERE " + column + " IS NULL";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    nullCount += rs.getInt("invalid_count");
                }
            }
        }
        return nullCount;
    }

    public int checkRangeValues(String tableName) throws SQLException {
        int rangeCount = 0;
        for (String column : rangeColumns) {
            String sql = "SELECT COUNT(*) AS invalid_count FROM " + tableName + " WHERE " + column + " < 0";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    rangeCount += rs.getInt("invalid_count");
                }
            }
        }
        return rangeCount;
    }

    public int checkDuplicates(String tableName) throws SQLException {
        int duplicateCount = 0;
        // Ensure duplicateColumns is defined and is a List<String>
        if (!duplicateColumns.isEmpty()) {
            // Create a comma-separated string of columns for GROUP BY
            String groupByColumns = String.join(", ", duplicateColumns); // Use double quotes for the delimiter

            // New SQL query for SQL Server
            String sql = String.format("""
            SELECT COUNT(*) as duplicate_count
            FROM (
                SELECT %s, COUNT(*) as record_count
                FROM %s
                GROUP BY %s
                HAVING COUNT(*) > 1
            ) AS DuplicateGroups
            """, groupByColumns, tableName, groupByColumns);

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    duplicateCount = rs.getInt("duplicate_count");
                }
            }
        }
        return duplicateCount;
    }
}
