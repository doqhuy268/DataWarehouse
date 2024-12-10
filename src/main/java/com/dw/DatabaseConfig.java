package com.dw;

import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

public class DatabaseConfig {
    private static Properties properties = new Properties();

    public static void loadFromFile(String filePath) throws Exception {
        try (var fis = new FileInputStream(filePath)) {
            properties.load(fis);
        }
    }

    public static String getWarehouseDbUrl() {
        return properties.getProperty("warehouse.db.url");
    }

    public static String getStagingDbUrl() {
        return properties.getProperty("staging.db.url");
    }

    public static String getControlDbUrl() {
        return properties.getProperty("control.db.url");
    }
}

