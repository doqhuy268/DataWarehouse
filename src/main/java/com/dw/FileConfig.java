package com.dw;

public class FileConfig {
    private String configKey;
    private String configValue;

    public FileConfig(String configKey, String configValue) {
        this.configKey = configKey;
        this.configValue = configValue;
    }

    public String getConfigKey() {
        return configKey;
    }

    public String getConfigValue() {
        return configValue;
    }
}

