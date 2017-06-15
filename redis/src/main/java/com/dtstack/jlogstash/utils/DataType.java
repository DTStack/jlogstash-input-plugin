package com.dtstack.jlogstash.utils;

public enum DataType {
    LIST("list"),CHANNEL("channel"),PATTERN_CHANNEL("pattern_channel");

    private String dataType;

    DataType(String dataType){
        this.dataType = dataType;
    }

    public String getDataType() {
        return dataType;
    }
}
