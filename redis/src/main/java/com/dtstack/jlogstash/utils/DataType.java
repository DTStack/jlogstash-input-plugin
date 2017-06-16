package com.dtstack.jlogstash.utils;

public enum DataType {
    LIST("list"),STRING("string"),SET("set"),HASH("hash");

    private String dataType;

    DataType(String dataType){
        this.dataType = dataType;
    }

    public String getDataType() {
        return dataType;
    }
}
