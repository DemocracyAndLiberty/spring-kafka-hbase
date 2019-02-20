package com.cwk.springbootkafkahbase.bean;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

public class Kafka_RealSync_Metadata {

    private String databasename;
    private String owner;
    private String tableName;
    private Long scnTimeMs;
    private String scn_time;
    private String operation_type;

    public Kafka_RealSync_Metadata() {
    }

    public String getDatabasename() {
        return databasename;
    }

    public void setDatabasename(String databasename) {
        this.databasename = databasename;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getScnTimeMs() {
        return scnTimeMs;
    }

    public void setScnTimeMs(Long scnTimeMs) {
        this.scnTimeMs = scnTimeMs;
    }

    public String getScn_time() {
        return scn_time;
    }

    public void setScn_time(String scn_time) {
        this.scn_time = scn_time;
    }

    public String getOperation_type() {
        return operation_type;
    }

    public void setOperation_type(String operation_type) {
        this.operation_type = operation_type;
    }

    public Map<String, String> getMetadataMap() {
        // 设置 metadataMap
        Map<String, String> metdataMap = new HashMap<String, String>();
        metdataMap.put("databasename", this.databasename);
        metdataMap.put("owner", this.owner);
        metdataMap.put("tableName", this.tableName);
        metdataMap.put("scnTimeMs", this.scnTimeMs.toString());
        metdataMap.put("scn_time", this.scn_time);
        metdataMap.put("operation_type", this.operation_type);
        return metdataMap;
    }

    public String getMetadataJson() {
        Map<String, String> metadataMap = this.getMetadataMap();
        String jsonString = JSON.toJSONString(metadataMap);
        return jsonString;
    }

}
