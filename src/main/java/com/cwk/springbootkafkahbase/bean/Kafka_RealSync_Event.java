package com.cwk.springbootkafkahbase.bean;

import com.cwk.springbootkafkahbase.utils.TimeUtils;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Kafka_RealSync_Event implements Serializable {

    private String batchID;

    private Integer batchRowCount;

    private Integer columnNum;

    private String operation_type;

    private String owner;

    private String rowNum;

    public Kafka_RealSync_Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Kafka_RealSync_Metadata metadata) {
        this.metadata = metadata;
    }

    private String tableName;

    private List<Kafka_RealSync_Column> columnListInfo;

    private Kafka_RealSync_Metadata metadata;


    private String scn_time;
    private String databasename;

    public String getScn_time() {
        return scn_time;
    }

    public void setScn_time(String scn_time) {
        this.scn_time = scn_time;
    }

    public String getDatabasename() {
        return databasename;
    }

    public void setDatabasename(String databasename) {
        this.databasename = databasename;
    }

    public Map<String, Kafka_RealSync_Column> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(Map<String, Kafka_RealSync_Column> columnMap) {
        this.columnMap = columnMap;
    }

    public Long getScnTimeMs() {
        return scnTimeMs;
    }

    public void setScnTimeMs(Long scnTimeMs) {
        this.scnTimeMs = scnTimeMs;
    }

    private Map<String, Kafka_RealSync_Column> columnMap;
    private Long scnTimeMs;


    public String getBatchID() {
        return batchID;
    }

    public void setBatchID(String batchID) {
        this.batchID = batchID;
    }

    public Integer getBatchRowCount() {
        return batchRowCount;
    }

    public void setBatchRowCount(Integer batchRowCount) {
        this.batchRowCount = batchRowCount;
    }

    public Integer getColumnNum() {
        return columnNum;
    }

    public void setColumnNum(Integer columnNum) {
        this.columnNum = columnNum;
    }

    public String getOperation_type() {
        return operation_type;
    }

    public void setOperation_type(String operation_type) {
        this.operation_type = operation_type;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getRowNum() {
        return rowNum;
    }

    public void setRowNum(String rowNum) {
        this.rowNum = rowNum;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Kafka_RealSync_Column> getColumnListInfo() {
        return columnListInfo;
    }

    public void setColumnListInfo(List<Kafka_RealSync_Column> columnListInfo) {
        this.columnListInfo = columnListInfo;
    }

    public Kafka_RealSync_Event updateMetadata() {

        Map<String, Kafka_RealSync_Column> columnMap = this.getColumnListInfo().stream()
//                .collect(Collectors.toMap(Kafka_RealSync_Column::getColumnName, col->col));
                .collect(Collectors.toMap(Kafka_RealSync_Column::getColumnName, (p) -> p, (k, v) -> v));
        Kafka_RealSync_Column scnTimeColumn = columnMap.getOrDefault("scn_time", null);
        Kafka_RealSync_Column databasenameColumn = columnMap.getOrDefault("databasename", null);

        // 转换 scn_time 字符串为毫秒
        if (scnTimeColumn != null) {
            this.scn_time = scnTimeColumn.getColumnValue();
            try {
                this.scnTimeMs = TimeUtils.sdf.parse(scnTimeColumn.getColumnValue()).getTime();
            } catch (ParseException e) {
                // this.scnTimeMs = -1L;
                this.scnTimeMs = null;
            }
        } else {
            // this.scnTimeMs = -1L;
            this.scnTimeMs = null;
        }

        if (databasenameColumn != null) {
            this.databasename = databasenameColumn.getColumnValue();
        }

        this.columnMap = columnMap.entrySet().stream()
                .filter(entry -> entry.getValue().getDataColNum() != -1) // 过滤非字段的属性
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return this;
    }

    public Kafka_RealSync_Event updateMetadataMap() {

        Kafka_RealSync_Metadata metadata = new Kafka_RealSync_Metadata();
        metadata.setDatabasename(this.databasename);
        metadata.setOwner(this.owner);
        metadata.setTableName(this.tableName);
        metadata.setOperation_type(this.operation_type);
        metadata.setScn_time(this.scn_time);
        metadata.setScnTimeMs(this.scnTimeMs);

        this.metadata = metadata;

        return this;
    }

    public Map<String, String> getDataColumnMap() {
        Map<String, String> dataColumnMap = this.getColumnMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, (p) -> p.getValue().getColumnValue()));
        return dataColumnMap;
    }


    @Override
    public String toString() {
        return "Kafka_RealSync_Event{" +
                "batchID='" + batchID + '\'' +
                ", batchRowCount=" + batchRowCount +
                ", columnNum=" + columnNum +
                ", operation_type='" + operation_type + '\'' +
                ", owner='" + owner + '\'' +
                ", rowNum='" + rowNum + '\'' +
                ", tableName='" + tableName + '\'' +
                ", columnListInfo=" + columnListInfo +
                ", metadata=" + metadata +
                ", scn_time='" + scn_time + '\'' +
                ", databasename='" + databasename + '\'' +
                ", columnMap=" + columnMap +
                ", scnTimeMs=" + scnTimeMs +
                '}';
    }
}
