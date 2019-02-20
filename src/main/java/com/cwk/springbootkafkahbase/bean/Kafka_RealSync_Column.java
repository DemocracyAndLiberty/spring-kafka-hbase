package com.cwk.springbootkafkahbase.bean;

import java.util.Arrays;

class Kafka_RealSync_Column {
    private Integer btype;
    private Integer cflag;
    private Integer colNum;
    private String columnName;
    private String columnValue;
    private Integer dataColNum;
    private Integer len;
    private int[] oraType;
    private Integer otype;
    private String rowID;
    private Integer rowNum;

    public Integer getBtype() {
        return btype;
    }

    public void setBtype(Integer btype) {
        this.btype = btype;
    }

    public Integer getCflag() {
        return cflag;
    }

    public void setCflag(Integer cflag) {
        this.cflag = cflag;
    }

    public Integer getColNum() {
        return colNum;
    }

    public void setColNum(Integer colNum) {
        this.colNum = colNum;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(String columnValue) {
        this.columnValue = columnValue;
    }

    public Integer getDataColNum() {
        return dataColNum;
    }

    public void setDataColNum(Integer dataColNum) {
        this.dataColNum = dataColNum;
    }

    public Integer getLen() {
        return len;
    }

    public void setLen(Integer len) {
        this.len = len;
    }

    public int[] getOraType() {
        return oraType;
    }

    public void setOraType(int[] oraType) {
        this.oraType = oraType;
    }

    public Integer getOtype() {
        return otype;
    }

    public void setOtype(Integer otype) {
        this.otype = otype;
    }

    public String getRowID() {
        return rowID;
    }

    public void setRowID(String rowID) {
        this.rowID = rowID;
    }

    public Integer getRowNum() {
        return rowNum;
    }

    public void setRowNum(Integer rowNum) {
        this.rowNum = rowNum;
    }

    @Override
    public String toString() {
        return "Kafka_RealSync_Column{" +
                "btype=" + btype +
                ", cflag=" + cflag +
                ", colNum=" + colNum +
                ", columnName='" + columnName + '\'' +
                ", columnValue='" + columnValue + '\'' +
                ", dataColNum=" + dataColNum +
                ", len=" + len +
                ", oraType=" + Arrays.toString(oraType) +
                ", otype=" + otype +
                ", rowID='" + rowID + '\'' +
                ", rowNum=" + rowNum +
                '}';
    }
}
