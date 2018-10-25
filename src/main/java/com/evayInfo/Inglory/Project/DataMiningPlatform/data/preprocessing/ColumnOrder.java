package com.evayInfo.Inglory.Project.DataMiningPlatform.data.preprocessing;

//mysql列排序
public class ColumnOrder {

    //字段名
    private String columnName;

    //排序方式
    private String orderType;

    public ColumnOrder() {
    }

    public ColumnOrder(String columnName, String orderType) {
        this.columnName = columnName;
        this.orderType = orderType;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getOrderType() {
        return orderType;
    }

    public void setOrderType(String orderType) {
        this.orderType = orderType;
    }
}
