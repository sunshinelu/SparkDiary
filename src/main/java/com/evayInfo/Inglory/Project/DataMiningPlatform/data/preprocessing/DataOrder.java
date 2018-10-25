package com.evayInfo.Inglory.Project.DataMiningPlatform.data.preprocessing;

//import com.evay.inglory.data.mining.data.acquisition.entity.ColumnOrder;
//import com.evay.inglory.data.mining.data.acquisition.jdbc.MysqlTool;

import com.evayInfo.Inglory.Project.DataMiningPlatform.data.acquisition.MysqlTool;

import java.util.List;

/**
 * 数据排序
 * @author gyz
 * */
public class DataOrder {

    MysqlTool mysqlTool = MysqlTool.getInstance();

    /**
     * @param originalTableName 源表名
     * @param columnList 字段名以及排序方式 "升序排序"  "降序排序"
     * @param targetTableName 靶表名
     * */
    public void processingData(String originalTableName, List<ColumnOrder> columnList, String targetTableName) {

        boolean flag = mysqlTool.ifExist(targetTableName);
        if (flag) {
            System.out.println("table " + targetTableName + " is exist, please rename");
        } else {
            //create table test4 select * from data_statistics order by id asc, var1 desc
            //建表
            String creatTableSQL = "create table " + targetTableName + " select * from " + originalTableName + " order by ";
            for(int i = 0; i < columnList.size(); i++) {
                ColumnOrder columnOrder = columnList.get(i);
                String columnName = columnOrder.getColumnName();
                System.out.println("columnName: " + columnName);
                System.out.println("originalType" + columnOrder.getOrderType());
                String orderType = transformToMySQL(columnOrder.getOrderType());
                System.out.println("orderType: " + orderType);
                if(i == columnList.size() - 1) {
                    creatTableSQL += columnName + " " + orderType;
                } else {
                    creatTableSQL += columnName + " " + orderType + ", ";
                }
            }
            System.out.println(creatTableSQL);
            mysqlTool.createTable(creatTableSQL);


        }
    }

    private String transformToMySQL(String orderType) {

        String result = "";
        if(orderType.equals("降序排序")) {
            result = "desc";
        } else {
            result = "asc";
        }
        return result;
    }


}
