package com.evayInfo.Inglory.Project.DataMiningPlatform.data.preprocessing;


import java.util.ArrayList;
import java.util.List;

/**
 * 数据排序
 * @author gyz
 * */
public class DataOrderTest {

    public static void main(String[] args) {

        //String originalTableName, List<ColumnOrder> columnList, String targetTableName
        String originalTableName = "data_statistics";
        String targetTableName = "preprocessing_column_order_test1";

        List<ColumnOrder> columnList = new ArrayList<>();
        ColumnOrder columnOrder1 = new ColumnOrder();
        columnOrder1.setColumnName("id");
        columnOrder1.setOrderType("升序排序");

        ColumnOrder columnOrder2 = new ColumnOrder();
        columnOrder2.setColumnName("var1");
        columnOrder2.setOrderType("降序排序");

        columnList.add(columnOrder1);
        columnList.add(columnOrder2);


        DataOrder dataOrder = new DataOrder();
        dataOrder.processingData(originalTableName, columnList, targetTableName);


    }
}
