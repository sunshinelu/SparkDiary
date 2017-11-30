package com.evayInfo.Inglory.ScalaDiary.Visualization.jfreechart;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.FileOutputStream;


/**
 * Created by sunlu on 17/11/30.
 * 来源：宫传华
 */
public class BarChart {
    ChartPanel frame1;
    public BarChart(){
        CategoryDataset dataset = getDataSet();
        System.out.println("=========");
        System.out.println(dataset.getColumnCount());
        System.out.println(dataset.getRowCount());


        JFreeChart chart = ChartFactory.createBarChart3D(
                "Title", // 图表标题
                "RetentionType", // 目录轴的显示标签
                "值", // 数值轴的显示标签
                dataset, // 数据集
                PlotOrientation.VERTICAL, // 图表方向：水平、垂直
                true,           // 是否显示图例(对于简单的柱状图必须是false)
                false,          // 是否生成工具
                false           // 是否生成URL链接
        );

        //从这里开始
        CategoryPlot plot=chart.getCategoryPlot();//获取图表区域对象
        CategoryAxis domainAxis=plot.getDomainAxis();         //水平底部列表
        domainAxis.setLabelFont(new Font("黑体",Font.BOLD,14));         //水平底部标题
        domainAxis.setTickLabelFont(new Font("宋体",Font.BOLD,12));  //垂直标题
        ValueAxis rangeAxis=plot.getRangeAxis();//获取柱状
        rangeAxis.setLabelFont(new Font("黑体",Font.BOLD,15));
        chart.getLegend().setItemFont(new Font("黑体", Font.BOLD, 15));
        chart.getTitle().setFont(new Font("宋体",Font.BOLD,20));//设置标题字体
        domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45); // 横轴上的label斜显示


        frame1=new ChartPanel(chart,true);        //这里也可以用chartFrame,可以直接生成一个独立的Frame


        File file =new File("result/BarChart.png");
        FileOutputStream fos_jpg=null;
        try {
            fos_jpg = new FileOutputStream(file,false);
            // 将报表保存为png文件
            ChartUtilities.writeChartAsPNG(fos_jpg, chart, 700, 490);
            fos_jpg.close();
        } catch (Exception e) {
            e.printStackTrace();
            // throw e;
        } finally {
            try {
                fos_jpg.close();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }


    }
    private static CategoryDataset getDataSet() {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        double[] d={0.0021, 0.0043,0.0043, 0.0043,0.0086,0.0107, 0.0107,0.0043, 0.0, 0.0};
        String[] s = {"oneDayRetention", "threeDayRetention", "sevenDayRetention", "fifthDayRetention","oneMonthRetention", "twoMonthRetention", "threeMonthRetention", "fourMonthRetention","fiveMonthRetention","sixMonthRetention"};
        for(int i=0;i<d.length;i++){
            dataset.addValue(d[i], s[i], s[i]);
        }
        return dataset;
    }


    public ChartPanel getChartPanel(){
        return frame1;

    }
    public static void main(String args[]){
        JFrame frame=new JFrame("Java数据统计图");
        frame.setLayout(new GridLayout(1,1,10,10));
        frame.add(new BarChart().getChartPanel());          //添加柱形图的另一种效果
        frame.setBounds(50, 50, 800, 600);
        frame.setVisible(true);
    }
}
