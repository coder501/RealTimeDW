package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {
    public static Connection conn = null;

    public static void InitConnection() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            //设置此链接要查询的schema信息
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static <T>List<T> queryList(String sql,Class<T> cls){

        //创建一个list集合用于存储查询结果集
        List<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try {
            if(conn == null){
                InitConnection();
            }

            ps = conn.prepareStatement(sql);

            resultSet = ps.executeQuery();
            //getMateData()方法能够返回查询结果的元数据信息即列名
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()){
                //创建一个bean对象,用于封装查询到的一行结果
                T rowData = cls.newInstance();
                for(int i = 1;i <= metaData.getColumnCount();i++){
                    //通过apache.commons.beanutils下的BeanUtils工具类将查询到的每一列值赋值给bean对象的相对应的属性
                    BeanUtils.setProperty(rowData,metaData.getColumnName(i),resultSet.getObject(i));
                }
                //将封装的bean对象添加到list集合中
                resultList.add(rowData);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if (ps != null) {
                    ps.close();
                }
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        return resultList;
    }

}
