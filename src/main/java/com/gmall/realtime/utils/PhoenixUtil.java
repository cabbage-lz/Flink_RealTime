package com.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * description:TODO 操作Phoenix的实体类：PhoenixUtil
 * TODO  可以从phoenix任何一张表中查数据
 * Created by thinkpad on 2021-10-12
 */
public class PhoenixUtil {

    private static Connection conn;

    private static void initConnection() throws Exception {

        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //创建连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //设置表空间
        conn.setSchema(GmallConfig.HBASE_SCHEMA);
    }

    /*
     * sql:查询的sql
     * clz：查询的结果集要封装的对象*/
    public static <T> List<T> queryList(String sql, Class<T> clz) {
        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            if (conn == null) {

                initConnection();
            }
            // 获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();
            //TODO 处理结果集     重要
            //TODO 获取结果集元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                //通过反射将对应的对象创建出来
                T obj = clz.newInstance();
//            遍历所有列
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
//                    获取列名
                    String columnName = metaData.getColumnName(i);
//                   获取列对应的值
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj, columnName, columnValue);  //三个参数：给对象赋值，属性名称，属性值
                }
                resList.add(obj);
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }

    public static void main(String[] args) throws Exception {
        List<JSONObject> dimList = queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class);
        System.out.println(dimList);
    }
}
