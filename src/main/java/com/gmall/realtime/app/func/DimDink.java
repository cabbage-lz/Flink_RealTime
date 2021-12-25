package com.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.common.GmallConfig;
import com.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * description:TODO 将维度数据写到phoenix
 * Created by thinkpad on 2021-10-09
 */
public class DimDink extends RichSinkFunction<JSONObject> {    //要重写RichSinkFunction
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
//        注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//        获取连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        super.invoke(jsonObj, context);
        PreparedStatement ps = null;
        String tableName = jsonObj.getString("sink_table");
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
        try {
            //创建数据库操作对象
            String upsertSQL = getUpsertSql(tableName, dataJsonObj);
            System.out.println("向Phoenix中插入数据的SQL:" + upsertSQL);
            ps = conn.prepareStatement(upsertSQL);
//            执行SQL
            ps.executeUpdate();
            conn.commit();  //TODO  注意phoenix不支持事物
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("向Phoenix表中插入数据失败");
        } finally {
//            释放资源
            if (ps != null) {
                ps.close();
            }
        }

//        TODO  如果维度数据发生变化，需要将redis中缓存的数据删掉
        if (jsonObj.getString("type").equals("update") || jsonObj.getString("type").equals("delete")) {
            DimUtil.deleteCached(tableName, dataJsonObj.getString("id"));
        }
    }

    // TODO 向phoenix插入数据
    private String getUpsertSql(String tableName, JSONObject dataJsonObj) {
        Set<String> keys = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();

        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName
                + " (" + StringUtils.join(keys, ",") + ") " +
                "values" +
                "('" + StringUtils.join(values, "','") + "')";
        return upsertSql;
    }
}
