package com.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.beans.TableProcess;
import com.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * description: TODO TableProcessFunction 业务数据动态分流
 * Created by thinkpad on 2021-10-08
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private OutputTag<JSONObject> dimTag;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //获取连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    public TableProcessFunction(OutputTag<JSONObject> dimTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.dimTag = dimTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //   处理主流数据   jsonObj从maxwell--》kafka主题中来
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
//        ，
        String table = jsonObj.getString("table");
//        获取主流业务表操作类型
        String type = jsonObj.getString("type");
        //TODO 注意：在使用maxwell处理历史数据的时候，标记的操作类型是bootstrap-insert，需要修复一下
        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObj.put("type", type);
        }
        //拼接key
        String key = table + ":" + type;
//        获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//        从状态中获取当前操作的业务数据对应的配置信息
        TableProcess tableProcess = broadcastState.get(key);   //TODO 在配置流广播状态中如果能获取到业务数据流拼接key的value，接下来进行分流
        if (tableProcess != null) {
            //在配置表中，找到了当前操作的业务数据对应的配置   接下来进行分流操作
            String sinkType = tableProcess.getSinkType();
            //TODO 注意：不管是事实还是维度  在向下游传递之前 都需要获取目的地
            String sinkTable = tableProcess.getSinkTable();
            jsonObj.put("sink_table", sinkTable);
// TODO 往下游传递前，对字段进行过滤
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

            filterColumn(dataJsonObj, tableProcess.getSinkColumns());
            if (sinkType.equals(TableProcess.SINK_TYPE_HBASE)) {
                //维度数据  放到维度侧输出流中
                ctx.output(dimTag, jsonObj);
            } else if (sinkType.equals(TableProcess.SINK_TYPE_KAFKA)) {
                //事实数据  放到主流中
                out.collect(jsonObj);
            }
        } else {
            //在配置表中，没有找到当前操作的业务数据对应的配置
            System.out.println("No this Key In TableProcess:" + key);
        }

    }

    //TODO 过滤字段   dataJsonObj :{"tm_name":"qq","logo_url":"ww","id":15}
//    TODO sinkColumns :id,tm_name
//    TODO  数组--》集合--》JSON对象取出（json底层也为map）
    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] filedArr = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(filedArr);
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
//        for (Map.Entry<String, Object> entry : entrySet) {
//            if (!fieldList.contains(entry.getKey())){
//                entrySet.remove(entry);
//            }
//        }
/*        Iterator<Map.Entry<String, Object>> it = entrySet.iterator();
        for (;it.hasNext();){
            Map.Entry<String, Object> entry = it.next();
            if (!fieldList.contains(entry.getKey())){
                it.remove();
            }
        }*/
//        删除集合元素
        entrySet.removeIf(entry -> !fieldList.contains(entry.getKey()));
    }

    //  处理广播流配置数据   jsonStr代表广播流数据  flinkcdc读过来的为字符串，json对象
//经过自定义反序列器之后，返回格式应该是一个json字符串  {"database":"gmall0428_realtime","table":"table_process","type":"insert","data":{配置表信息}}
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
//将读取到的字符串转换为对象
        JSONObject jsonObj = JSON.parseObject(jsonStr);
//获取配置信息--data属性
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
//        data配置信息放到状态中，状态类型为Tableprocess，将data配置信息转换为Tableprocess类型
        final TableProcess tableprocess = JSON.parseObject(jsonObj.getString("data"), TableProcess.class);


// 获取业务数据库表名
        String sourceTable = tableprocess.getSourceTable();
// 获取操作类型
        String operateType = tableprocess.getOperateType();
        //获取数据类型  维度 hbase|事实kafka
        String sinkType = tableprocess.getSinkType();
        //获取目的地   维度表名  或者 主题名
        String sinkTable = tableprocess.getSinkTable();
        //获取主键
        String sinkPk = tableprocess.getSinkPk();
        //获取保留字段
        String sinkColumns = tableprocess.getSinkColumns();
        //获取建表扩展
        String sinkExtend = tableprocess.getSinkExtend();

        //拼接key
        String key = sourceTable + ":" + operateType;
//
        //判断当前读取的配置信息是不是维度配置
        if (sinkType.equals(TableProcess.SINK_TYPE_HBASE) && "insert".equals(operateType)) {
            //如果是维度配置，那么提前在Phoenix中创建维度表
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
        }

//        TODO  将读取到的配置信息放到广播状态中
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(key, tableprocess);
    }

    //读取广播流配置信息创建维度表   拼接phoenix建表语句
    private void checkTable(String tableName, String fields, String pk, String ext) {
        if (pk == null) {
            pk = "id";
        }
        if (ext == null) {
            ext = "";
        }
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] fieldArr = fields.split(",");
        for (int i = 0; i < fieldArr.length; i++) {
            String fieldName = fieldArr[i];
            if (pk.equals(fieldName)) {
                createSql.append(fieldName + " varchar primary key ");
            } else {
                createSql.append(fieldName + " varchar ");
            }

            if (i < fieldArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")" + ext);
        System.out.println("在Phoenix中建表语句为:" + createSql);

        PreparedStatement ps = null;
        try {

            //创建数据库操作对象
            ps = conn.prepareStatement(createSql.toString());
            //执行SQL语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("在Phoenix中建表失败~~~");
        } finally {
            //释放资源
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
