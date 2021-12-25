package com.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.utils.DimUtil;
import com.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

/**
 * description:
 * Created by thinkpad on 2021-10-13
 * 发送异步请求请求进行维度关联
 * * 模板方法设计模式：
 * * 在父类中定义完成某一个功能的核心算法骨架，具体的实现延迟到子类中去完成。
 * * 子类在不改变父类核心算法骨架的前提下，每个子类都可以有不同的实现。
 */

//在类的后面声明泛型模板
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    private ExecutorService executorService;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //开启新的线程，发送异步请求，进行关联
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            long start = System.currentTimeMillis();
                            //步骤1：获取维度关联主键
                            String key = getKey(obj);
                            //步骤2：根据主键查询维度信息
                            JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                            //步骤3：进行维度关联
                            if (dimInfoJsonObj != null && dimInfoJsonObj.size() > 0) {
                                join(obj, dimInfoJsonObj);
                            }
                            long end = System.currentTimeMillis();
                            System.out.println("异步维度关联耗时:" + (end - start) + "毫秒");
                            resultFuture.complete(Collections.singleton(obj));
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("异步维度管理发生异常了~~");
                        }
                    }
                }
        );

    }

    public abstract void join(T obj, JSONObject dimInfoJsonObj) throws ParseException, Exception;

    public abstract String getKey(T obj);


}
