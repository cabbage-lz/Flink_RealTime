package com.gmall.realtime.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * description:创建线程池工具类
 * TODO  双重校验锁解决单例设计模式懒汉式的线程安全问题
 * Created by thinkpad on 2021-10-12
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor pool;  //线程池对象

    //getInstance 获取线程池
    public static ExecutorService getInstance() {
        if (pool == null) {
            synchronized (ThreadPoolUtil.class) {
                if (pool == null) {
                    System.out.println("~~~开辟线程池~~~");
                    pool = new ThreadPoolExecutor(
                            4,
                            20,
                            300,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return pool;
    }

}
