package com.gmall.realtime.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Author: Felix
 * Date: 2021/10/13
 * Desc: 日期相关的工具类
 * 注意：SimpleDateFormat存在线程安全的问题
 * 解决方案：使用jdk1.8之后的日期包下的类进行处理
 * SimpleDateFormat --- DateTimeFormatter
 * Date ---- LocalDateTime
 * Calendar--- Instant
 */
public class DateTimeUtil {

    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    //将日期数据转换为字符串
    public static String toYmdhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    //将字符串转换为毫秒数
    public static Long toTs(String dateStr) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, dtf);
        Instant instant = localDateTime.toInstant(ZoneOffset.of("+8"));
        return instant.toEpochMilli();
    }

    public static void main(String[] args) {
        System.out.println(ZoneId.systemDefault());
    }
}
