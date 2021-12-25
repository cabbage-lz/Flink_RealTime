package com.gmall.realtime.beans;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * description:TODO 定义注解
 * Created by thinkpad on 2021-10-15
 */
@Target(ElementType.FIELD)    //修饰注解的注解为源注解，指定被修饰的注解加在哪里
@Retention(RetentionPolicy.RUNTIME)    //程序运行时起作用
public @interface TransientSink {

}
