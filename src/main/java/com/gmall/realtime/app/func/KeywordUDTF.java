package com.gmall.realtime.app.func;

import com.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * Author: Felix
 * Date: 2021/10/16
 * Desc: 自定义UDTF函数
 */
//指定输出的Row中有哪些字段，以及字段类型
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {

    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
