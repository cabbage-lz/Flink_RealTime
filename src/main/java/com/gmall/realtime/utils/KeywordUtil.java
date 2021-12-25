package com.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Felix
 * Date: 2021/10/16
 * Desc: 分词工具类
 */
public class KeywordUtil {
    public static List<String> analyze(String text) {
        List<String> resList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        Lexeme lexeme = null;
        try {
            while ((lexeme = ikSegmenter.next()) != null) {
                String keyword = lexeme.getLexemeText();
                resList.add(keyword);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resList;
    }

    public static void main(String[] args) {
        List<String> resList = analyze("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待");
        System.out.println(resList);
    }
}
