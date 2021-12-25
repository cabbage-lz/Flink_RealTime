package com.gmall.realtime.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author: Felix
 * Date: 2021/10/16
 * Desc: 关键词统计实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String keyword;
    private Long ct;
    private String source;
    private String stt;
    private String edt;
    private Long ts;
}
