package com.lzy.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.lzy.retail.com.lzy.stream.realtime.v1.realtime.bean.TableProcessDim
 * @Author zheyuan.liu
 * @Date 2025/4/8 8:47
 * @description: TableProcessDim
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableProcessDim {
    String sourceTable;
    String sinkTable;
    String sinkColumns;
    String sinkFamily;
    String sinkRowKey;
    String op;
}
