package com.lzy.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.lzy.stream.realtime.v1.bean.DimSkuInfoMsg
 * @Author zheyuan.liu
 * @Date 2025/5/15 19:46
 * @description:
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimSkuInfoMsg {
    private String id;
    private String spuid;
    private String category3_id;
    private String tm_name;
}
