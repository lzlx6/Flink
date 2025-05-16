package com.lzy.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.lzy.stream.realtime.v1.bean.DimBaseCategory
 * @Author zheyuan.liu
 * @Date 2025/5/14 14:37
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory {
    private String id;
    private String name3;
    private String name2;
    private String name1;
}
