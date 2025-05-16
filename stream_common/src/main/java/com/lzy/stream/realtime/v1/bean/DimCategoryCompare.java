package com.lzy.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.lzy.stream.realtime.v1.bean.DimCategoryCompare
 * @Author zheyuan.liu
 * @Date 2025/5/14 14:38
 * @description:
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare {
    private Integer id;
    private String category_name;
    private String search_category;
}
