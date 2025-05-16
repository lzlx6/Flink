package com.lzy.stream.realtime.v1.utils;

import static com.lzy.stream.realtime.v1.utils.SiliconFlowApi.generateBadReview;

/**
 * @Package com.lzy.stream.realtime.v1.utils.CommonGenerateTempLate
 * @Author zheyuan.liu
 * @Date 2025/5/7 10:25
 * @description:
 */

public class CommonGenerateTempLate {

    private static final String COMMENT_TEMPLATE = "生成一个电商%s,商品名称为%s,20字数以内,%s不需要思考过程 ";

    private static final String COMMENT = "差评";

    private static final String API_TOKEN = "sk-mbkquofajmeyzfowolnioseunulvulauadlgcvoetcvayiye";

    public static String GenerateComment(String comment,String productName){
        if (comment.equals(COMMENT)){
            return generateBadReview(
                    String.format(COMMENT_TEMPLATE,COMMENT, productName, "攻击性拉满,使用脏话"),
                    API_TOKEN
            );
        }
        return generateBadReview(
                String.format(COMMENT_TEMPLATE,COMMENT, productName,""),
                API_TOKEN
        );
    }

}