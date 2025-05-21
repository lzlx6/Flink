package com.lzy.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lzy.stream.realtime.v1.bean.DimBaseCategory;
import com.lzy.stream.realtime.v1.bean.DimSkuInfoMsg;
import com.lzy.stream.realtime.v1.utils.FlinkSourceUtil;
import com.lzy.stream.realtime.v1.utils.JdbcUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.time.Duration;
import java.util.*;

/**
 * @Package com.lzy.app.dwd.DwdTimeSoure
 * @Author zheyuan.liu
 * @Date 2025/5/15 9:14
 * @description:
 */

public class DwdTimeSoure {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取 Kafka 数据：使用 FlinkSourceUtil.getKafkaSource 读取 Kafka 中的订单消息；
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_order_info_join", "DwdTimeSoure");
        DataStreamSource<String> fromSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

//        fromSource.print();

        //JSON 解析与时间戳提取：将字符串转为 JSONObject，并基于 ts_ms 字段设置事件时间；
        SingleOutputStreamOperator<JSONObject> streamOperator = fromSource.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));

//        operator.print();

        //RichMapFunction 处理逻辑：
        //open 方法：连接 MySQL，加载商品（Sku）和类目维度表到内存；
        //map 方法：
        //根据 search_item 补充商品信息（c3id, tname）；
        //根据 c3id 补充类目一级名称（b1_name）；
        //对不同维度（时间、金额、品牌、类目）进行评分；
        //close 方法：关闭数据库连接；
        SingleOutputStreamOperator<JSONObject> operator1 = streamOperator.map(new RichMapFunction<JSONObject, JSONObject>() {
            private Connection connection;
            List<DimSkuInfoMsg> dimSkuInfoMsgs;
            private List<DimBaseCategory> dimBaseCategories;
            private List<DimBaseCategory> dim_base_categories;
            private double timeRate = 0.1;
            private double amountRate = 0.15;
            private double brandRate = 0.2;
            private double categoryRate = 0.3;
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                connection = JdbcUtil.getMySQLConnection();

                String sql1 = "  SELECT                                                                  \n" +
                    "   b3.id, b3.name3 b3name, b2.name2 b2name, b1.name1 b1name                         \n" +
                    "   FROM realtime_dmp.base_category3 as b3                                           \n" +
                    "   JOIN realtime_dmp.base_category2 as b2                                           \n" +
                    "   ON b3.category2_id = b2.id                                                       \n" +
                    "   JOIN realtime_dmp.base_category1 as b1                                           \n" +
                    "   ON b2.category1_id = b1.id                                                         ";
                dim_base_categories = JdbcUtil.queryList(connection, sql1, DimBaseCategory.class, false);

                String querySkuSql = "  select sku_info.id AS id,                                        \n" +
                    "                   spu_info.id AS spuid,                                            \n" +
                    "                   spu_info.category3_id AS c3id,                                   \n" +
                    "                   base_trademark.tm_name AS name                                   \n" +
                    "                   from realtime_dmp.sku_info                                       \n" +
                    "                   join realtime_dmp.spu_info                                       \n" +
                    "                   on sku_info.spu_id = spu_info.id                                 \n" +
                    "                   join realtime_dmp.base_trademark                                 \n" +
                    "                   on realtime_dmp.spu_info.tm_id = realtime_dmp.base_trademark.id    ";

                dimSkuInfoMsgs = JdbcUtil.queryList(connection, querySkuSql, DimSkuInfoMsg.class);

                for (DimSkuInfoMsg dimSkuInfoMsg : dimSkuInfoMsgs) {
                    System.err.println(dimSkuInfoMsg);
                }
            }

            @Override
            public JSONObject map(JSONObject jsonObject) {
                String skuId = jsonObject.getString("search_item");
                if (skuId != null && !skuId.isEmpty()){
                    for (DimSkuInfoMsg dimSkuInfoMsg : dimSkuInfoMsgs) {
                        if (dimSkuInfoMsg.getId().equals(skuId)){
                            jsonObject.put("c3id",dimSkuInfoMsg.getCategory3_id());
                            jsonObject.put("tname",dimSkuInfoMsg.getTm_name());
                            break;
                        }
                    }
                }

                String c3id = jsonObject.getString("c3id");
                if (c3id != null && !c3id.isEmpty()){
                    for (DimBaseCategory dimBaseCategory : dim_base_categories) {
                        if (c3id.equals(dimBaseCategory.getId())){
                            jsonObject.put("b1_name",dimBaseCategory.getName1());
                            break;
                        }
                    }
                }

                // 时间打分
                String payTimeSlot = jsonObject.getString("create_time");
                if (payTimeSlot != null && !payTimeSlot.isEmpty()){
                    switch (payTimeSlot) {
                        case "凌晨":
                            jsonObject.put("pay_time_18-24", round(0.2 * timeRate));
                            jsonObject.put("pay_time_25-29", round(0.1 * timeRate));
                            jsonObject.put("pay_time_30-34", round(0.1 * timeRate));
                            jsonObject.put("pay_time_35-39", round(0.1 * timeRate));
                            jsonObject.put("pay_time_40-49", round(0.1 * timeRate));
                            jsonObject.put("pay_time_50", round(0.1 * timeRate));
                            break;
                        case "早晨":
                            jsonObject.put("pay_time_18-24", round(0.1 * timeRate));
                            jsonObject.put("pay_time_25-29", round(0.1 * timeRate));
                            jsonObject.put("pay_time_30-34", round(0.1 * timeRate));
                            jsonObject.put("pay_time_35-39", round(0.1 * timeRate));
                            jsonObject.put("pay_time_40-49", round(0.2 * timeRate));
                            jsonObject.put("pay_time_50", round(0.3 * timeRate));
                            break;
                        case "上午":
                            jsonObject.put("pay_time_18-24", round(0.2 * timeRate));
                            jsonObject.put("pay_time_25-29", round(0.2 * timeRate));
                            jsonObject.put("pay_time_30-34", round(0.2 * timeRate));
                            jsonObject.put("pay_time_35-39", round(0.2 * timeRate));
                            jsonObject.put("pay_time_40-49", round(0.3 * timeRate));
                            jsonObject.put("pay_time_50", round(0.4 * timeRate));
                            break;
                        case "中午":
                            jsonObject.put("pay_time_18-24", round(0.4 * timeRate));
                            jsonObject.put("pay_time_25-29", round(0.4 * timeRate));
                            jsonObject.put("pay_time_30-34", round(0.4 * timeRate));
                            jsonObject.put("pay_time_35-39", round(0.4 * timeRate));
                            jsonObject.put("pay_time_40-49", round(0.4 * timeRate));
                            jsonObject.put("pay_time_50", round(0.3 * timeRate));
                            break;
                        case "下午":
                            jsonObject.put("pay_time_18-24", round(0.4 * timeRate));
                            jsonObject.put("pay_time_25-29", round(0.5 * timeRate));
                            jsonObject.put("pay_time_30-34", round(0.5 * timeRate));
                            jsonObject.put("pay_time_35-39", round(0.5 * timeRate));
                            jsonObject.put("pay_time_40-49", round(0.5 * timeRate));
                            jsonObject.put("pay_time_50", round(0.4 * timeRate));
                            break;
                        case "晚上":
                            jsonObject.put("pay_time_18-24", round(0.8 * timeRate));
                            jsonObject.put("pay_time_25-29", round(0.7 * timeRate));
                            jsonObject.put("pay_time_30-34", round(0.6 * timeRate));
                            jsonObject.put("pay_time_35-39", round(0.5 * timeRate));
                            jsonObject.put("pay_time_40-49", round(0.4 * timeRate));
                            jsonObject.put("pay_time_50", round(0.3 * timeRate));
                            break;
                        case "夜间":
                            jsonObject.put("pay_time_18-24", round(0.9 * timeRate));
                            jsonObject.put("pay_time_25-29", round(0.7 * timeRate));
                            jsonObject.put("pay_time_30-34", round(0.5 * timeRate));
                            jsonObject.put("pay_time_35-39", round(0.3 * timeRate));
                            jsonObject.put("pay_time_40-49", round(0.2 * timeRate));
                            jsonObject.put("pay_time_50", round(0.1 * timeRate));
                            break;
                    }
                }

                // 价格打分
                double totalAmount = jsonObject.getDoubleValue("total_amount");
                if (totalAmount < 1000){
                    jsonObject.put("amount_18-24", round(0.8 * amountRate));
                    jsonObject.put("amount_25-29", round(0.6 * amountRate));
                    jsonObject.put("amount_30-34", round(0.4 * amountRate));
                    jsonObject.put("amount_35-39", round(0.3 * amountRate));
                    jsonObject.put("amount_40-49", round(0.2 * amountRate));
                    jsonObject.put("amount_50",    round(0.1 * amountRate));
                }else if (totalAmount > 1000 && totalAmount < 4000){
                    jsonObject.put("amount_18-24", round(0.2 * amountRate));
                    jsonObject.put("amount_25-29", round(0.4 * amountRate));
                    jsonObject.put("amount_30-34", round(0.6 * amountRate));
                    jsonObject.put("amount_35-39", round(0.7 * amountRate));
                    jsonObject.put("amount_40-49", round(0.8 * amountRate));
                    jsonObject.put("amount_50",    round(0.7 * amountRate));
                }else {
                    jsonObject.put("amount_18-24", round(0.1 * amountRate));
                    jsonObject.put("amount_25-29", round(0.2 * amountRate));
                    jsonObject.put("amount_30-34", round(0.3 * amountRate));
                    jsonObject.put("amount_35-39", round(0.4 * amountRate));
                    jsonObject.put("amount_40-49", round(0.5 * amountRate));
                    jsonObject.put("amount_50",    round(0.6 * amountRate));
                }


                // 品牌
                String tname = jsonObject.getString("tname");
                if (tname != null && !tname.isEmpty()){
                    switch (tname) {
                        case "TCL":
                            jsonObject.put("tname_18-24", round(0.2 * brandRate));
                            jsonObject.put("tname_25-29", round(0.3 * brandRate));
                            jsonObject.put("tname_30-34", round(0.4 * brandRate));
                            jsonObject.put("tname_35-39", round(0.5 * brandRate));
                            jsonObject.put("tname_40-49", round(0.6 * brandRate));
                            jsonObject.put("tname_50", round(0.7 * brandRate));
                            break;
                        case "苹果":
                        case "联想":
                        case "小米":
                            jsonObject.put("tname_18-24", round(0.9 * brandRate));
                            jsonObject.put("tname_25-29", round(0.8 * brandRate));
                            jsonObject.put("tname_30-34", round(0.7 * brandRate));
                            jsonObject.put("tname_35-39", round(0.7 * brandRate));
                            jsonObject.put("tname_40-49", round(0.7 * brandRate));
                            jsonObject.put("tname_50", round(0.5 * brandRate));
                            break;
                        case "欧莱雅":
                            jsonObject.put("tname_18-24", round(0.5 * brandRate));
                            jsonObject.put("tname_25-29", round(0.6 * brandRate));
                            jsonObject.put("tname_30-34", round(0.8 * brandRate));
                            jsonObject.put("tname_35-39", round(0.8 * brandRate));
                            jsonObject.put("tname_40-49", round(0.9 * brandRate));
                            jsonObject.put("tname_50", round(0.2 * brandRate));
                            break;
                        case "香奈儿":
                            jsonObject.put("tname_18-24", round(0.3 * brandRate));
                            jsonObject.put("tname_25-29", round(0.4 * brandRate));
                            jsonObject.put("tname_30-34", round(0.6 * brandRate));
                            jsonObject.put("tname_35-39", round(0.8 * brandRate));
                            jsonObject.put("tname_40-49", round(0.9 * brandRate));
                            jsonObject.put("tname_50", round(0.2 * brandRate));
                            break;
                        default:
                            jsonObject.put("tname_18-24", round(0.1 * brandRate));
                            jsonObject.put("tname_25-29", round(0.2 * brandRate));
                            jsonObject.put("tname_30-34", round(0.3 * brandRate));
                            jsonObject.put("tname_35-39", round(0.4 * brandRate));
                            jsonObject.put("tname_40-49", round(0.5 * brandRate));
                            jsonObject.put("tname_50", round(0.6 * brandRate));
                            break;
                    }
                }

                // 类目

                String b1Name = jsonObject.getString("b1_name");
                if (b1Name != null && !b1Name.isEmpty()){
                    switch (b1Name){
                        case "数码":
                        case "手机":
                        case "电脑办公":
                        case "个护化妆":
                        case "服饰内衣":
                            jsonObject.put("b1name_18-24", round(0.9 * categoryRate));
                            jsonObject.put("b1name_25-29", round(0.8 * categoryRate));
                            jsonObject.put("b1name_30-34", round(0.6 * categoryRate));
                            jsonObject.put("b1name_35-39", round(0.4 * categoryRate));
                            jsonObject.put("b1name_40-49", round(0.2 * categoryRate));
                            jsonObject.put("b1name_50",    round(0.1 * categoryRate));
                            break;
                        case "家居家装":
                        case "图书、音像、电子书刊":
                        case "厨具":
                        case "鞋靴":
                        case "母婴":
                        case "汽车用品":
                        case "珠宝":
                        case "家用电器":
                            jsonObject.put("b1name_18-24", round(0.2 * categoryRate));
                            jsonObject.put("b1name_25-29", round(0.4 * categoryRate));
                            jsonObject.put("b1name_30-34", round(0.6 * categoryRate));
                            jsonObject.put("b1name_35-39", round(0.8 * categoryRate));
                            jsonObject.put("b1name_40-49", round(0.9 * categoryRate));
                            jsonObject.put("b1name_50",    round(0.7 * categoryRate));
                            break;
                        default:
                            jsonObject.put("b1name_18-24", round(0.1 * categoryRate));
                            jsonObject.put("b1name_25-29", round(0.2 * categoryRate));
                            jsonObject.put("b1name_30-34", round(0.4 * categoryRate));
                            jsonObject.put("b1name_35-39", round(0.5 * categoryRate));
                            jsonObject.put("b1name_40-49", round(0.8 * categoryRate));
                            jsonObject.put("b1name_50",    round(0.9 * categoryRate));
                    }
                }


                return jsonObject;
            }

            @Override
            public void close() throws Exception {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }
        });

        operator1.print();

//        operator1.map(data -> data.toString()).sinkTo(FlinkSinkUtil.getKafkaSink("dwd_time_soure"));

        env.execute("DwdTimeSoure");
    }
    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }
}
