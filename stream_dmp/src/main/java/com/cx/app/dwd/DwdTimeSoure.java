package com.lzy.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lzy.stream.realtime.v1.bean.DimBaseCategory;
import com.lzy.stream.realtime.v1.bean.DimCategoryCompare;
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

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_order_info_join", "DwdTimeSoure");
        DataStreamSource<String> fromSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");



//        fromSource.print();

        SingleOutputStreamOperator<JSONObject> streamOperator = fromSource.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));

        SingleOutputStreamOperator<JSONObject> map = streamOperator.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject value) throws Exception {
                JSONObject object = new JSONObject();
                String create_time = value.getString("create_time");
                String time = create_time.split(" ")[1];
                String hourStr = time.substring(0, 2);
                int hour = Integer.parseInt(hourStr);
                double timeRate = 0.1;
                // 定义时间段分类
                String timePeriod;
                if (hour >= 0 && hour < 6) {
                    timePeriod = "凌晨";
                    object.put("time_18_24", round(0.2 * timeRate));
                    object.put("time_25_29", round(0.1 * timeRate));
                    object.put("time_30_34", round(0.1 * timeRate));
                    object.put("time_35_39", round(0.1 * timeRate));
                    object.put("time_40_49", round(0.1 * timeRate));
                    object.put("time_50", round(0.1 * timeRate));
                } else if (hour >= 6 && hour < 9) {
                    timePeriod = "早晨";
                    object.put("time_18_24", round(0.1 * timeRate));
                    object.put("time_25_29", round(0.1 * timeRate));
                    object.put("time_30_34", round(0.1 * timeRate));
                    object.put("time_35_39", round(0.1 * timeRate));
                    object.put("time_40_49", round(0.2 * timeRate));
                    object.put("time_50", round(0.3 * timeRate));
                } else if (hour >= 9 && hour < 12) {
                    timePeriod = "上午";
                    object.put("time_18_24", round(0.2 * timeRate));
                    object.put("time_25_29", round(0.2 * timeRate));
                    object.put("time_30_34", round(0.2 * timeRate));
                    object.put("time_35_39", round(0.2 * timeRate));
                    object.put("time_40_49", round(0.3 * timeRate));
                    object.put("time_50", round(0.4 * timeRate));
                } else if (hour >= 12 && hour < 14) {
                    timePeriod = "中午";
                    object.put("time_18_24", round(0.4 * timeRate));
                    object.put("time_25_29", round(0.4 * timeRate));
                    object.put("time_30_34", round(0.4 * timeRate));
                    object.put("time_35_39", round(0.4 * timeRate));
                    object.put("time_40_49", round(0.4 * timeRate));
                    object.put("time_50", round(0.3 * timeRate));
                } else if (hour >= 14 && hour < 18) {
                    timePeriod = "下午";
                    object.put("time_18_24", round(0.4 * timeRate));
                    object.put("time_25_29", round(0.5 * timeRate));
                    object.put("time_30_34", round(0.5 * timeRate));
                    object.put("time_35_39", round(0.5 * timeRate));
                    object.put("time_40_49", round(0.5 * timeRate));
                    object.put("time_50", round(0.4 * timeRate));
                } else if (hour >= 18 && hour < 22) {
                    timePeriod = "晚上";
                    object.put("time_18_24", round(0.8 * timeRate));
                    object.put("time_25_29", round(0.7 * timeRate));
                    object.put("time_30_34", round(0.6 * timeRate));
                    object.put("time_35_39", round(0.5 * timeRate));
                    object.put("time_40_49", round(0.4 * timeRate));
                    object.put("time_50", round(0.3 * timeRate));
                } else {
                    timePeriod = "夜间";
                    object.put("time_18_24", round(0.9 * timeRate));
                    object.put("time_25_29", round(0.7 * timeRate));
                    object.put("time_30_34", round(0.5 * timeRate));
                    object.put("time_35_39", round(0.3 * timeRate));
                    object.put("time_40_49", round(0.2 * timeRate));
                    object.put("time_50", round(0.1 * timeRate));
                }
                object.put("name",value.getString("sku_name"));
                object.put("create_time",value.getString("create_time"));
                object.put("time_period", timePeriod);

                return object;
            }
        });

//        map.print();

        SingleOutputStreamOperator<JSONObject> operator = streamOperator.map(new RichMapFunction<JSONObject, JSONObject>() {
            final double lowPriceThreshold = 100.0;
            final double highPriceThreshold = 500.0;
            double priceRate = 0.15;

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                JSONObject object = new JSONObject();
                double price = value.getDouble("order_price");
                String priceRange;

                if (price < lowPriceThreshold) {
                    priceRange = "低价商品";
                    object.put("price_18_24", round(0.8 * priceRate));
                    object.put("price_25_29", round(0.6 * priceRate));
                    object.put("price_30_34", round(0.4 * priceRate));
                    object.put("price_35_39", round(0.3 * priceRate));
                    object.put("price_40_49", round(0.2 * priceRate));
                    object.put("price_50", round(0.1 * priceRate));
                } else if (price <= highPriceThreshold) {
                    priceRange = "中价商品";
                    object.put("price_18_24", round(0.2 * priceRate));
                    object.put("price_25_29", round(0.4 * priceRate));
                    object.put("price_30_34", round(0.6 * priceRate));
                    object.put("price_35_39", round(0.7 * priceRate));
                    object.put("price_40_49", round(0.8 * priceRate));
                    object.put("price_50", round(0.7 * priceRate));
                } else {
                    priceRange = "高价商品";
                    object.put("price_18_24", round(0.1 * priceRate));
                    object.put("price_25_29", round(0.2 * priceRate));
                    object.put("price_30_34", round(0.3 * priceRate));
                    object.put("price_35_39", round(0.4 * priceRate));
                    object.put("price_40_49", round(0.5 * priceRate));
                    object.put("price_50", round(0.6 * priceRate));
                }

                object.put("price_range", priceRange);
                object.put("name", value.getString("sku_name"));

                return object;
            }
        });

//        operator.print();

        SingleOutputStreamOperator<JSONObject> operator1 = streamOperator.map(new RichMapFunction<JSONObject, JSONObject>() {
            private List<DimBaseCategory> dim_base_categories;
            private Map<String, DimBaseCategory> categoryMap;
            private Connection connection;
            final double searchRate = 0.3;

            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                categoryMap = new HashMap<>();
                connection = JdbcUtil.getMySQLConnection();

                String sql1 = "  SELECT                                                        \n" +
                        "   b3.id, b3.name3 b3name, b2.name2 b2name, b1.name1 b1name     \n" +
                        "   FROM realtime_dmp.base_category3 as b3                             \n" +
                        "   JOIN realtime_dmp.base_category2 as b2                             \n" +
                        "   ON b3.category2_id = b2.id                                         \n" +
                        "   JOIN realtime_dmp.base_category1 as b1                             \n" +
                        "   ON b2.category1_id = b1.id                                         ";

                dim_base_categories = JdbcUtil.queryList(connection, sql1, DimBaseCategory.class, false);

            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                JSONObject result = new JSONObject();
                String skuId = value.getString("sku_id");
                if (skuId != null && !skuId.isEmpty()) {
                    for (DimBaseCategory dim_base_category : dim_base_categories) {
                        if (skuId.equals(dim_base_category.getId())) {
                            result.put("cast", dim_base_category.getName1());
                            break;
                        }
                    }
                }

                String searchCategory = result.getString("cast");
                if (searchCategory == null) {
                    searchCategory = "unknown";
                }
                switch (searchCategory) {
                    case "家居家装":
                        result.put("search_18_24", round(0.9 * searchRate));
                        result.put("search_25_29", round(0.7 * searchRate));
                        result.put("search_30_34", round(0.5 * searchRate));
                        result.put("search_35_39", round(0.3 * searchRate));
                        result.put("search_40_49", round(0.2 * searchRate));
                        result.put("search_50", round(0.1 * searchRate));
                        break;
                    case "服饰内衣":
                        result.put("search_18_24", round(0.2 * searchRate));
                        result.put("search_25_29", round(0.4 * searchRate));
                        result.put("search_30_34", round(0.6 * searchRate));
                        result.put("search_35_39", round(0.7 * searchRate));
                        result.put("search_40_49", round(0.8 * searchRate));
                        result.put("search_50", round(0.8 * searchRate));
                        break;
                    case "运动健康":
                        result.put("search_18_24", round(0.1 * searchRate));
                        result.put("search_25_29", round(0.4 * searchRate));
                        result.put("search_30_34", round(0.6 * searchRate));
                        result.put("search_35_39", round(0.7 * searchRate));
                        result.put("search_40_49", round(0.8 * searchRate));
                        result.put("search_50", round(0.8 * searchRate));
                        break;
                }

                return result;
            }

            @Override
            public void close() throws Exception {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }
        });

        operator1.print();

        env.execute("DwdTimeSoure");
    }
    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }
}
