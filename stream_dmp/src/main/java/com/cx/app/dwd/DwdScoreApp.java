package com.cx.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cx.bean.DimBaseCategory;
import com.cx.bean.DimCategoryCompare;
import com.cx.utils.FlinkSourceUtil;
import com.cx.utils.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DwdScoreApp {



    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KafkaSource<String> kafkaSourceLog = FlinkSourceUtil.getKafkaSource("minutes_page_Log", "page_Log");

        SingleOutputStreamOperator<String> kafka_source_log = env.fromSource(kafkaSourceLog, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> streamOperatorlog = kafka_source_log.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> operator = streamOperatorlog.map(new RichMapFunction<JSONObject, JSONObject>() {

            private List<DimBaseCategory> dim_base_categories;
            private Map<String, DimBaseCategory> categoryMap;
            private List<DimCategoryCompare> dimCategoryCompares;
            private Connection connection;

            final double deviceRate = 0.1;
            final double searchRate = 0.15;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化 Map
                categoryMap = new HashMap<>();

                // 获取数据库连接
                connection = JdbcUtil.getMySQLConnection();

                String sql1 = "select b3.id,                          \n" +
                        "            b3.name as b3name,              \n" +
                        "            b2.name as b2name,              \n" +
                        "            b1.name as b1name               \n" +
                        "     from realtime_dmp.base_category3 as b3  \n" +
                        "     join realtime_dmp.base_category2 as b2  \n" +
                        "     on b3.category2_id = b2.id             \n" +
                        "     join realtime_dmp.base_category1 as b1  \n" +
                        "     on b2.category1_id = b1.id";
                dim_base_categories = JdbcUtil.queryList(connection, sql1, DimBaseCategory.class, false);

                String sql2 = "select id, category_name, search_category from realtime_dmp.category_compare_dic;";
                dimCategoryCompares = JdbcUtil.queryList(connection, sql2, DimCategoryCompare.class, false);

                // 在 open 方法中初始化 categoryMap
                for (DimBaseCategory category : dim_base_categories) {
                    categoryMap.put(category.getB3name(), category);
                }

                super.open(parameters);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) {
                String os = jsonObject.getString("os");
                String[] labels = os.split(",");
                String judge_os = labels[0];
                jsonObject.put("judge_os", judge_os);

                if (judge_os.equals("iOS")) {
                    jsonObject.put("device_18_24", round(0.7 * deviceRate));
                    jsonObject.put("device_25_29", round(0.6 * deviceRate));
                    jsonObject.put("device_30_34", round(0.5 * deviceRate));
                    jsonObject.put("device_35_39", round(0.4 * deviceRate));
                    jsonObject.put("device_40_49", round(0.3 * deviceRate));
                    jsonObject.put("device_50", round(0.2 * deviceRate));
                } else if (judge_os.equals("Android")) {
                    jsonObject.put("device_18_24", round(0.8 * deviceRate));
                    jsonObject.put("device_25_29", round(0.7 * deviceRate));
                    jsonObject.put("device_30_34", round(0.6 * deviceRate));
                    jsonObject.put("device_35_39", round(0.5 * deviceRate));
                    jsonObject.put("device_40_49", round(0.4 * deviceRate));
                    jsonObject.put("device_50", round(0.3 * deviceRate));
                }


                String searchItem = jsonObject.getString("search_item");
                if (searchItem != null && !searchItem.isEmpty()) {
                    DimBaseCategory category = categoryMap.get(searchItem);
                    if (category != null) {
                        jsonObject.put("b1_category", category.getB1name());
                    }
                }
                // search
                String b1Category = jsonObject.getString("b1_category");
                if (b1Category != null && !b1Category.isEmpty()) {
                    for (DimCategoryCompare dimCategoryCompare : dimCategoryCompares) {
                        if (b1Category.equals(dimCategoryCompare.getCategoryName())) {
                            jsonObject.put("searchCategory", dimCategoryCompare.getSearchCategory());
                            break;
                        }
                    }
                }

                String searchCategory = jsonObject.getString("searchCategory");
                if (searchCategory == null) {
                    searchCategory = "unknown";
                }
                switch (searchCategory) {
                    case "时尚与潮流":
                        jsonObject.put("search_18_24", round(0.9 * searchRate));
                        jsonObject.put("search_25_29", round(0.7 * searchRate));
                        jsonObject.put("search_30_34", round(0.5 * searchRate));
                        jsonObject.put("search_35_39", round(0.3 * searchRate));
                        jsonObject.put("search_40_49", round(0.2 * searchRate));
                        jsonObject.put("search_50", round(0.1 * searchRate));
                        break;
                    case "性价比":
                        jsonObject.put("search_18_24", round(0.2 * searchRate));
                        jsonObject.put("search_25_29", round(0.4 * searchRate));
                        jsonObject.put("search_30_34", round(0.6 * searchRate));
                        jsonObject.put("search_35_39", round(0.7 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.8 * searchRate));
                        break;
                    case "健康与养生":
                    case "家庭与育儿":
                        jsonObject.put("search_18_24", round(0.1 * searchRate));
                        jsonObject.put("search_25_29", round(0.2 * searchRate));
                        jsonObject.put("search_30_34", round(0.4 * searchRate));
                        jsonObject.put("search_35_39", round(0.6 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.7 * searchRate));
                        break;
                    case "科技与数码":
                        jsonObject.put("search_18_24", round(0.8 * searchRate));
                        jsonObject.put("search_25_29", round(0.6 * searchRate));
                        jsonObject.put("search_30_34", round(0.4 * searchRate));
                        jsonObject.put("search_35_39", round(0.3 * searchRate));
                        jsonObject.put("search_40_49", round(0.2 * searchRate));
                        jsonObject.put("search_50", round(0.1 * searchRate));
                        break;
                    case "学习与发展":
                        jsonObject.put("search_18_24", round(0.4 * searchRate));
                        jsonObject.put("search_25_29", round(0.5 * searchRate));
                        jsonObject.put("search_30_34", round(0.6 * searchRate));
                        jsonObject.put("search_35_39", round(0.7 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.7 * searchRate));
                        break;
                    default:
                        jsonObject.put("search_18_24", 0);
                        jsonObject.put("search_25_29", 0);
                        jsonObject.put("search_30_34", 0);
                        jsonObject.put("search_35_39", 0);
                        jsonObject.put("search_40_49", 0);
                        jsonObject.put("search_50", 0);
                }


                return jsonObject;
            }

            private double round(double value) {
                return BigDecimal.valueOf(value)
                        .setScale(3, RoundingMode.HALF_UP)
                        .doubleValue();
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        operator.print();


        env.execute("minutes_page_Log");
    }
}