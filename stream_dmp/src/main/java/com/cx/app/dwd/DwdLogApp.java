package com.cx.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cx.bean.DimBaseCategory;
import com.cx.bean.DimCategoryCompare;
import com.cx.utils.ConfigUtils;
import com.cx.utils.FlinkSinkUtil;
import com.cx.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.time.Duration;
import java.util.*;

public class DwdLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSourceLog = FlinkSourceUtil.getKafkaSource("topic_log", "dwd_app");

        SingleOutputStreamOperator<String> kafka_source_log = env.fromSource(kafkaSourceLog, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> streamOperatorlog = kafka_source_log.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
               }));

        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = streamOperatorlog.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("common")) {
                    JSONObject common = jsonObject.getJSONObject("common");
                    result.put("uid", common.getString("uid") != null ? common.getString("uid") : "-1");
                    result.put("ts", jsonObject.getLongValue("ts"));
                    JSONObject deviceInfo = new JSONObject();
                    common.remove("sid");
                    common.remove("mid");
                    common.remove("is_new");
                    deviceInfo.putAll(common);
                    result.put("deviceInfo", deviceInfo);
                    if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()) {
                        JSONObject pageInfo = jsonObject.getJSONObject("page");
                        if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")) {
                            String item = pageInfo.getString("item");
                            result.put("search_item", item);
                        }
                    }
                }
                JSONObject deviceInfo = result.getJSONObject("deviceInfo");
                String os = deviceInfo.getString("os").split(" ")[0];
                deviceInfo.put("os", os);

                return result;
            }
        });

        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));

        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private Logger LOG = LoggerFactory.getLogger(String.class);
            private ValueState<HashSet<String>> processedDataState;

            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<HashSet<String>> descriptor = new ValueStateDescriptor<>(
                        "processedDataState",
                        TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<HashSet<String>>() {})
                );
                processedDataState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                HashSet<String> processedData = processedDataState.value();
                if (processedData == null) {
                    processedData = new HashSet<>();
                }

                String dataStr = value.toJSONString();
                LOG.info("Processing data: {}", dataStr);
                if (!processedData.contains(dataStr)) {
                    LOG.info("Adding new data to set: {}", dataStr);
                    processedData.add(dataStr);
                    processedDataState.update(processedData);
                    out.collect(value);
                } else {
                    LOG.info("Duplicate data found: {}", dataStr);
                }
            }
        });
//        logDeviceInfoDs.print();

        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(value -> value.getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    ValueState<Long> pvState;
                    MapState<String, Set<String>> uvState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        pvState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pv", Long.class));

                        // 初始化字段集合状态（使用TypeHint保留泛型信息）
                        MapStateDescriptor<String, Set<String>> fieldsDescriptor =
                                new MapStateDescriptor<>("fields-state", Types.STRING, TypeInformation.of(new TypeHint<Set<String>>() {}));

                        uvState = getRuntimeContext().getMapState(fieldsDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 更新PV
                        Long pv = pvState.value() == null ? 1L : pvState.value() + 1;
                        pvState.update(pv);

                        // 提取设备信息和搜索词
                        JSONObject deviceInfo = value.getJSONObject("deviceInfo");
                        String os = deviceInfo.getString("os");
                        String ch = deviceInfo.getString("ch");
                        String md = deviceInfo.getString("md");
                        String ba = deviceInfo.getString("ba");
                        String searchItem = value.containsKey("search_item") ? value.getString("search_item") : null;

                        // 更新字段集合
                        updateField("os", os);
                        updateField("ch", ch);
                        updateField("md", md);
                        updateField("ba", ba);
                        if (searchItem != null) {
                            updateField("search_item", searchItem);
                        }

                        // 构建输出JSON
                        JSONObject output = new JSONObject();
                        output.put("uid", value.getString("uid"));
                        output.put("pv", pv);
                        output.put("os", String.join(",", getField("os")));
                        output.put("ch", String.join(",", getField("ch")));
                        output.put("md", String.join(",", getField("md")));
                        output.put("ba", String.join(",", getField("ba")));
                        output.put("search_item", String.join(",", getField("search_item")));

                        out.collect(output);
                    }

                    // 辅助方法：更新字段集合
                    private void updateField(String field, String value) throws Exception {
                        Set<String> set = uvState.get(field) == null ? new HashSet<>() : uvState.get(field);
                        set.add(value);
                        uvState.put(field, set);
                    }

                    // 辅助方法：获取字段集合
                    private Set<String> getField(String field) throws Exception {
                        return uvState.get(field) == null ? Collections.emptySet() : uvState.get(field);
                    }
                });

        SingleOutputStreamOperator<JSONObject> reduce = win2MinutesPageLogsDs.keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2);

        SingleOutputStreamOperator<String> operator = reduce.map(data -> data.toString());

        operator.print();

//        operator.sinkTo(FlinkSinkUtil.getKafkaSink("minutes_page_Log"));

        env.execute("DwdLogApp");
    }
}
