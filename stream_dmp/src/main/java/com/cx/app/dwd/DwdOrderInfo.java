package com.lzy.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lzy.stream.realtime.v1.utils.FlinkSinkUtil;
import com.lzy.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.*;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.lzy.app.dwd.DwdOrderInfo
 * @Author zheyuan.liu
 * @Date 2025/5/14 19:21
 * @description:
 */

public class DwdOrderInfo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KafkaSource<String> kafkaSourceBd = FlinkSourceUtil.getKafkaSource("topic_dmp_db", "dwd_app");

        DataStreamSource<String> kafka_source = env.fromSource(kafkaSourceBd, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> streamOperator = kafka_source.map(JSON::parseObject).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts_ms");
                    }
                }));

        SingleOutputStreamOperator<JSONObject> orderDs = streamOperator.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"));

        SingleOutputStreamOperator<JSONObject> detailDs = streamOperator.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"));

        SingleOutputStreamOperator<JSONObject> operator = orderDs.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject rebuce = new JSONObject();
                JSONObject after = value.getJSONObject("after");
                rebuce.put("id", after.getString("id"));
                rebuce.put("user_id", after.getString("user_id"));
                rebuce.put("total_amount", after.getString("total_amount"));
                if (after != null && after.containsKey("create_time")) {
                    Long timestamp = after.getLong("create_time");
                    if (timestamp != null) {
                        // 使用LocalDateTime保留时间信息
                        LocalDateTime dateTime = Instant.ofEpochMilli(timestamp)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDateTime();

                        // 使用包含时间的格式
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        rebuce.put("create_time", dateTime.format(formatter));
                    }
                }
                rebuce.put("ts_ms", value.getLong("ts_ms"));
                out.collect(rebuce);
            }
        });

        SingleOutputStreamOperator<JSONObject> detail = detailDs.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject rebuce = new JSONObject();
                JSONObject after = value.getJSONObject("after");
                rebuce.put("id", after.getString("id"));
                rebuce.put("order_id", after.getString("order_id"));
                rebuce.put("sku_id", after.getString("sku_id"));
                rebuce.put("sku_name", after.getString("sku_name"));
                rebuce.put("sku_num", after.getString("sku_num"));
                rebuce.put("order_price", after.getString("order_price"));
                rebuce.put("split_activity_amount", value.getLong("split_activity_amount"));
                rebuce.put("split_total_amount", value.getLong("split_total_amount"));
                rebuce.put("ts_ms", value.getLong("ts_ms"));
                out.collect(rebuce);
            }
        });

        KeyedStream<JSONObject, String> idBy = operator.keyBy(data -> data.getString("id"));

        KeyedStream<JSONObject, String> oidBy = detail.keyBy(data -> data.getString("order_id"));

        SingleOutputStreamOperator<JSONObject> outputStreamOperator = idBy.intervalJoin(oidBy)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject merged = new JSONObject();
                        merged.put("id", left.getString("id"));
                        merged.put("user_id", left.getString("user_id"));
                        merged.put("total_amount", left.getString("total_amount"));
                        merged.put("create_time", left.getString("create_time"));
                        merged.put("detail_id", right.getLong("id"));
                        merged.put("order_id", right.getString("order_id"));
                        merged.put("sku_id", right.getString("sku_id"));
                        merged.put("sku_name", right.getString("sku_name"));
                        merged.put("sku_num", right.getString("sku_num"));
                        merged.put("order_price", right.getString("order_price"));
                        merged.put("split_activity_amount", right.getLong("split_activity_amount"));
                        merged.put("split_total_amount", right.getLong("split_total_amount"));
                        merged.put("ts_ms", right.getLong("ts_ms"));
                        out.collect(merged);
                    }
                });


        SingleOutputStreamOperator<JSONObject> operator1 = outputStreamOperator.keyBy(data -> data.getString("detail_id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<Long> latestTsState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Long> descriptor =
                                new ValueStateDescriptor<>("latestTs", Long.class);
                        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(1)).build());
                        latestTsState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        Long storedTs = latestTsState.value();
                        long currentTs = value.getLong("ts_ms");

                        if (storedTs == null || currentTs > storedTs) {
                            latestTsState.update(currentTs);
                            out.collect(value);
                        }
                    }
                });

        operator1.map(data -> data.toString()).sinkTo(FlinkSinkUtil.getKafkaSink("dwd_order_info_join"));


        env.execute("DwdOrderInfo");
    }
}
