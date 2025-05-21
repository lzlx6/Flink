package com.lzy.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lzy.stream.realtime.v1.function.ProcessJoinBase2And4BaseFunc;
import com.lzy.stream.realtime.v1.utils.FlinkSinkUtil;
import com.lzy.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.*;
import java.time.Duration;

/**
 * @Package com.lzy.app.dwd.DwdMerge
 * @Author zheyuan.liu
 * @Date 2025/5/15 20:25
 * @description:
 */

public class DwdMerge {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KafkaSource<String> timeBd = FlinkSourceUtil.getKafkaSource("dwd_time_soure", "dwd_app_time");

        KafkaSource<String> scoureBd = FlinkSourceUtil.getKafkaSource("DwdScore", "dwd_sheb_app");

        KafkaSource<String> DbBd = FlinkSourceUtil.getKafkaSource("DwdDbApp", "dwd_db_app");

        DataStreamSource<String> timeDs = env.fromSource(timeBd, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStreamSource<String> sourceDs = env.fromSource(scoureBd, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStreamSource<String> DbDs = env.fromSource(DbBd, WatermarkStrategy.noWatermarks(), "Kafka Source");


        SingleOutputStreamOperator<JSONObject> mapBase4LabelDs = timeDs.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));


        SingleOutputStreamOperator<JSONObject> mapBase2LabelDs = sourceDs.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));


        SingleOutputStreamOperator<JSONObject> mapBase6LabelDs = DbDs.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));

//        mapBase6LabelDs.print("用户");
//        mapBase2LabelDs.print("设备");
//        mapBase4LabelDs.print("时间");


        SingleOutputStreamOperator<JSONObject> join2_4Ds = mapBase2LabelDs.keyBy(o -> o.getString("uid").isEmpty())
                .intervalJoin(mapBase4LabelDs.keyBy(o -> o.getString("id").isEmpty()))
                .between(Time.hours(-24), Time.hours(24))
                .process(new ProcessJoinBase2And4BaseFunc());

//        join2_4Ds.print();

        SingleOutputStreamOperator<JSONObject> waterJoin2_4 = join2_4Ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLongValue("ts_ms")));

//        waterJoin2_4.print();

        SingleOutputStreamOperator<JSONObject> userLabelProcessDs = waterJoin2_4.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase6LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-80), Time.hours(80))
                .process(new ProcessLabelFunc());


        SingleOutputStreamOperator<String> map = userLabelProcessDs.map(data -> data.toString());

        map.print();

        map.sinkTo(FlinkSinkUtil.getKafkaSink("Merge"));

        String outputPath = "D:\\学习\\output.csv";

        map.addSink(new ExportToCSVSinkFunction(outputPath));

        env.execute();
    }
    public static class ExportToCSVSinkFunction implements SinkFunction<String> {
        private String filePath;

        public ExportToCSVSinkFunction(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void invoke(String value, Context context) throws IOException {
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(filePath, true), "UTF-8"))) {
                writer.write(value);
                writer.newLine();
            }
        }
    }

    public static class ProcessLabelFunc extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
        private transient ListState<JSONObject> processedObjectsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<JSONObject> descriptor = new ListStateDescriptor<>(
                    "processedObjects",
                    TypeInformation.of(new TypeHint<JSONObject>() {})
            );
            processedObjectsState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(JSONObject left, JSONObject right, Context ctx, Collector<JSONObject> out) throws Exception {
            // 合并左右流的数据
            JSONObject mergedObject = new JSONObject();
            mergedObject.putAll(left);
            mergedObject.putAll(right);

            // 检查是否已存在（此处简化为比较JSON字符串，实际可能需要更复杂的比较逻辑）
            boolean exists = false;
            for (JSONObject obj : processedObjectsState.get()) {
                if (obj.toString().equals(mergedObject.toString())) {
                    exists = true;
                    break;
                }
            }

            if (!exists) {
                // 添加到状态并输出
                processedObjectsState.add(mergedObject);
                out.collect(mergedObject);
            }
        }
    }
}
