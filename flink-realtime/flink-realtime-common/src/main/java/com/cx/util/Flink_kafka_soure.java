package com.cx.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class Flink_kafka_soure {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 流处理执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 Kafka 消费者
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cdh03:9092");
        properties.setProperty("group.id", "flink-consumer-group");

        // 创建 Kafka 消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "topic_log",
                new SimpleStringSchema(),
                properties);

        // 添加 Kafka 消费者为数据源
        DataStream<String> stream = env.addSource(kafkaConsumer);

        // 简单的数据处理（将输入字符串拆分为单词）
        DataStream<String> words = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) {
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        });

        // 将处理后的数据打印到控制台
        words.print();

        // 启动作业
        env.execute("Flink Kafka Consumer Job");
    }
}
