package com.lzy.app.ods;

import com.lzy.stream.realtime.v1.utils.FlinkSinkUtil;
import com.lzy.stream.realtime.v1.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.lzy.app.ods.MysqlToKafka
 * @Author zheyuan.liu
 * @Date 2025/5/12 16:33
 * @description: 读取mysql数据写入kafka
 */

public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> realpriceV1 = FlinkSourceUtil.getMySqlSource("realprice_dmp", "*");

        DataStreamSource<String> mySQLSource = env.fromSource(realpriceV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mySQLSource.print();

        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("topic_dmp_db");

        mySQLSource.sinkTo(topic_db);

        env.execute();

    }
}
