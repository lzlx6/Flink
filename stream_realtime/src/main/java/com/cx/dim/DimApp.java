package com.cx.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cx.bean.TableProcessDim;
import com.cx.constant.Constant;
import com.cx.function.HBaseSinkFunction;
import com.cx.function.TableProcessFunction;
import com.cx.utils.FlinkSourceUtil;
import com.cx.utils.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;


public class DimApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(5000L , CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

//        kafkaStrDS.print();

        //通过数据库和操作类型还有数据进行过滤保留符合条件的数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {

                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getJSONObject("source").getString("db");
                        String type = jsonObj.getString("op");
                        String data = jsonObj.getString("after");

                        if ("realtime_v1".equals(db)
                                && ("c".equals(type)
                                || "u".equals(type)
                                || "d".equals(type)
                                || "r".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );

//        jsonObjDS.print();

        //读取dim的表的基本结构，进行一个封装TableProcessDim传到下游
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v2", "table_process_dim");
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim;
                        if("d".equals(op)){
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        }else{
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

//        tpDS.print();

        //hbase建表
        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) {
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)){
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else{
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);

//        tpDS.print();

        //通过表找到数据进行封装TableProcessFunction，发送到下游
        //3> ({"birthday":10354,"op":"r","login_name":"5vifw38y83","gender":"M","create_time":1654646400000,"name":"马亮政","user_level":"3","id":1},
        // TableProcessDim(sourceTable=user_info, sinkTable=dim_user_info, sinkColumns=id,login_name,name,user_level,birthday,gender,create_time,operate_time, sinkFamily=info, sinkRowKey=id, op=null))
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("mapStateDescriptor",String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS
                .process(new TableProcessFunction(mapStateDescriptor));

        dimDS.print();

        dimDS.addSink(new HBaseSinkFunction());

        env.execute("dim");

    }
}
