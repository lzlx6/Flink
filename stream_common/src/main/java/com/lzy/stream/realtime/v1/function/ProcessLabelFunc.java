package com.lzy.stream.realtime.v1.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.lzy.stream.realtime.v1.function.ProcessLabelFunc
 * @Author zheyuan.liu
 * @Date 2025/5/15 20:40
 * @description:
 */

public class ProcessLabelFunc extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        left.putAll(right);
        collector.collect(left);

    }
}
