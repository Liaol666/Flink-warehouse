package com.zll.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zll.utils.MyKafkaUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @ClassName BaseLogApp
 * @Description TODO 处理行为日志，主要做新老用户校验及分流
 * TODO 涉及到使用valueState进行分流，使用侧输出流进行分流
 * @Author 17588
 * @Date 2021-08-09 15:26
 * @Version 1.0
 */
@Slf4j
public class BaseLogApp {
    private static OutputTag<String> displayTag = new OutputTag<>("display");
    private static OutputTag<String> pageTag = new OutputTag<>("page");
    public static void main(String[] args) throws Exception {
//        注册环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/realtime/baselog"));
        System.setProperty("HADOOP_USER_NAME", "zll");
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
//        获取kafka数据
        String logSource = "ods_base_log";
        String startSink = "dwd_start_log";
        String displaySink = "dwd_display_log";
        String pageSink = "dwd_page_log";
        String groupId = "baseLogApp";
        DataStreamSource<String> baseLogDS = env.addSource(MyKafkaUtils.getKafkaSource(logSource, groupId));
//        数据转换成javaObject，使用process主要是防止脏数据
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {};
        SingleOutputStreamOperator<JSONObject> jsonDS = baseLogDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String data, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(data);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    log.info(e.getMessage() + "数据转换异常");
                    context.output(dirtyTag, data);
                }
            }
        });
        DataStream<String> dirtyData = jsonDS.getSideOutput(dirtyTag);  //如果有脏数据可以在这里获取
//        分组，新老用户校验,因为要使用open,使用richMapFunc来进行判断
        KeyedStream<JSONObject, String> keybyDS = jsonDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> isnewDS = keybyDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState = null;
            private SimpleDateFormat simpleDateFormat = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("is_new", String.class));
            }
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                String value = valueState.value();
                if (is_new == "1") {
                    if (value != null && value.length() > 0) {
                        is_new = "0";
                        jsonObject.getJSONObject("common").put("is_new", is_new);
                    } else {
                        String format = simpleDateFormat.format(ts);
                        valueState.update(format);
                    }
                }
                return jsonObject;
            }
        });
//        使用侧输出流进行分流

        SingleOutputStreamOperator<String> startDS = isnewDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                String start = jsonObject.getString("start");
                JSONArray displays = jsonObject.getJSONArray("displays");
                String page = jsonObject.getString("page");
                if (start != null && start.length() > 0) {
                    collector.collect(jsonObject.toString());
                } else {
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            context.output(displayTag, display.toString());
                            display.put("page", jsonObject.getJSONObject("page").getString("page_id"));
                        }
                    }
                    context.output(pageTag, jsonObject.toString());
                }
            }
        });
        DataStream<String> displayDS = startDS.getSideOutput(displayTag);
        DataStream<String> pageDS = startDS.getSideOutput(pageTag);
//        写入dwd层的kafka中
        startDS.addSink(MyKafkaUtils.kafkaSink(startSink));
        startDS.addSink(MyKafkaUtils.kafkaSink(displaySink));
        startDS.addSink(MyKafkaUtils.kafkaSink(pageSink));
//        执行任务
        env.execute("base_log_app");
    }
}
