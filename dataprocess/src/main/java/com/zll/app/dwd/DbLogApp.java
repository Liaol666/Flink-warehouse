package com.zll.app.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName DbLogApp
 * @Description TODO
 * @Author 17588
 * @Date 2021-08-09 16:18
 * @Version 1.0
 */
public class DbLogApp {
    public static void main(String[] args) {
        //    获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().setCheckpointTimeout(6000L);

//    获取kafka中数据流
//    数据进行转换
//    分组
//    使用动态分流进行处理
//    将数据写入kafka
//    执行任务
    }
}

