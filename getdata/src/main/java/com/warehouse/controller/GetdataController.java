package com.warehouse.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName GetdataController
 * @Description TODO 将获取到的日志数据写入到kafka的ods_base_log主题，同时使用将日志写入文件中
 * @Author 17588
 * @Date 2021-08-09 15:02
 * @Version 1.0
 */

@RestController
@Slf4j
public class GetdataController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @RequestMapping("/kafka")
    public String getLogger(@RequestParam(value = "param") String param) {
        log.info(param);
        kafkaTemplate.send("ods_base_log", param);
        return "success";
    }
}
