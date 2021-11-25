package com.atguigu.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String getLogger(@RequestParam("param") String logStr){
        //将数据落盘  使用第三方记录日志的插件logback完成
        //打印
        log.info(logStr);
        //将数据发送到kafka
        kafkaTemplate.send("ods_base_log",logStr);
        return "success";
    }
}
