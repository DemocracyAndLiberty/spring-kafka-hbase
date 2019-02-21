package com.cwk.springbootkafkahbase.listen;

import com.alibaba.fastjson.JSON;
import com.cwk.springbootkafkahbase.bean.Kafka_RealSync_Event;
import com.cwk.springbootkafkahbase.utils.HbaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class BatchEvent2HbaseListner {

//    private static final Logger log = LoggerFactory.getLogger(BatchEvent2HbaseListner.class);
//
//    @KafkaListener(id = "topic-dsg005-consumer", topics = "topic-dsg005",containerFactory = "batchContainerFactory")
//    public void batchListenerWithPartition(List<String> datas) throws Exception {
//        log.info("topic-dsg005 consumer  receive data time:" + System.currentTimeMillis());
//
//        //检查表
//        if (!HbaseUtils.tableExist1("kafka2HbaseEventTest")) {
//            HbaseUtils.createTable("kafka2HbaseEventTest", "event","metadata");
//        }
//        LinkedList<Object> list = new LinkedList<>();
//        for (String data : datas) {
//            Kafka_RealSync_Event event = JSON.parseObject(data, Kafka_RealSync_Event.class);
//            event.updateMetadata();
//            event.updateMetadataMap();
//            list.add(event);
//        }
//        //UUID.randomUUID().toString().replaceAll("-", "")
//        //UUID.randomUUID().toString().replaceAll("-", "")
//        HbaseUtils.putDataBatch("kafka2HbaseEventTest", list,"event","metadata");
//
//    }

}
