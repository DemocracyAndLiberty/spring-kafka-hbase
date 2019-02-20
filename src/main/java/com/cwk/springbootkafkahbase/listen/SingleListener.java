package com.cwk.springbootkafkahbase.listen;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SingleListener {
//
//    private static final Logger log = LoggerFactory.getLogger(SingleListener.class);
//
//    @KafkaListener(id = "consumer", topics = "topic.cwkkafka2HbaseTest")
//    public void consumerListener(ConsumerRecord<Integer, String> record) {
//        log.info("topic.cwkkafka2HbaseTest receive : " + record.toString());
//    }
}