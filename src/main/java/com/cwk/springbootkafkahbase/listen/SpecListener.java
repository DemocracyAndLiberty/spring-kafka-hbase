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
import java.util.UUID;

@Component
public class SpecListener {

    private static final Logger log = LoggerFactory.getLogger(SpecListener.class);

//    @Bean
//    public NewTopic batchWithPartitionTopic() {
//        return new NewTopic("topic.quick.batch.partition", 8, (short) 1);
//    }

    //    @KafkaListener(id = "batchWithPartition", clientIdPrefix = "bwp", containerFactory = "batchContainerFactory",topics = "topic-dsg005"
////            topicPartitions = {
////                    @TopicPartition(topic = "topic-dsg005",
////                            partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))
////            }
//    )
//    @KafkaListener(id = "topic-dsg005-consumer", topics = "topic-dsg005",containerFactory = "batchContainerFactory")
//    public void batchListenerWithPartition(List<String> datas) throws Exception {
//        log.info("topic-dsg005 consumer  receive data time:" + System.currentTimeMillis());
//
//        //检查表
//        if (!HbaseUtils.tableExist("kafka2HbaseTest")) {
//            HbaseUtils.createTable("kafka2HbaseTest", "info");
//        }
//        LinkedList<Object> list = new LinkedList<>();
//        for (String data : datas) {
//            Kafka_RealSync_Event event = JSON.parseObject(data, Kafka_RealSync_Event.class);
//            list.add(event);
//        }
//        //UUID.randomUUID().toString().replaceAll("-", "")
//        //UUID.randomUUID().toString().replaceAll("-", "")
//        HbaseUtils.putDataBatch("kafka2HbaseListener", list);
//
//    }


}
