package com.cwk.springbootkafkahbase.listen;

import com.cwk.springbootkafkahbase.config.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class BatchListner2Hbase {
    private static final Logger log = LoggerFactory.getLogger(BatchListner2Hbase.class);

//    @KafkaListener(id = "cwkkafka2HbaseTest-batch", clientIdPrefix = "cwkkafka2HbaseTest-batch", topics = {"topic.cwkkafka2HbaseTest"}, containerFactory = "batchContainerFactory")
//    public void batchListener(List<String> data) throws Exception {
//        log.info(System.currentTimeMillis()+"\t topic.kafka2Hbase receive data");
//        //判断表是否存在
//        try {
//            HbaseUtils.createTable("cwkkafka2HbaseTest", "info", "test_info");
//        } catch (Exception e) {
//        }
//        for (String s : data) {
//            //写入Hbase
//            HbaseUtils.putData("cwkkafka2HbaseTest", (System.currentTimeMillis()+"").substring(5), "info", "test_value", "test_value_"+s);
//        }
//    }

//    @Bean
//    public NewTopic batchTopic() {
//        return new NewTopic("topic.cwkkafka2HbaseTest", 1, (short) 1);
//    }

//    @KafkaListener(id = "cwkkafka2HbaseTest-batch", clientIdPrefix = "cwkkafka2HbaseTest-batch", topics = {"topic.cwkkafka2HbaseTest"},containerFactory = "batchContainerFactory")
//    public void batchListener(List<String> data) throws Exception {
//        log.info(System.currentTimeMillis() + "\t cwkkafka2HbaseTest receive data");
//
//        for (String s : data) {
//            System.out.println("cwkkafka2HbaseTest data is: " + s);
//        }
//
////        //判断表是否存在
////        try {
////            HbaseUtils.createTable("cwkkafka2HbaseTest", "info", "test_info");
////        } catch (Exception e) {
////        }
////        for (String s : data) {
////            //写入Hbase
////            HbaseUtils.putData("cwkkafka2HbaseTest", (System.currentTimeMillis()+"").substring(5), "info", "test_value", "test_value_"+s);
////        }
//    }


  /*  @Autowired
    KafkaProperties properties;

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getServers().get(0) + ":" + properties.getServerPort());
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.23.121:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.isEnableAutoCommit());
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getAutoCommitInterval());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, properties.getSessionTimeoutMs());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getAutoOffSetReset());
        //一次拉取消息数量
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
        try {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName(properties.getKeyDeserializer()));
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName(properties.getValueDeserializer()));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return props;
    }

    @Bean("kafka2HbaseTestContainerFactory")
    public ConcurrentKafkaListenerContainerFactory listenerContainer() {
        ConcurrentKafkaListenerContainerFactory container = new ConcurrentKafkaListenerContainerFactory();
        container.setConsumerFactory(new DefaultKafkaConsumerFactory(consumerProps()));
        //设置并发量，小于或等于Topic的分区数
        container.setConcurrency(1);
        //设置为批量监听
        container.setBatchListener(true);
        return container;
    }*/

//    @Bean
//    public NewTopic batchTopic() {
//        return new NewTopic("topic-kafka2HbaseTest", 1, (short) 1);
//    }

    //@KafkaListener(id = "kafka2HbaseConsumer-batch",clientIdPrefix = "kafka2HbaseTest-batch", topics = {"topic-kafka2HbaseTest"},containerFactory = "kafka2HbaseTestContainerFactory")

    //public static int sum=0;

//    @KafkaListener(id = "batchWithPartition", clientIdPrefix = "bwp", containerFactory = "kafka2HbaseTestContainerFactory", topics = "topic-kafka2HbaseTest",
//            topicPartitions = {
//                    @TopicPartition(topic = "topic-kafka2HbaseTest",
//                            partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))
//            }
//    )
//    public void kafka2HbaseListener(List<String> datas) throws Exception {
//
//        log.info("kafka2HbaseListener received data");
//
//        //检查表
//        if (!HbaseUtils.tableExist("kafka2HbaseListener")) {
//            HbaseUtils.createTable("kafka2HbaseListener", "info");
//        }
//        LinkedList<Object> list = new LinkedList<>();
//        for (String data : datas) {
//            Person person = JSON.parseObject(data, Person.class);
//            list.add(person);
//        }
//        HbaseUtils.putDataBatch("kafka2HbaseListener", UUID.randomUUID().toString().replaceAll("-", ""), "info", list);
//    }
}
