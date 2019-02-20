package com.cwk.springbootkafkahbase.listen;

import com.cwk.springbootkafkahbase.config.KafkaProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 批量消费
 *
 * 重新创建一份新的消费者配置，配置为一次拉取5条消息
 * 创建一个监听容器工厂，设置其为批量消费并设置并发量为5，这个并发量根据分区数决定，
 * 必须小于等于分区数，否则会有线程一直处于空闲状态
 * 创建一个分区数为8的Topic
 * 创建监听方法，设置消费id为batch，clientID前缀为batch，监听topic.quick.batch，使用batchContainerFactory工厂创建该监听容器
 *
 */
@Component
public class BatchListener {

    private static final Logger log = LoggerFactory.getLogger(BatchListener.class);

    @Autowired
    KafkaProperties properties;

    private Map<String, Object> consumerProps()  {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getServers().get(0)+":"+properties.getServerPort());
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.23.121:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.isEnableAutoCommit());
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getAutoCommitInterval());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, properties.getSessionTimeoutMs());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,properties.getAutoOffSetReset());
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

    @Bean("batchContainerFactory")
    public ConcurrentKafkaListenerContainerFactory listenerContainer() {
        ConcurrentKafkaListenerContainerFactory container = new ConcurrentKafkaListenerContainerFactory();
        container.setConsumerFactory(new DefaultKafkaConsumerFactory(consumerProps()));
        //设置并发量，小于或等于Topic的分区数
        container.setConcurrency(1);
        //设置为批量监听
        container.setBatchListener(true);
        return container;
    }

//    @Bean
//    public NewTopic batchTopic() {
//        return new NewTopic("topic.quick.batch", 8, (short) 1);
//    }


//    @KafkaListener(id = "batch", clientIdPrefix = "batch", topics = {"topic.quick.batch"}, containerFactory = "batchContainerFactory")
//    public void batchListener(List<String> data) {
//        log.info("topic.quick.batch  receive : ");
//        for (String s : data) {
//            log.info(s);
//        }
//    }

}