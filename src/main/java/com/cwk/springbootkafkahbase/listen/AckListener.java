package com.cwk.springbootkafkahbase.listen;

import com.cwk.springbootkafkahbase.config.KafkaProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class AckListener {
    @Autowired
    KafkaProperties properties;
    private static final Logger log = LoggerFactory.getLogger(AckListener.class);

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getServers().get(0) + ":" + properties.getServerPort());
        //设置ENABLE_AUTO_COMMIT_CONFIG=false，禁止自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getAutoCommitInterval());
        //props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, properties.getSessionTimeoutMs());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getAutoOffSetReset());
        //一次拉取消息数量
        //props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
        try {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName(properties.getKeyDeserializer()));
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName(properties.getValueDeserializer()));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return props;
    }

    @Bean("ackContainerFactory")
    public ConcurrentKafkaListenerContainerFactory ackContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        //设置AckMode=MANUAL_IMMEDIATE
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE);
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory(consumerProps()));
        factory.setConcurrency(2);
        //设置为批量监听
        //factory.setBatchListener(false);
        return factory;
    }

//    @Bean
//    public NewTopic batchTopic() {
//        return new NewTopic("topic-quick-ack", 2, (short) 1);
//    }


    /**
     * 使用Kafka的Ack机制比较简单，只需简单的三步即可：
     * <p>
     * 设置ENABLE_AUTO_COMMIT_CONFIG=false，禁止自动提交
     * 设置AckMode=MANUAL_IMMEDIATE
     * 监听方法加入Acknowledgment ack 参数
     * <p>
     * 怎么拒绝消息呢，只要在监听方法中不调用ack.acknowledge()即可
     *
     * @param record
     * @param ack
     */
    @KafkaListener(id = "topic-2parts", topics = "topic-2parts", topicPartitions = {
            @TopicPartition(topic = "topic-2parts",
                    partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))
    }, containerFactory = "ackContainerFactory")
    public void ackListener(ConsumerRecord record, Acknowledgment ack) {
        log.info("topic-2parts receive : " + record.value());
        ack.acknowledge();
    }

}
