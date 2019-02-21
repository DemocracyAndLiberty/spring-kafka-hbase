package com.cwk.springbootkafkahbase.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfiguration {

    @Autowired
    KafkaProperties properties;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = new HashMap<>();
        //配置Kafka实例的连接地址
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getServers().get(0) + ":" + properties.getServerPort());
        KafkaAdmin admin = new KafkaAdmin(props);
        return admin;
    }

    /**
     * ConcurrentKafkaListenerContainerFactory为创建Kafka监听器的工程类，这里只配置了消费者
     * 我们在创建监听容器前需要创建一个监听容器工厂
     *
     * @return
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() throws ClassNotFoundException {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    /**
     * 有了监听容器工厂，我们就可以使用它去创建我们的监听容器
     * Bean方式创建监听容器
     *
     * @return
     */
//    @Bean
//    public KafkaMessageListenerContainer demoListenerContainer() throws ClassNotFoundException {
//        ContainerProperties properties = new ContainerProperties("topic-quick-ack");
//
//        properties.setGroupId("topic-quick-ack-bean");
//
//        properties.setMessageListener(new MessageListener<Integer, String>() {
//            private Logger log = LoggerFactory.getLogger(this.getClass());
//
//            @Override
//            public void onMessage(ConsumerRecord<Integer, String> record) {
//                log.info("topic-quick-ack-bean receive : " + record.toString());
//            }
//        });
//
//        return new KafkaMessageListenerContainer(consumerFactory(), properties);
//    }

    //根据consumerProps填写的参数创建消费者工厂
    @Bean
    public ConsumerFactory<String, String> consumerFactory() throws ClassNotFoundException {
        return new DefaultKafkaConsumerFactory<>(consumerProps());
    }

    /**
     * 根据senderProps填写的参数创建生产者工厂
     * 使用注解方式开启事务还是比较方便的，不过首先需要我们配置KafkaTransactionManager，
     * 这个类就是Kafka提供给我们的事务管理类，我们需要使用生产者工厂来创建这个事务管理类。
     * 需要注意的是，我们需要在producerFactory中开启事务功能，并设置TransactionIdPrefix，
     * TransactionIdPrefix是用来生成Transactional.id的前缀。
     * 使用@Transactional注解方式
     *
     * @return
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() throws ClassNotFoundException {
        DefaultKafkaProducerFactory factory = new DefaultKafkaProducerFactory<>(senderProps());
        //factory.transactionCapable();
        //factory.setTransactionIdPrefix("tran-");
        return factory;
    }

//    @Bean
//    public KafkaTransactionManager transactionManager(ProducerFactory producerFactory) {
//        KafkaTransactionManager manager = new KafkaTransactionManager(producerFactory);
//        return manager;
//    }

    //kafkaTemplate实现了Kafka发送接收等功能
    @Bean
    //@Primary注解的意思是在拥有多个同类型的Bean时优先使用该Bean，到时候方便我们使用@Autowired注解自动注入
    @Primary
    public KafkaTemplate<String, String> kafkaTemplate() throws ClassNotFoundException {
        KafkaTemplate template = new KafkaTemplate<String, String>(producerFactory());
        return template;
    }

    //带有默认Topic参数的KafkaTemplate
    //在声明defaultKafkaTemplate这个Bean的时候添加了topicName
    //只要调用sendDefault方法，kafkaTemplate会自动把消息发送到名为"topic.quick.default"的Topic中
    @Bean("defaultKafkaTemplate")
    public KafkaTemplate<String, String> defaultKafkaTemplate() throws ClassNotFoundException {
        KafkaTemplate template = new KafkaTemplate<String, String>(producerFactory());
        template.setDefaultTopic("topic.quick.default");
        return template;
    }

    @Component
    public class KafkaSendResultHandler implements ProducerListener {

        private final Logger log = LoggerFactory.getLogger(KafkaSendResultHandler.class);

        @Override
        public void onSuccess(ProducerRecord producerRecord, RecordMetadata recordMetadata) {
            log.info("Message send success : " + producerRecord.toString());
        }

        @Override
        public void onError(ProducerRecord producerRecord, Exception exception) {
            log.info("Message send error : " + producerRecord.toString());
        }
    }


    //消费者配置参数
    private Map<String, Object> consumerProps() throws ClassNotFoundException {
        Map<String, Object> props = new HashMap<>();
        //连接地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getServers().get(1) + ":" + properties.getServerPort());
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.23.121:9092");
        //GroupID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bootKafka");
        //是否自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.isEnableAutoCommit());
        //自动提交的频率
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        //Session超时设置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        //键的反序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName(properties.getKeyDeserializer()));
        //值的反序列化方式
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName(properties.getValueDeserializer()));
        return props;
    }

    //生产者配置
    private Map<String, Object> senderProps() throws ClassNotFoundException {
        Map<String, Object> props = new HashMap<>();
        //连接地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getServers().get(1) + ":" + properties.getServerPort());
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.23.121:9092");
        //重试，0为不启用重试机制
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //控制批处理大小，单位为字节
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //批量发送，延迟为1毫秒，启用该功能能有效减少生产者发送消息次数，从而提高并发量
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //生产者可以使用的总内存字节来缓冲等待发送到服务器的记录
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024000);
        //键的序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Class.forName(properties.getKeySerializer()));
        //值的序列化方式
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Class.forName(properties.getValueSerializer()));
        return props;
    }

}
