package com.cwk.springbootkafkahbase.listen;

import com.alibaba.fastjson.JSON;
import com.cwk.springbootkafkahbase.bean.Kafka_RealSync_Event;
import com.cwk.springbootkafkahbase.utils.HbaseUtils;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

@Component
public class SpecListener {

   /* private static final Logger log = LoggerFactory.getLogger(SpecListener.class);

    @Bean
    public NewTopic batchWithPartitionTopic() {
        return new NewTopic("topic-quick-batch-partition", 5, (short) 1);
    }

    *//**
     * @TopicPartition：topic--需要监听的Topic的名称，partitions --需要监听Topic的分区id，
     * partitionOffsets --可以设置从某个偏移量开始监听
     * @PartitionOffset：partition --分区Id，非数组，initialOffset --初始偏移量
     *
     *//*
     *//*   @KafkaListener(id = "batchWithPartition", clientIdPrefix = "bwp", containerFactory = "batchContainerFactory",topics = "topic-dsg005",
            topicPartitions = {
                    @TopicPartition(topic = "topic-dsg005",
                            partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))
            }
    )*//*
     *//**
     * data ： 对于data值的类型其实并没有限定，根据KafkaTemplate所定义的类型来决定。
     * data为List集合的则是用作批量消费。
     * ConsumerRecord：具体消费数据类，包含Headers信息、分区信息、时间戳等
     * Acknowledgment：用作Ack机制的接口
     * Consumer：消费者类，使用该类我们可以手动提交偏移量、控制消费速率等功能
     *
     *
     * id：消费者的id，当GroupId没有被配置的时候，默认id为GroupId
     * containerFactory：上面提到了@KafkaListener区分单数据还是多数据消费只需要配置一下
     * 注解的containerFactory属性就可以了，这里面配置的是监听容器工厂，
     * 也就是ConcurrentKafkaListenerContainerFactory，配置BeanName
     * topics：需要监听的Topic，可监听多个
     * topicPartitions：可配置更加详细的监听信息，必须监听某个Topic中的指定分区，
     * 或者从offset为200的偏移量开始监听
     * errorHandler：监听异常处理器，配置BeanName
     * groupId：消费组ID
     * idIsGroup：id是否为GroupId
     * clientIdPrefix：消费者Id前缀
     * beanRef：真实监听容器的BeanName，需要在 BeanName前加 "__"
     *
     *//*
     *//**
     * 当你接收的消息包含请求头，以及你监听方法需要获取该消息非常多的字段时可以通过这种方式，
     * 毕竟get方法代码量还是稍多点的。这里使用的是默认的监听容器工厂创建的，
     * 如果你想使用批量消费，把对应的类型改为List即可，比如List<String> data ， List<Integer> key。
     * @Payload：获取的是消息的消息体，也就是发送内容
     * //如果是消费单条消息，没有传key的话这里获取会抛异常，批量消费则会置为null
     * Missing header 'kafka_receivedMessageKey' for method parameter type [int]
     * @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY)：获取发送消息的key
     * @Header(KafkaHeaders.RECEIVED_PARTITION_ID)：获取当前消息是从哪个分区中监听到的
     * @Header(KafkaHeaders.RECEIVED_TOPIC)：获取监听的TopicName
     * @Header(KafkaHeaders.RECEIVED_TIMESTAMP)：获取时间戳
     *
     * @param data
     * @param key
     * @param partition
     * @param topic
     * @param ts
     * @throws Exception
     *//*
    @KafkaListener(id = "topic-topic-quick-batch-partition-consumer",groupId = "topic-quick-batch-partition-group", topics = "topic-quick-batch-partition",containerFactory = "batchContainerFactory")
    public void batchListenerWithPartition(@Payload String data,
                                           @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) int key,
                                           @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition,
                                           @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                           @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long ts
                                           ) throws Exception {
        log.info("topic-topic-quick-batch-partition-consumer consumer  receive datas time:" + System.currentTimeMillis());

        //for (String data : datas) {
            //log.info("topic-topic-quick-batch-partition-consumer print:"+data);
        log.info("\n"+"data: "+data+"\n"
        +"key: "+key+"\n"
                +"partition: "+partition+"\n"
                +"topic: "+topic+"\n"
                +"ts: "+ts+"\n"
                );
        //}

    }*/


}
