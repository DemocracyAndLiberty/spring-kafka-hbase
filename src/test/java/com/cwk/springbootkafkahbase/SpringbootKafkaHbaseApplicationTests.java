package com.cwk.springbootkafkahbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.cwk.springbootkafkahbase.bean.Kafka_RealSync_Event;
import com.cwk.springbootkafkahbase.config.KafkaConfiguration;
import com.cwk.springbootkafkahbase.config.KafkaProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootKafkaHbaseApplicationTests {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Test
    public void testDemo() throws InterruptedException {
        kafkaTemplate.send("first", "this is my first demo");
        //休眠5秒，为了使监听器有足够的时间监听到topic的数据
        Thread.sleep(5000);
    }

    @Autowired(required = false)
    private AdminClient adminClient;

    @Test
    public void testCreateTopic() throws InterruptedException {
        NewTopic topic = new NewTopic("topic.quick.initial2", 1, (short) 1);
        adminClient.createTopics(Arrays.asList(topic));
        Thread.sleep(1000);
    }

    @Test
    public void testSelectTopicInfo() throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList("topic.quick.initial"));
        result.all().get().forEach((k, v) -> System.out.println("k: " + k + " ,v: " + v.toString() + "\n"));
    }

    @Resource
    private KafkaTemplate defaultKafkaTemplate;

    @Test
    public void testDefaultKafkaTemplate() {
        defaultKafkaTemplate.sendDefault("I`m send msg to default topic");
    }

    @Test
    public void testTemplateSend() {
        //发送带有时间戳的消息
        kafkaTemplate.send("topic.quick.demo", 0, System.currentTimeMillis(), 0, "send message with timestamp");

        //使用ProducerRecord发送消息
        ProducerRecord record = new ProducerRecord("topic.quick.demo", "use ProducerRecord to send message");
        kafkaTemplate.send(record);

        //使用Message发送消息
        Map map = new HashMap();
        map.put(KafkaHeaders.TOPIC, "topic.quick.demo");
        map.put(KafkaHeaders.PARTITION_ID, 0);
        map.put(KafkaHeaders.MESSAGE_KEY, 0);
        GenericMessage message = new GenericMessage("use Message to send message", new MessageHeaders(map));
        kafkaTemplate.send(message);
    }


    @Autowired
    private KafkaConfiguration.KafkaSendResultHandler producerListener;

    /**
     * 用KafkaSendResultHandler实现消息发送结果回调
     *
     * @throws InterruptedException
     */
    @Test
    public void testProducerListen() throws InterruptedException {
        kafkaTemplate.setProducerListener(producerListener);
        kafkaTemplate.send("topic.quick.demo", "test producer listen");
        Thread.sleep(1000);
    }

    /**
     * KafkaTemplate同步发送消息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testSyncSend() throws ExecutionException, InterruptedException {
        kafkaTemplate.send("topic.quick.demo", "test sync send message").get();
    }

    /**
     * 测试一下Kafka事务是否能正常使用
     *
     * @throws InterruptedException
     */
//    @Test
//    @Transactional
//    public void testTransactionalAnnotation() throws InterruptedException {
//        //kafkaTemplate.send("topic.quick.tran", "test transactional annotation");
//        //throw new RuntimeException("fail");
//    }

    /**
     * 使用KafkaTemplate.executeInTransaction开启事务
     * 这种方式开启事务是不需要配置事务管理器的，也可以称为本地事务。直接编写测试方法
     *
     * @throws InterruptedException
     */
    // @Test
//    public void testExecuteInTransaction() throws InterruptedException {
//        kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback() {
//            @Override
//            public Object doInOperations(KafkaOperations kafkaOperations) {
//                //kafkaOperations.send("topic.quick.tran", "test executeInTransaction");
//                //throw new RuntimeException("fail");
//                //return true;
//            }
//        });
//    }

    /**
     * 测试监听器工厂
     * 测试一下监听器能不能正常运行
     */
    @Test
    //@Transactional
    public void topicquickbeantest() {
        kafkaTemplate.send("topic.quick.bean", "send msg to beanListener");
    }

    /**
     * 编写测试方法，发送数据到对应的Topic中，运行测试我们可以看到控制台打印的日志，日志里面包含topic、partition、offset等信息，这其实就是完整的消息储存结构。
     */
    @Test
    public void testConsumerRecord() throws InterruptedException {
        kafkaTemplate.send("topic.quick.consumer", "test receive by consumerRecord");
        Thread.sleep(5000);
    }


    /**
     * 那我们来编写一下测试方法，在短时间内发送12条消息到topic中，可以看到运行结果，
     * 对应的监听方法总共拉取了三次数据，其中两次为5条数据，一次为2条数据
     * 加起来就是我们在测试方法发送的12条数据。证明我们的批量消费方法是按预期进行的
     */
    @Test
    public void testBatch() throws InterruptedException {
        for (int i = 0; i < 12; i++) {
            kafkaTemplate.send("topic.quick.batch", "test batch listener,dataNum-" + i);
        }
        Thread.sleep(10000);
    }

    /**
     * 只监听了0、1、2、3、4这几个partition
     *
     * @throws InterruptedException
     */
    @Test
    public void testBatch01() throws InterruptedException {
        for (int i = 0; i < 12; i++) {
            kafkaTemplate.send("topic.quick.batch.partition", "test batch listener,dataNum-" + i);
        }
        Thread.sleep(10000);
    }

    //topic.kafka2Hbase

    /**
     * @throws InterruptedException
     */
    @Test
    public void testBatchkafka2Hbase() throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            kafkaTemplate.send("topic.kafka2Hbase", "topic.kafka2Hbase_" + i);
        }
        Thread.sleep(60000);
    }


    @Autowired
    private KafkaProperties properties;

    /**
     * @throws InterruptedException
     */
    @Test
    public void testKafkaProperties() throws InterruptedException {
        System.out.println("###############properties##############");
        System.err.println(properties.getValueDeserializer() +
                "\n" + properties.getKeyDeserializer() +
                "\n" + properties.getAutoOffSetReset() +
                "\n" + properties.getServers()
        );
        System.out.println("###############properties end##############");
        Thread.sleep(60000);
    }

    @Test
    public void testBatch011() throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            kafkaTemplate.send("topic.cwkkafka2HbaseTest", "topic.cwkkafka2HbaseTest batch listener,dataNum-" + i);
        }
        Thread.sleep(30000);
    }

    @Test
    public void testBatch0111() throws InterruptedException {
        for (int i = 0; i < 5000; i++) {
            kafkaTemplate.send("topic-kafka2HbaseTest", "{\"name\":\"cwk"+i+"\",\"sex\":"+new Random().nextInt(2)+",\"age\":" + Math.round(new Random().nextDouble() * 100) + ",\"nationality\":\"China\",\"city\":\"chengdu\"}");
        }
        Thread.sleep(600000);
    }

    @Test
    public void json2Bean(){
        String data="{\"batchID\":\"201902151323421550208222708\",\"batchRowCount\":1,\"columnListInfo\":[{\"btype\":15,\"cflag\":64,\"colNum\":0,\"columnName\":\"ID\",\"columnValue\":\"13\",\"dataColNum\":0,\"len\":2,\"oraType\":[0,2],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":0,\"colNum\":1,\"columnName\":\"NAME\",\"columnValue\":\"a13\",\"dataColNum\":1,\"len\":3,\"oraType\":[0,9],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":2,\"columnName\":\"scn_code\",\"columnValue\":\"17398172\",\"dataColNum\":-1,\"len\":8,\"oraType\":[0,2],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":3,\"columnName\":\"scn_time\",\"columnValue\":\"2019-02-15 13:23:32\",\"dataColNum\":-1,\"len\":19,\"oraType\":[0,2],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":4,\"columnName\":\"loadtime\",\"columnValue\":\"2019-02-15 13:23:42\",\"dataColNum\":-1,\"len\":19,\"oraType\":[0,12],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":5,\"columnName\":\"trans_id\",\"columnValue\":\"1688978709293049\",\"dataColNum\":-1,\"len\":16,\"oraType\":[0,2],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":6,\"columnName\":\"seq_id\",\"columnValue\":\"1\",\"dataColNum\":-1,\"len\":1,\"oraType\":[0,2],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":7,\"columnName\":\"ds_rowid\",\"columnValue\":\"AAASSvAAEAAAAlVAAF\",\"dataColNum\":-1,\"len\":18,\"oraType\":[0,1],\"otype\":0,\"rowID\":\"\",\"rowNum\":0}],\"columnNum\":7,\"operation_type\":\"I\",\"owner\":\"X1\",\"rowNum\":0,\"tableName\":\"TEST001\"}";
        Kafka_RealSync_Event event = JSON.parseObject(data, Kafka_RealSync_Event.class);
        event.updateMetadata();
        event.updateMetadataMap();
        System.out.println(event.getMetadata().getMetadataJson());

    }


    @Test
    public void testBatch02() throws InterruptedException {
        for (int i = 0; i < 32; i++) {
            kafkaTemplate.send("topic-quick-batch-partition", "test batch listener,dataNum-" + i);

            //kafkaTemplate.send("topic-quick-batch-partition", "test-key-"+i,"test batch listener,dataNum-" + i);
        }
        Thread.sleep(20000);
    }

    @Test
    public void testAck() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            kafkaTemplate.send("topic-2parts", "topic-2parts-" + i);
        }
        Thread.sleep(20000);
    }


}
