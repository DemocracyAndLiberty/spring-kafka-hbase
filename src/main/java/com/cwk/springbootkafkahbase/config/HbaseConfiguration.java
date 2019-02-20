package com.cwk.springbootkafkahbase.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.context.annotation.Bean;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

@Configuration
public class HbaseConfiguration {


    private static Admin admin = null;
    private static Connection connection = null;
    private static org.apache.hadoop.conf.Configuration configuration = null;


    @Autowired
    ZookeeperProperties properties;

    @Bean
    public Connection getConnection(){
        if (configuration==null)
        {
            //Hbase配置文件
            configuration = HBaseConfiguration.create();
            //配置zk主机
            configuration.set("hbase.zookeeper.quorum", properties.getQuorums().get(2));
            configuration.set("hbase.zookeeper.property.clientPort", properties.getClientPort());
        }
        if(connection==null)
        {
            try {
                connection= ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return connection;
    }

    @Bean
    public Admin getAdmin() {

        //Hbase配置文件
        configuration = HBaseConfiguration.create();
        //配置zk主机
        configuration.set("hbase.zookeeper.quorum", properties.getQuorums().get(2));
        configuration.set("hbase.zookeeper.property.clientPort", properties.getClientPort());


        //创建连接
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return admin;
    }


}
