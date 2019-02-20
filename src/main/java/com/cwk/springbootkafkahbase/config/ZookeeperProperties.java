package com.cwk.springbootkafkahbase.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "hbase.zookeeper")
public class ZookeeperProperties {

    private List<String> quorums = new ArrayList<String>();
    @Value("${hbase.zookeeper.property.clientPort}")
    private String clientPort;

    public List<String> getQuorums() {
        return quorums;
    }

    public void setQuorums(List<String> quorums) {
        this.quorums = quorums;
    }

    public String getClientPort() {
        return clientPort;
    }

    public void setClientPort(String clientPort) {
        this.clientPort = clientPort;
    }
}
