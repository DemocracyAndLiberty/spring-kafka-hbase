package com.cwk.springbootkafkahbase;

import com.cwk.springbootkafkahbase.config.KafkaProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringbootKafkaHbaseApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootKafkaHbaseApplication.class, args);
    }

}
