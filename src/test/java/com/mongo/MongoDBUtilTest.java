package com.mongo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by hjl on 2016/8/9.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath*:test/spring-mongodb-test.xml")
public class MongoDBUtilTest {


    @Test
    public void test(){
        MongoDBUtil.instance.getDB("mongodb-test");
        System.out.print(MongoDBUtil.instance.getAllCollections("mongodb-test"));

    }





}
