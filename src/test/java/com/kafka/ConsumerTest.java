package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Properties;

/**
 * Created by hjl on 2016/8/15.
 */

//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = "classpath*:test/spring-consumer-test.xml")
public class ConsumerTest {

    @Test
    public void testConsumer(){
        ApplicationContext ac = new ClassPathXmlApplicationContext("classpath*:test/spring-consumer-test.xml");

    }






    @Test
    public void test(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.109.130:9092,192.168.109.135:9092,192.168.109.136:9092");
        props.put("group.id", "consumerGroup2");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");  //自动commit
        props.put("auto.commit.interval.ms", "1000"); //定时commit的周期
        props.put("session.timeout.ms", "30000"); //consumer活性超时时间
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String , String>(props);
        consumer.subscribe("ubuntu-test4"); //subscribe，foo，bar，两个topic
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100).get("ubuntu-test");
            System.out.print(records.records(0));
            //poll 100 条 records
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("partition = %d ,offset = %d, key = %s, value = %s\n" ,record.partition(),record.offset(), record.key(), record.value());

        }
    }
}
