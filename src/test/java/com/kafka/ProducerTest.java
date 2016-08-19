package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Properties;

/**
 * Created by hjl on 2016/8/15.
 */


//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = "classpath*:test/spring-producer-test.xml")
public class ProducerTest {

    @Test
    public void testProducer(){

        ApplicationContext ac = new ClassPathXmlApplicationContext("classpath*:test/spring-producer-test.xml");

    }



    @Test
    public void testPro() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.109.130:9092");
        props.put("acks", "all"); //ack方式，all，会等所有的commit最慢的方式
        props.put("retries", 0); //失败是否重试，设置会有可能产生重复数据
        props.put("batch.size", 10); //对于每个partition的batch buffer大小
        props.put("linger.ms", 1);  //等多久，如果buffer没满，比如设为1，即消息发送会多1ms的延迟，如果buffer没满
        props.put("buffer.memory", 3355); //整个producer可以用于buffer的内存大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String,String>(props);
        for(int i = 0; i < 100; i++){
            producer.send(new ProducerRecord<String, String>("ubuntu-test", Integer.toString(i), Integer.toString(i)));
            System.out.println("===================发送"+ i);
            Thread.sleep(100);
        }

        producer.close();
    }

    @Test
    public void testTopicFilter(){
        String rawRegex = ".*";
        System.out.println(rawRegex.trim().replace(',', '|').replace(" ", "").replaceAll("^[\"\']+", "").replaceAll("[\"\']+$", ""));
    }

}
