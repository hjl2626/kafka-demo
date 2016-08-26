package com.mongo;

import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
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
        MongoCollection coll = MongoDBUtil.instance.getCollection("mongodb-test","logs");
        MongoCursor<Document> cur = coll.find().iterator();
        while(cur.hasNext()){
           Document _doc = cur.next();
            System.out.print(_doc);
        }

    }





}
