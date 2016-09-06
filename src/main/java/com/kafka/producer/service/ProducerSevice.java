package com.kafka.producer.service;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * Created by hjl on 2016/8/15.
 */
public interface ProducerSevice {
    Future<RecordMetadata> sendMessage(int partition, String key, String value);
    Future<RecordMetadata> sendMessage(int partition,String value);
    Future<RecordMetadata> sendMessage(String key,String value);
    Future<RecordMetadata> sendMessage(String value);

}
