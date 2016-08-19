package com.kafka.producer.service;

/**
 * Created by hjl on 2016/8/15.
 */
public interface ProducerSevice {
    void sendMessage(int partition,String key,String value);
    void sendMessage(int partition,String value);
    void sendMessage(String key,String value);
    void sendMessage(String value);

}
