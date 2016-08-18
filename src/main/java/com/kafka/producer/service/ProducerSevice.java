package com.kafka.producer.service;

/**
 * Created by hjl on 2016/8/15.
 */
public interface ProducerSevice {
    void sendeMessage(int index,String message);
    //void sendeMessage(String message);
}
