package com.kafka.producer.thread;

import com.kafka.producer.service.impl.ProducerServiceImpl;
import org.apache.log4j.Logger;

import java.util.Date;

/**
 * Created by hjl on 2016/8/4.
 */
public class ProducerThread implements Runnable {

    private Logger log = Logger.getLogger(ProducerThread.class);

    private ProducerServiceImpl producerServiceImpl;

    private String message;

    private int sleepTime;


    public void run() {
        int index = 0;
        log.info("=================================   " + Thread.currentThread().getName() + "  run");
        while(true) {
            this.producerServiceImpl.sendeMessage(index % 50,Thread.currentThread().getName() + "--->" +new Date().toString());
            try {
                log.info("=================================sleep 3 second");
                Thread.sleep(this.sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            index++;
            log.info("=================================" + index);
        }
    }

    public ProducerServiceImpl getProducerServiceImpl() {
        return producerServiceImpl;
    }

    public void setProducerServiceImpl(ProducerServiceImpl producerServiceImpl) {
        this.producerServiceImpl = producerServiceImpl;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }
}
