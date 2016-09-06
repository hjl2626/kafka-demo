package com.kafka.producer.thread;

import com.kafka.producer.service.impl.ProducerServiceImpl;
import org.apache.log4j.Logger;

/**
 * Created by hjl on 2016/8/4.
 */
public class ProducerThread implements Runnable {

    private static Logger logger = Logger.getLogger(ProducerThread.class);

    /**
     *
     */
    private ProducerServiceImpl producerServiceImpl;

    /**
     *
     */
    private String message;

    /**
     *
     */
    private Integer sleepTime;

    /**
     *
     */
    private Integer partitionNum;


    public void run() {
        int index = 0;
        logger.info("=================================   " + Thread.currentThread().getName() + "  run");
        while (true) {
            this.producerServiceImpl.sendMessage(index % partitionNum, Thread.currentThread().getName() + "--->" + System.nanoTime());
            try {
                logger.info("=================================sleep " + sleepTime + "ms");
                Thread.sleep(this.sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            index++;
            logger.info("================================= " + index);
        }
    }

    public ProducerServiceImpl getProducerServiceImpl() {
        return producerServiceImpl;
    }

    public ProducerThread setProducerServiceImpl(ProducerServiceImpl producerServiceImpl) {
        this.producerServiceImpl = producerServiceImpl;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public ProducerThread setMessage(String message) {
        this.message = message;
        return this;
    }

    public Integer getSleepTime() {
        return sleepTime;
    }

    public ProducerThread setSleepTime(Integer sleepTime) {
        this.sleepTime = sleepTime;
        return this;
    }

    public Integer getPartitionNum() {
        return partitionNum;
    }

    public ProducerThread setPartitionNum(Integer partitionNum) {
        this.partitionNum = partitionNum;
        return this;
    }
}
