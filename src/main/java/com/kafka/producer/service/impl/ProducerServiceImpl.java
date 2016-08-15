package com.kafka.producer.service.impl;

import com.kafka.producer.service.ProducerSevice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Created by hjl on 2016/8/4.
 */

public class ProducerServiceImpl implements ProducerSevice{
    /**
     * kafka的brokers列表
     */

    private static Logger log = Logger.getLogger(ProducerServiceImpl.class);

    private String brokers;

    /**
     * acks的值，只能有三种-1、0还有1
     */
    private String required_acks = "0";

    /**
     * 请求超时间，默认为1000l
     */
    private Long request_timeout = 1000l;

    /**
     * kafka主服务对象
     */
    private Producer<String,String> producer;

    /**
     * 分区数量
     */
    //private Integer partitionNumber;

    /**
     *
     */
    private String topicName;

    /**
     *
     */
    private String producerType;

    /**
     *
     */

    private Integer retries;

    /**
     *
     */
    private Integer batchSize;

    /**
     *
     */
    private Integer bufferMemory;

    /**
     *
     */
    private String keySerializer;

    /**
     *
     */
    private String valueSerializer;


    /* (non-Javadoc)
     * @see test.interrupter.producer.ProducerServiceImpl#init()
     */
    public void init() {
        // 验证所有必要属性都已设置

        log.info("===================>>>>>>>             ProducerServiceImpl  init");
//        if (StringUtils.isEmpty(this.brokers)) {
//            throw new RuntimeException("至少需要指定一个broker的位置");
//        }
//        if (this.required_acks != 0 && this.required_acks != 1
//                && this.required_acks != -1) {
//            throw new RuntimeException("错误的required_acks值！");
//        }
//        if (this.partitionNumber <= 0) {
//            throw new RuntimeException("partitionNumber至少需要有1个");
//        }



        Properties props = new Properties();
        props.put("bootstrap.servers", this.brokers);
        props.put("acks", this.required_acks); //ack方式，all，会等所有的commit最慢的方式
        props.put("retries", this.retries); //失败是否重试，设置会有可能产生重复数据
        props.put("batch.size", this.batchSize); //对于每个partition的batch buffer大小
        //props.put("linger.ms", 1);  //等多久，如果buffer没满，比如设为1，即消息发送会多1ms的延迟，如果buffer没满
        props.put("buffer.memory", this.bufferMemory); //整个producer可以用于buffer的内存大小
        props.put("key.serializer", this.keySerializer);
        props.put("value.serializer", this.valueSerializer);

        this.producer = new KafkaProducer<String,String>(props);


    }

    /* (non-Javadoc)
     * @see test.interrupter.producer.ProducerServiceImpl#senderMessage(java.lang.String)
     */
    public void sendeMessage(String message) {
        // 创建和发送消息
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>(this.topicName,message,message);
        this.producer.send(producerRecord);
        log.info("===================================>>>>>>>>>>>     发送 message= " + message);
    }

    public void destroy(){
        this.producer.close();
    }
    /**
     * @return the brokers
     */
    public String getBrokers() {
        return brokers;
    }

    /**
     * @param brokers the brokers to set
     */
    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }

    /**
     * @return the required_acks
     */
    public String getRequired_acks() {
        return required_acks;
    }

    /**
     * @param required_acks the required_acks to set
     */
    public void setRequired_acks(String required_acks) {
        this.required_acks = required_acks;
    }

    /**
     * @return the request_timeout
     */
    public Long getRequest_timeout() {
        return request_timeout;
    }

    /**
     * @param request_timeout the request_timeout to set
     */
    public void setRequest_timeout(Long request_timeout) {
        this.request_timeout = request_timeout;
    }

    /**
     * @return the partitionNumber
     */
//    public Integer getPartitionNumber() {
//        return partitionNumber;
//    }

    /**
     * @param partitionNumber the partitionNumber to set
     */
//    public void setPartitionNumber(Integer partitionNumber) {
//        this.partitionNumber = partitionNumber;
//    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName(){
        return this.topicName;
    }

    public String getProducerType() {
        return producerType;
    }

    public void setProducerType(String producerType) {
        this.producerType = producerType;
    }

    public Producer<String, String> getProducer() {
        return producer;
    }

    public void setProducer(Producer<String, String> producer) {
        this.producer = producer;
    }

    public Integer getRetries() {
        return retries;
    }

    public void setRetries(Integer retries) {
        this.retries = retries;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Integer getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(Integer bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
    }
}
