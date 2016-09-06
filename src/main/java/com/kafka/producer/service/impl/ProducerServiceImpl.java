package com.kafka.producer.service.impl;

import com.kafka.producer.service.ProducerSevice;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by hjl on 2016/8/4.
 */

public class ProducerServiceImpl implements ProducerSevice {


    private static Logger log = Logger.getLogger(ProducerServiceImpl.class);

    /**
     * kafka的brokers列表
     */
    private String brokers;

    /**
     * acks的值，只能有三种-1、0还有all
     */
    private String required_acks;

    /**
     * topic
     */
    private String topicName;

    /**
     * 重试次数
     */
    private Integer retries;

    /**
     * batchSize
     */
    private Integer batchSize;

    /**
     * 缓存容量 bytes
     */
    private Integer bufferMemory;

    /**
     * lingerMs
     */
    private Integer lingerMs;

    /**
     * keySerializer
     */
    private String keySerializer;

    /**
     * valueSerializer
     */
    private String valueSerializer;

    /**
     * kafka主服务对象
     */
    private Producer<String, String> producer;

    /* (non-Javadoc)
     * @see test.interrupter.producer.ProducerServiceImpl#init()
     */
    public void init() {
        // 验证所有必要属性都已设置

        log.info("+++++++++++++++++++++++>>>>>>>             ProducerServiceImpl  init");

        if (StringUtils.isEmpty(this.brokers)) {
            throw new RuntimeException("至少需要指定一个broker的位置");
        }

        if (!this.required_acks.equals("0") && !this.required_acks.equals("1")
                && !this.required_acks.equals("all")) {
            throw new RuntimeException("错误的required_acks值 ");
        }

        if (StringUtils.isEmpty(this.topicName)) {
            throw new RuntimeException("topicName 不能为空  ");
        }

        if (StringUtils.isEmpty(this.valueSerializer)) {
            throw new RuntimeException("value.serializer 不能为空  ");
        }

        if (StringUtils.isEmpty(this.keySerializer)) {
            throw new RuntimeException("key.serializer 不能为空  ");
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", this.brokers);
        props.put("acks", this.required_acks); //ack方式，all，会等所有的commit最慢的方式

        if (StringUtils.isNotEmpty(this.retries.toString())) {
            props.put("retries", this.retries); //失败是否重试，设置会有可能产生重复数据
        }

        if (null != this.batchSize) {
            props.put("batch.size", this.batchSize); //对于每个partition的batch buffer大小
        }

        if (null != this.lingerMs) {
            props.put("linger.ms", this.lingerMs);  //等多久，如果buffer没满，比如设为1，即消息发送会多1ms的延迟，如果buffer没满
        }

        if (null != this.bufferMemory) {
            props.put("buffer.memory", this.bufferMemory); //整个producer可以用于buffer的内存大小
        }

        props.put("key.serializer", this.keySerializer);
        props.put("value.serializer", this.valueSerializer);
        this.producer = new KafkaProducer<String, String>(props);

    }

    /* (non-Javadoc)
     * @see test.interrupter.producer.ProducerServiceImpl#senderMessage(java.lang.String)
     */
    public Future<RecordMetadata> sendMessage(int partition, String key, String value) {
        // 创建和发送消息
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(this.topicName, partition, key, value);
        log.info("+++++++++++++++++++++++++++++>>>>>> " + Thread.currentThread().getName() + "   发送 message= " + value);
        return this.producer.send(producerRecord);

    }

    public Future<RecordMetadata> sendMessage(int partition, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(this.topicName, partition, value, value);
        log.info("+++++++++++++++++++++++++++++>>>>>> " + Thread.currentThread().getName() + "   发送 message= " + value);
        return this.producer.send(producerRecord);

    }

    public Future<RecordMetadata> sendMessage(String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(this.topicName, key, value);
        log.info("+++++++++++++++++++++++++++++>>>>>> " + Thread.currentThread().getName() + "   发送 message= " + value);
        return this.producer.send(producerRecord);

    }

    public Future<RecordMetadata> sendMessage(String value) {
        // 创建和发送消息
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(this.topicName, value, value);
        log.info("+++++++++++++++++++++++++++++>>>>>> " + Thread.currentThread().getName() + "   发送 message= " + value);
        return this.producer.send(producerRecord);

    }

    public void destroy() {
        this.producer.close();
    }

    public String getRequired_acks() {
        return required_acks;
    }

    public ProducerServiceImpl setRequired_acks(String required_acks) {
        this.required_acks = required_acks;
        return this;
    }

    public String getBrokers() {
        return brokers;
    }

    public ProducerServiceImpl setBrokers(String brokers) {
        this.brokers = brokers;
        return this;
    }

    public String getTopicName() {
        return topicName;
    }

    public ProducerServiceImpl setTopicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public Integer getRetries() {
        return retries;
    }

    public ProducerServiceImpl setRetries(Integer retries) {
        this.retries = retries;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public ProducerServiceImpl setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Integer getLingerMs() {
        return lingerMs;
    }

    public ProducerServiceImpl setLingerMs(Integer lingerMs) {
        this.lingerMs = lingerMs;
        return this;
    }

    public Integer getBufferMemory() {
        return bufferMemory;
    }

    public ProducerServiceImpl setBufferMemory(Integer bufferMemory) {
        this.bufferMemory = bufferMemory;
        return this;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public ProducerServiceImpl setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public ProducerServiceImpl setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public Producer<String, String> getProducer() {
        return producer;
    }

    public ProducerServiceImpl setProducer(Producer<String, String> producer) {
        this.producer = producer;
        return this;
    }
}
