package com.kafka.consumer;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

import com.kafka.consumer.thread.ConsumerThread;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 这是Kafka的topic消费者
 *
 * @author yinwenjie
 */
public class KafkaConsumerLauncher implements ApplicationListener<ContextRefreshedEvent> {


    private static Logger log = Logger.getLogger(KafkaConsumerLauncher.class);
    /**
     * zookeeper连接地址串
     */
    private String zookeeper_connects;

    /**
     * zookeeper连接超时事件
     */
    private Long zookeeper_timeout;

    /**
     * 分区数量
     */
    private Integer consumerNumber;

    /**
     * 消息消费者处理线程池。
     * 每一个消费者都是线程池中的一个线程<br>
     * 且线程池中线程数量就是分区数量
     */
    private ThreadPoolExecutor consumerPool;



    private String topicName;

    private String groupName;

    /* (non-Javadoc)
     * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
     */
    public void onApplicationEvent(ContextRefreshedEvent event) {
        ApplicationContext ac = event.getApplicationContext();
        //这里的条件保证启动 zk的连接和消费者线程的启动是在spring框架完成初始化以后
        if (ac.getParent() == null) {
            this.startConsumerStream(ac);
        }
    }

    /**
     * 开启消费者线程
     *
     * @param context
     */
    public void startConsumerStream(ApplicationContext context) {


        // ==============首先各种连接属性
        Properties props = new Properties();
        props.put("zookeeper.connect", this.zookeeper_connects);
//        List list = new ArrayList();
//        list.add("192.168.109.130:9092");
//        list.add("192.168.109.135:9092");
//        list.add("192.168.109.136:9092");
//
//        props.put("bootstrap.servers", list);
//        props.put("auto.offset.reset", "largest");
        props.put("auto.offset.reset", "largest");
        props.put("zookeeper.connection.timeout.ms", this.zookeeper_timeout.toString());
        props.put("group.id", this.groupName);

        //==============
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        HashMap<String, Integer> map = new HashMap<String, Integer>();
        String topicName = this.topicName;
        map.put(topicName, this.consumerNumber);
        Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = consumerConnector.createMessageStreams(map);

        // 获取并启动消费线程，注意看关键就在这里，一个消费线程可以负责消费一个topic中的多个partition
        // 但是一个partition只能分配到一个消费者线程
        List<KafkaStream<byte[], byte[]>> streamList = topicMessageStreams.get(topicName);

        // 为每一个消费者创建一个处理线程。并放置到线程池中运行
        // 注意：并不需要监控这些消费线程的运行状态，
        // 因为没有消息接收的时候，线程就自然会在"iterator.hasNext()"位置等待
        for (int index = 0; index < streamList.size(); index++) {
            KafkaStream<byte[], byte[]> stream = streamList.get(index);
            ConsumerThread consumerThread = (ConsumerThread) context.getBean("consumerThread");
            consumerThread.setStream(stream);
            this.consumerPool.submit(consumerThread);
        }
    }

    /**
     * @return the zookeeper_connects
     */
    public String getZookeeper_connects() {
        return zookeeper_connects;
    }

    /**
     * @param zookeeper_connects the zookeeper_connects to set
     */
    public void setZookeeper_connects(String zookeeper_connects) {
        this.zookeeper_connects = zookeeper_connects;
    }

    /**
     * @return the zookeeper_timeout
     */
    public Long getZookeeper_timeout() {
        return zookeeper_timeout;
    }

    /**
     * @param zookeeper_timeout the zookeeper_timeout to set
     */
    public void setZookeeper_timeout(Long zookeeper_timeout) {
        this.zookeeper_timeout = zookeeper_timeout;
    }

    /**
     * @return the consumerNumber
     */
    public Integer getConsumerNumber() {
        return consumerNumber;
    }

    /**
     * @param consumerNumber the consumerNumber to set
     */
    public void setConsumerNumber(Integer consumerNumber) {
        this.consumerNumber = consumerNumber;
    }

    /**
     * @return the consumerPool
     */
    public ThreadPoolExecutor getConsumerPool() {
        return consumerPool;
    }

    /**
     * @param consumerPool the consumerPool to set
     */
    public void setConsumerPool(ThreadPoolExecutor consumerPool) {
        this.consumerPool = consumerPool;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }
}