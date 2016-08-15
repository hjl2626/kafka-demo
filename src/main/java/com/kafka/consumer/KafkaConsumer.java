package com.kafka.consumer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.springframework.stereotype.Component;

/**
 * 这是Kafka的topic消费者
 * @author yinwenjie
 */


public class KafkaConsumer {

    public  static void main(String[] args) {
        // ==============首先各种连接属性
        // Kafka消费者的完整连接属性在Apache Kafka官网http://kafka.apache.org/documentation.html#consumerconfigs
        // 有详细介绍（请参看Old Consumer Configs。New Consumer Configs是给Kafka V0.9.0.0+使用的）
        // 这里我们设置几个关键属性
        Properties props = new Properties();
        // zookeeper相关的，如果有多个zk节点，这里以“,”进行分割
        props.put("zookeeper.connect", "192.168.109.130:2181/kafka-1");
        props.put("zookeeper.connection.timeout.ms", "10000");
        // 还记得上文的说明吗：对于一个topic而言，同一用户组内的所有用户只被允许访问一个分区。
        // 所以要让多个Consumer实现对一个topic的负载均衡，每个groupid的名称都要一样
        String groupname = "group1";
        props.put("group.id", groupname);

        //==============
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        // 我们只创建一个消费者
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put("ubuntu-test", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = consumerConnector.createMessageStreams(map);

        // 获取并启动消费线程，注意看关键就在这里，一个消费线程可以负责消费一个topic中的多个partition
        // 但是一个partition只能分配到一个消费线程去
        KafkaStream<byte[], byte[]> stream = topicMessageStreams.get("ubuntu-test").get(0);
        new Thread(new ConsumerThread(stream)).start();

        // 接着锁住主线程，让其不退出
        synchronized (KafkaConsumer.class) {
            try {
                KafkaConsumer.class.wait();
            } catch (InterruptedException e) {
                e.printStackTrace(System.out);
            }
        }
    }

    /**
     * @author yinwenjie
     */
    private static class ConsumerThread implements Runnable {

        private KafkaStream<byte[], byte[]> stream;

        /**
         * @param stream
         */
        public ConsumerThread(KafkaStream<byte[], byte[]> stream) {
            this.stream = stream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> iterator =  this.stream.iterator();
            //============这个消费者获取的数据在这里
            while(iterator.hasNext()){
                MessageAndMetadata<byte[], byte[]> message = iterator.next();
                int partition = message.partition();
                String topic = message.topic();
                String messageT = new String(message.message());
                System.out.println("接收到: " + messageT + "来自于topic：[" + topic + "] + 第partition[" + partition + "]");
            }
        }
    }
}