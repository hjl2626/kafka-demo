package com.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;

/**
 * kafka消息生产者演示，
 * @author yinwenjie
 */

public class KafkaProduce {

    public static void main(String[] args) {
        Properties props = new Properties();
        // 指定kafka节点列表，不需要由zookeeper进行协调
        // 并且连接的目的也不是为了发送消息，而是为了在这些节点列表中选取一个，来获取topic的分区状况
        props.put("zk.connect", "192.168.109.131:2181");
        props.put("metadata.broker.list", "192.168.109.131:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 使用这个属性可以指定“将消息送到topic的哪一个partition中”，如果业务规则比较复杂的话可以指定分区控制器
        // 不过开发者最好要清楚topic有多少个分区，这样才好进行多线程（负载均衡）发送
        //props.put("partitioner.class", "kafkaTQ.PartitionerController");
        // 可以通过这个参数控制是异步发送还是同步发送（默认为“同步”）
        //props.put("producer.type", "async");
        // 可以通过这个属性控制复制过程的一致性规则
        //props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        // 创建消费者
        Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(config);

        // 由于我们为topic创建了四个partition，所以将数据分别发往这四个分区
        for (Integer partitionIndex = 0; ; partitionIndex++) {
            Date time = new Date();
            // 创建和发送消息，可以指定这条消息的key，producer根据这个key来决定这条消息发送到哪个parition中
            // 另外一个可以决定parition的方式是实现kafka.producer.Partitioner接口
            String messageContext_Value = "this message from producer 由producer指的partitionIndex：[" + partitionIndex % 2 + "]" + time.getTime();
            System.out.println(messageContext_Value);
            byte[] messageContext = messageContext_Value.getBytes();
            byte[] key = partitionIndex.toString().getBytes();

            // 这是消息对象，请注意第二个参数和第三个参数，下一小节将会进行详细介绍
            KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>("idea-test", key , partitionIndex % 2 ,  messageContext);
            producer.send(message);

            // 休息0.5秒钟，循环发
            synchronized (KafkaProduce.class) {
                try {
                    KafkaProduce.class.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace(System.out);
                }
            }
        }
    }


}