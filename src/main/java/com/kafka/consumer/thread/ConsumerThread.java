package com.kafka.consumer.thread;


import com.mongo.MongoDBUtil;
import com.mongodb.client.MongoCollection;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 消息消费线程
 *
 * @author yinwenjie
 */


public class ConsumerThread implements Runnable {


    private  MongoCollection<Document> coll = MongoDBUtil.instance.getCollection("mongodb-test", "logs");

    private static AtomicLong index = new AtomicLong();


    private static Logger log = Logger.getLogger(ConsumerThread.class);

    private KafkaStream<byte[], byte[]> stream;

    public ConsumerThread(KafkaStream<byte[], byte[]> stream) {
        this.stream = stream;
    }

    public ConsumerThread() {
    }

    /**
     * 日志
     */

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    public void run() {

        log.info("=================================   " + Thread.currentThread().getName() + "  run");
        ConsumerIterator<byte[], byte[]> iterator = this.stream.iterator();
        /*
         * 这个消费者获取的数据在这里
         * 注意进行异常的捕获：
         * 如果有异常抛出但是又没有在方法中进行捕获，就会导致线程执行终止
         * */
        while (iterator.hasNext()) {
            MessageAndMetadata<byte[], byte[]> message = iterator.next();
            int partition = message.partition();
            String topic = message.topic();

            String messageT = new String(message.message());
            log.info(Thread.currentThread().getName() + "  接收到: " + messageT + "来自于topic：[" + topic + "] + 第partition[  " + partition + "  ]" + "   key = " + message.key());
                Document bson = new Document();
                bson.put("id", ConsumerThread.index.getAndIncrement());
                bson.put("content", messageT);
                coll.insertOne(bson);
            /*
             * 这里需要选择一种 "合适的存储方案"
             * */
        }
    }

    /**
     * @param stream the stream to set
     */
    public void setStream(KafkaStream<byte[], byte[]> stream) {
        this.stream = stream;
    }
}