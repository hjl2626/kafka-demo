package com.kafka.consumer.thread;


import com.mongo.MongoDBUtil;
import com.mongodb.client.MongoCollection;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 消息消费线程
 *
 * @author yinwenjie
 */


public class ConsumerThread implements Runnable {

    private static Logger log = Logger.getLogger(ConsumerThread.class);
    /**
     * 数据库名称
     */
    private String dbName;

    /**
     * 集合名称
     */
    private String collName;

    /**
     *
     */
    private MongoCollection<Document> coll;

    /**
     * 序号
     */
    private static AtomicLong index;

    private KafkaStream<byte[], byte[]> stream;

    private Document bson;

    public void init() {

        bson = new Document();

        index = new AtomicLong();

        if (StringUtils.isEmpty(dbName)) {
            throw new RuntimeException("数据库名不能为空 ！");
        }

        if (StringUtils.isEmpty(collName)) {
            throw new RuntimeException("集合名不能为空 ！");
        }

        this.coll = MongoDBUtil.instance.getCollection(this.dbName, this.collName);
    }

    public ConsumerThread(KafkaStream<byte[], byte[]> stream) {
        this.stream = stream;
    }

    public ConsumerThread() {
    }


    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    public void run() {

        log.info("=================================   " + Thread.currentThread().getName() + "  run");

        try {
            ConsumerIterator<byte[], byte[]> iterator = this.stream.iterator();
            /*
             * 这个消费者获取的数据在这里
             * 注意进行异常的捕获：
             * 如果有异常抛出但是又没有在方法中进行捕获，就会导致线程执行终止
             * */
//            Document bson = new Document();
            while (iterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> message = iterator.next();
                int partition = message.partition();
                String topic = message.topic();

                String messageT = new String(message.message());
                log.info(Thread.currentThread().getName() + "  接收到: " + messageT + "来自于topic：[" + topic + "] + 第partition[  " + partition + "  ]" + "   offset = " + message.offset());
                bson.clear();
                bson.put("id", ConsumerThread.index.getAndIncrement());
                bson.put("content", messageT);
                coll.insertOne(bson);
                /*
                 * 这里需要选择一种 "合适的存储方案"
                 * */
            }
        } catch (Exception e) {
            log.info(e);
        }
    }

    /**
     * @param stream the stream to set
     */
    public ConsumerThread setStream(KafkaStream<byte[], byte[]> stream) {
        this.stream = stream;
        return this;
    }

    public String getCollName() {
        return collName;
    }

    public ConsumerThread setCollName(String collName) {
        this.collName = collName;
        return this;
    }

    public String getDbName() {
        return dbName;
    }

    public ConsumerThread setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }
}