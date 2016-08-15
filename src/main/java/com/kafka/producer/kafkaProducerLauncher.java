package com.kafka.producer;

import com.kafka.producer.thread.ProducerThread;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by hjl on 2016/8/4.
 */
public class kafkaProducerLauncher implements ApplicationListener<ContextRefreshedEvent> {


    private ThreadPoolExecutor producerPool;

    private int producerNum;


    public void onApplicationEvent(ContextRefreshedEvent event) {
        ApplicationContext ac = event.getApplicationContext();
        //这里的条件保证启动 zk的连接和消费者线程的启动是在spring框架完成初始化以后
        if (ac.getParent() == null) {
                this.startProduce(ac);
        }
    }

    private void startProduce(ApplicationContext ac){
        for(int i=0; i<producerNum; i++){
            ProducerThread producerThread = (ProducerThread) ac.getBean("producerThread");
            producerThread.setMessage(""+System.nanoTime()+"你好produce");
            this.producerPool.submit(producerThread);
        }

    }

    public int getProducerNum() {
        return producerNum;
    }

    public void setProducerNum(int producerNum) {
        this.producerNum = producerNum;
    }

    public ThreadPoolExecutor getProducerPool() {
        return producerPool;
    }

    public void setProducerPool(ThreadPoolExecutor producerPool) {
        this.producerPool = producerPool;
    }
}
