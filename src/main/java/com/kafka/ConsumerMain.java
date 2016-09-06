package com.kafka;


import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * Created by hjl on 2016/8/4.
 */
public class ConsumerMain {

    private static Logger log = Logger.getLogger(ConsumerMain.class.toString());

    public static void main(String[] args) {
        log.info("==============================>>>>>>>>>        consumer run");
        ApplicationContext app = new ClassPathXmlApplicationContext("classpath:test/spring-consumer-test.xml");
    }
}
