package com.kafka.util;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggerFactory;

/**
 * Created by hjl on 2016/8/4.
 */
public class LogUtil {
    static {
        PropertyConfigurator.configure("log4j.properties");
    }

    public static Logger getLogger(String clazz){
//        return L/ogger.
        return Logger.getLogger(clazz);
    }
}
