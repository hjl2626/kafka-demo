package com;

import scala.io.BytePickle;

import java.util.regex.Pattern;

/**
 * Created by hjl on 2016/9/2.
 */
public class Test {

    @org.junit.Test
    public void test(){
        String str = "[\\d-\\:,\\ ]*(\\w+?)\\ \\[.*";

        Pattern pattern = Pattern.compile(str);
        System.out.print(pattern.matcher(""));
    }
}
