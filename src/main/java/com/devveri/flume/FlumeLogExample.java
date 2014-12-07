package com.devveri.flume;

import org.apache.log4j.Logger;

/**
 * User: hilter
 * Date: 06/08/14
 * Time: 10:40
 */
public class FlumeLogExample {

    // http://www.thecloudavenue.com/2013/11/using-log4jflume-to-log-application.html
    public static void main(String[] args) {
        Logger logger = Logger.getLogger("flume");
        for (int i = 1; i <= 1000000; i++) {
            logger.info("this is my #" + i + " data");
        }
    }

}
