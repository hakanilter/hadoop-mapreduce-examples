package com.devveri.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * User: hilter
 * Date: 21/02/14
 * Time: 16:53
 */
public class HBaseScanExample {

    private static final String ZOOKEEPER_QUORUM = "zkhost1,zkhost2,zkhost3";
    private static final String ZOOKEEPER_PORT = "2181";

    public void test() throws IOException {
        // configuration
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);

        // connection
        HConnection connection = null;
        HTableInterface table = null;
        try {
            connection = HConnectionManager.createConnection(config);
            table = connection.getTable("test");

            byte[] family = Bytes.toBytes("c");
            byte[] qualifier = Bytes.toBytes("name");

            Scan scan = new Scan();
            scan.setBatch(1000);
            scan.setCaching(100);
            scan.addColumn(family, qualifier);
            ResultScanner rs = table.getScanner(scan);
            for (Result r = rs.next(); r != null; r = rs.next()) {
                byte[] valueObj = r.getValue(family, qualifier);
                String value = new String(valueObj);
                System.out.println(value);
            }
        } finally {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        new HBaseScanExample().test();
    }

}