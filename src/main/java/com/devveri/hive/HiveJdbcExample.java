package com.devveri.hive;

import java.sql.*;

/**
 * User: hilter
 * Date: 21/02/14
 * Time: 13:49
 */
public class HiveJdbcExample {

    private static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String JDBC_URL = "jdbc:hive2://myhost:10000/test";
    private static final String JDBC_USER = "";
    private static final String JDBC_PASS = "";

    public void test() throws ClassNotFoundException, SQLException {
        // load class
        Class.forName(JDBC_DRIVER);

        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
            stmt = con.createStatement();
            rs = stmt.executeQuery("select * from test.nyse limit 10");
            while (rs.next()) {
                System.out.println(String.format("%s\t%s\t%f",
                        rs.getString("exchange"),
                        rs.getString("date"),
                        rs.getDouble("stock_price_open")));
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                con.close();
            }
        }
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        new HiveJdbcExample().test();
    }

}
