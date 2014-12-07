package com.devveri.hive;

import java.sql.*;

/**
 * User: hilter
 * Date: 07/12/14
 * Time: 16:46
 */
public class PrestoJdbcExample {

    private static final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    private static final String JDBC_URL = "jdbc:presto://localhost:8080/hive/test";
    private static final String JDBC_USER = "test";
    private static final String JDBC_PASS = "test";

    public void test() throws ClassNotFoundException, SQLException {
        // load class
        Class.forName(JDBC_DRIVER);

        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
            stmt = con.createStatement();
            rs = stmt.executeQuery("select * from test.nyse limit 20");
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
        new PrestoJdbcExample().test();
    }

}
