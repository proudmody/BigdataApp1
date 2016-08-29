package com.triman.bigdata.util; /**
 * Created by mdd on 2016/3/3.
 */

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class Hive {
        private static String driverName =
                "org.apache.hive.jdbc.HiveDriver";

        public static void main(String[] args)
                throws SQLException {
            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }

            Connection con = DriverManager.getConnection(
                    "jdbc:hive2://192.168.3.2:10000/default", "", "");//用户名 密码默认为空
            Statement stmt = con.createStatement();
            String tableName = "hx_a_ajjbqk";
//            stmt.execute("drop table if exists " + tableName);
//            stmt.execute("create table " + tableName +
//                    " (key int, value string)");
//            System.out.println("Create table success!");
            // show tables
            String sql = "show tables '" + tableName + "'";
            System.out.println("Running: " + sql);
            ResultSet res = stmt.executeQuery(sql);
            if (res.next()) {
                System.out.println(res.getString(1));
            }

            // describe table
            sql = "describe " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }


            sql = "select * from " + tableName+" limit 5";
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(String.valueOf(res.getString(1)) + "\t"
                        + res.getString(2));
            }

//            sql = "select count(1) from " + tableName+" limit 5";
//            System.out.println("Running: " + sql);
//            res = stmt.executeQuery(sql);
//            while (res.next()) {
//                System.out.println(res.getString(1));
//            }
        }
}
