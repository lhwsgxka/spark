package com.zhiyou100;


import org.apache.spark.sql.types.Decimal;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;

public class PhoenixCURD {

    static Connection conn;
    static Statement stat;
    static ResultSet rs;

    static {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection("jdbc:phoenix:master:2181","root","root");
            stat = conn.createStatement();
            System.out.println("-----------init success------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void createTable() throws SQLException {
        String sql="create table indextest(id integer not null primary key ,a varchar,b varchar)";
        stat.executeUpdate(sql);
        conn.commit();
        close();
        System.out.println("---------------end---------------");
    }


    public static void insertTable() throws SQLException {
        String sql="upsert into javaapi(mykey,mycolumn)values(2,\'zhang\')";
        stat.executeUpdate(sql);
        conn.commit();
        close();
        System.out.println("--------end----------");
    }


    public static void insertBatch() throws SQLException {
        long time1 = System.currentTimeMillis();
        String sql = "upsert into indextest(id,a,b)values(?,?,?)";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        int num=0;
        for (int i = 1; i <=6000000; i++) {
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, "a"+i);
            preparedStatement.setString(3, "b"+i);
            preparedStatement.executeUpdate();
            if (i%10000==0) {
                conn.commit();
                num++;
                System.out.println(num*10000);
            }
        }
//        preparedStatement.executeUpdate();
        conn.commit();
        close();
        long time2 = System.currentTimeMillis();
        System.out.println(time2-time1);
        System.out.println("-----------------insert batch success---------------");
    }


    public static void deleteTab() throws SQLException {
        String sql = "delete from indextest where id>0";
        stat.executeUpdate(sql);
        conn.commit();
        close();
        System.out.println("---------delete success----------");
    }


    public static void dropTab() throws SQLException {
        String sql = "drop table javaapi";
        stat.executeUpdate(sql);
        conn.commit();
        close();
        System.out.println("---------DROP success----------");
    }



    public static void selectTable() throws SQLException {
        long time1 = System.currentTimeMillis();
        String sql = "select name from test where name='yu'";
        ResultSet resultSet = stat.executeQuery(sql);
        while (resultSet.next()) {
            String string = resultSet.getString(1);
//            String string1 = resultSet.getString(1);
            System.out.println(string);
        }
        long time2 = System.currentTimeMillis();
        System.out.println(time2-time1);

    }


    public static void updataTab() throws SQLException {
        String sql = "upsert into test values(2,'jack')";
        int i = stat.executeUpdate(sql);
        conn.commit();
        close();
        System.out.println(i);
        System.out.println("-----------updata success-----------");
    }



    public static void close() throws SQLException {
        if (stat!=null) {
            stat.close();
        }
        if (conn!=null) {
            conn.close();
        }
    }


    /*public static void createRecord() throws SQLException {
        String sql="create table record(id integer not null primary key ,recordid varchar,hospitalid varchar,diseaseid varchar,departmentid varchar,doctorid varchar,flag integer ,starttime varchar,endtime varchar,allcost decimal,isrecovery integer,reimbursetime varchar,recost decimal )";
        stat.executeUpdate(sql);
        conn.commit();
        close();
        System.out.println("---------------end---------------");
    }*/

    @Test
    public void createRecord() throws SQLException {
        String sql="create table hospital_analyze(hospitalid varchar primary key,hcount INTEGER,hcost decimal,hreimburse decimal,hrecovery INTEGER,hday INTEGER)";
        stat.executeUpdate(sql);
        conn.commit();
        close();
        System.out.println("---------------end---------------");
    }


    public static void insertRecord(String hospitalid,int hcount,BigDecimal hcost,BigDecimal hreimburse,int hrecovery,int hday) throws SQLException {
        String sql = "upsert into hospital_analyze(hospitalid,hcount,hcost,hreimburse,hrecovery,hday)values(?,?,?,?,?,?)";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.setString(1,hospitalid);
        preparedStatement.setInt(2,hcount);
        preparedStatement.setBigDecimal(3,hcost);
        preparedStatement.setBigDecimal(4,hreimburse);
        preparedStatement.setInt(5,hrecovery);
        preparedStatement.setInt(6,hday);
        /*preparedStatement.setString(1,"dsfsaewew");
        preparedStatement.setString(2,"cdsdfdc");
        preparedStatement.setBigDecimal(3,BigDecimal.valueOf(0.112));
        preparedStatement.setString(4,"2018-11-20");*/
        preparedStatement.executeUpdate();
//        conn.commit();
//        close();
        System.out.println("插入成功");

    }

    public static void commitAndClose() throws SQLException {
         conn.commit();
         close();
    }


}
