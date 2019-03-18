package com.zhiyou100.util;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class PhoenixTest {
    private Connection conn;
    private Statement stat;
    private ResultSet rs;

    @Before
    public void initResource() throws Exception {
        Class.forName(ConfigUtils.getDriver());
        conn = DriverManager.getConnection(ConfigUtils.getUrl(), ConfigUtils.getUserName(), ConfigUtils.getPassWord());
        stat = conn.createStatement();

    }

    //创建表
    @Test
    public void testCreateTable() throws SQLException {
        //String sql = "create table student1(id bigint not null primary key,name varchar(20),age tinyint)";

       String sql="create table if not exists guide_hospital_history(hospitalid varchar(64) \n" +
               " primary key,hcount integer,hcost decimal(10,2),hreimburse decimal(10,2),hrecovery integer,hday integer,\n" +
               "ocount integer,ocost decimal(10,2),oreimburse decimal(10,2),orecovery decimal(10,2),oday integer)";
        stat.executeUpdate(sql);
        conn.commit();
    }

    //查询表
    @Test
    public void selectTable() throws SQLException {
        String sql = "select * from student";
        ResultSet set = stat.executeQuery(sql);
        while (set.next()) {
            int id = set.getInt("id");
            String name = set.getString("name");
            int age = set.getInt("age");
            System.out.println("id:" + id+" name:"+name+"age"+age);
        }
    }

    //添加表数据
    @Test
    public void upsert() throws SQLException {
        for (int i = 1; i < 50; i++) {
            String sql="upsert into student values ("+i+", 'hanwen', 18)";
            stat.executeUpdate(sql);
            conn.commit();
           /* if (i%100000==0){
                conn.commit();
                System.out.println("插入"+i+"条");
            }*/
        }

    }

    //删除
    @Test
    public void delete() throws SQLException {
       // String sql1 = "delete from test_phoenix_api where mykey = 3";

      String sql1 = "drop table student";
        stat.executeUpdate(sql1);
        conn.commit();
    }


    @After
    public void closeResource() throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (stat != null) {
            stat.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}