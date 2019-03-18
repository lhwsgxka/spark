package com.zhiyou100;

import org.junit.Test;

/**
 * 数据生产工厂测试类
 * Created by zhangxin on 2016/12/1.
 */
public class MedicalDataFactoryTest {

    @Test
    public void createMedicalDataTest(){
        MedicalDataFactory medicalDataFactory = new MedicalDataFactory();
        medicalDataFactory.createMedicalData(100); //要创造的数据量
    }

}
