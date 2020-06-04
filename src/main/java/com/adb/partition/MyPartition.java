package com.adb.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * kafka自定义分区类
 */
public class MyPartition implements Partitioner{
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        /*自定义分区的实现方法
        可以参考该接口（Partitioner）默认的实现类"DefaultPartitioner"的partition方法来实现
        */
        //todo
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
