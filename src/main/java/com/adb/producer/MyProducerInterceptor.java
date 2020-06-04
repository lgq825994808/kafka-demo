package com.adb.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

public class MyProducerInterceptor {
    public static void main(String[] args) {
        Properties props = new Properties();

        //String[] servers = new String[]{"192.168.133.131:9092", "192.168.133.132:9092", "192.168.133.133:9092"};
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.133.131:9092");
        //ack级别  0:直接返回  1：等待leader写完成，在返回   -1（"all"）：等待leader和所有follower写入完成，在返回
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //批次大小 16kb
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //缓冲区大小  32M
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);


        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //添加拦截器
        ArrayList<String> arrayList = new ArrayList<>();
        arrayList.add("com.adb.Interceptor.TimeInterceptor");
        arrayList.add("com.adb.Interceptor.CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,arrayList);

        Producer<String, String> producer = new KafkaProducer<>(props);
        //直接发送
        for (int i = 0; i < 7; i++) {
            producer.send(new ProducerRecord("adb", "kafka----" + i));
            //System.out.println("topic:adb,value:kafka----"+i);
        }

        producer.close();
        System.out.println("执行完毕");
    }

}
