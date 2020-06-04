package com.adb.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class MyProducerPartition {
    public static void main(String[] args) throws Exception {

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

        //添加自定义分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.adb.partition.MyPartition");

        Producer<String, String> producer = new KafkaProducer<>(props);
        //带回调的方法
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("adb", "kafka----adb+" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        System.out.println("topic: " + recordMetadata.topic() + ", partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
                    }else{
                        e.printStackTrace();
                    }
                }
            });
        }
        producer.close();
        System.out.println("执行完毕");
    }
}
