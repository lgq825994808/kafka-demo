package com.adb.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        //String[] servers = new String[]{"192.168.133.131:9092", "192.168.133.132:9092", "192.168.133.133:9092"};
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.133.131:9092");
        //开启自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        //自动提交的延时（毫秒）
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");

        //设置手动提交offset
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        //设置offset的重置方式（配合主题，可以设置该消费者组重新消费该主题）
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //设置订阅组
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"bigdata");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        //设置订阅主题
        kafkaConsumer.subscribe(Arrays.asList("adb"));

        while (true){
            //获取消息,并设置拉取时间间隔（毫秒）
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                System.out.println("consumer --- key:"+consumerRecord.key()+",-----value:"+consumerRecord.value());
            }
            //手动提交offset里面的同步提交
            //kafkaConsumer.commitSync();
            //手动提交offset里面的异步提交
            /*kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if(e != null){
                        //做提交失败后的逻辑
                        System.out.println("commit fail :"+map);
                    }
                }
            });*/

        }
    }
}
