package com.adb.Interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String,String>{

    /**
     * 每收到一条消息都会调用一次
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String value = record.value();

        return new ProducerRecord(record.topic(),record.partition(),record.key(),
                System.currentTimeMillis()+"--"+value);
    }

    /**
     * 每发送一条消息，都会回调用一次该方法
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    /**
     *在主线程里面调用 producer.close()方法时，就会调用一次该方法
     */
    @Override
    public void close() {

    }

    /**
     * 启动时调用一次，读取配置文件用
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
