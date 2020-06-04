package com.adb.Interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CountInterceptor implements ProducerInterceptor<String,String> {

    //成功发送个数
    private AtomicInteger success=new AtomicInteger();

    //失败个数
    private AtomicInteger error=new AtomicInteger();

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(recordMetadata!=null){
            success.addAndGet(1);
        }else{
            error.addAndGet(1);
        }
    }

    @Override
    public void close() {
        System.out.println("success:"+success.get());
        System.out.println("error:"+error.get());
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
