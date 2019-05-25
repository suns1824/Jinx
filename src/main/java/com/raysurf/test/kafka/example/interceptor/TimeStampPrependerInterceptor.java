package com.raysurf.test.kafka.example.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeStampPrependerInterceptor implements ProducerInterceptor<String, String> {
    /*
    这个方法在KafkaProducer.send中被使用到，表明其运行在主线程中。producer确保在消息被序列化之前调用该方法，用户可以在这里对消息做任何操作，但最好不要
    修改消息所属的topic和分区有关的信息，否则会影响目标分区的计算。
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //将时间戳信息加到消息value的最前部
        return new ProducerRecord(record.topic(), record.partition(), record.timestamp(), record.key(), System.currentTimeMillis() + "," + record.value().toString());
    }

    /*
    该方法运行在producer的I/O线程，在消息被应答之前或者消息发送失败时调用。
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    //关闭interceptor,做资源清理相关工作
    @Override
    public void close() {

    }
    //实现Configurable接口
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
