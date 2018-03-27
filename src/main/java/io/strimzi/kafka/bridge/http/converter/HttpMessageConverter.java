package io.strimzi.kafka.bridge.http.converter;

import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.HttpMessage;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;

public class HttpMessageConverter implements MessageConverter<String, byte[], HttpMessage> {

    @Override
    public KafkaProducerRecord<String, byte[]> toKafkaRecord(String kafkaTopic, HttpMessage message) {
        return new KafkaProducerRecordImpl<>(message.getTopic(),message.getKey(),message.getValue(),message.getPartition());
    }

    @Override
    public HttpMessage toMessage(String address, KafkaConsumerRecord<String, byte[]> record) {
        return new HttpMessage(record.partition(),record.topic(),record.offset(),record.key(),record.value());
    }

}
