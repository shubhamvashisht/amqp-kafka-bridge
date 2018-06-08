package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class HttpDefaultMessageConverter implements MessageConverter<String,byte[],HttpMessage> {

    @Override
    public KafkaProducerRecord<String, byte[]> toKafkaRecord(String kafkaTopic, HttpMessage message) {
        KafkaProducerRecord<String, byte[]> record = KafkaProducerRecord.create(message.getTopic(),
                message.getKey(), message.getValue(), message.getTimeStamp(), message.getPartition());

        return record;
    }

    @Override
    public HttpMessage toMessage(String address, KafkaConsumerRecord<String, byte[]> record) {
        return null;
    }
}
