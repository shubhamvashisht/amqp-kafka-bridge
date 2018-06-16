/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpJsonMessageConverter;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;

public class HttpSourceBridgeEndpoint extends SourceBridgeEndpoint {
    private MessageConverter messageConverter;

    public HttpSourceBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties bridgeConfigProperties) {
        super(vertx, bridgeConfigProperties);
    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        HttpServerRequest httpServerRequest = (HttpServerRequest) endpoint.get();

        messageConverter = new HttpJsonMessageConverter();

        //split path to extract params
        String[] params = httpServerRequest.path().split("/");

        //path is like this : /producer/{topic_name}, topic will be at the last position of param[]
        String topic = params[params.length - 1];


        httpServerRequest.bodyHandler(buffer -> {
            KafkaProducerRecord<String , byte[]> kafkaProducerRecord = messageConverter.toKafkaRecord(topic, buffer);

            this.send(kafkaProducerRecord, writeResult -> {
                if (writeResult.failed()) {

                    Throwable exception = writeResult.cause();
                    log.error("Error on delivery to Kafka {}", exception.getMessage());
                    this.sendDeliveryStatus("Failed", httpServerRequest.response());

                } else {

                    RecordMetadata metadata = writeResult.result();
                    log.debug("Delivered to Kafka on topic {} at partition {} [{}]", metadata.getTopic(), metadata.getPartition(), metadata.getOffset());
                    this.sendDeliveryStatus("Accepted", httpServerRequest.response());

                }
            });

        });

    }

    private void sendDeliveryStatus(String status, HttpServerResponse response){
        JsonObject jsonResponse = new JsonObject();
        jsonResponse.put("status", status);
        response.putHeader("Content-length", String.valueOf(jsonResponse.toBuffer().length()));
        response.write(jsonResponse.toBuffer());
        response.end();
    }
}
