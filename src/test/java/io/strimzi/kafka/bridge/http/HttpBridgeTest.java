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

import io.strimzi.kafka.bridge.KafkaClusterTestBase;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@RunWith(VertxUnitRunner.class)
public class HttpBridgeTest extends KafkaClusterTestBase {

    private static final Logger log = LoggerFactory.getLogger(HttpBridgeTest.class);

    private static final String BRIDGE_HOST = "0.0.0.0";
    private static final int BRIDGE_PORT = 8080;

    private Vertx vertx;
    private HttpBridge httpBridge;

    private HttpBridgeConfigProperties bridgeConfigProperties = new HttpBridgeConfigProperties();

    @Before
    public void before(TestContext context) {

        vertx = Vertx.vertx();

        this.httpBridge = new HttpBridge();
        this.httpBridge.setBridgeConfigProperties(this.bridgeConfigProperties);

        this.vertx.deployVerticle(this.httpBridge, context.asyncAssertSuccess());
    }

    @After
    public void after(TestContext context) {

        this.vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void sendSimpleMessage(TestContext context) {
        String topic = "mytopic";
        kafkaCluster.createTopic(topic, 1, 1);

        Async async = context.async();

        String value = "Hi, This is kafka bridge";

        JsonObject json = new JsonObject();
        json.put("value", value);

        HttpClient client = vertx.createHttpClient();

        client.post(BRIDGE_PORT, BRIDGE_HOST, "/producer/"+topic, response ->{
            response.bodyHandler(buffer -> {
                String deliveryStatus = buffer.toJsonObject().getString("status");
                context.assertEquals("Accepted", deliveryStatus);
            });
        }).putHeader("Content-length", String.valueOf(json.toBuffer().length())).write(json.toBuffer()).end();

        Properties config = kafkaCluster.useTo().getConsumerProperties("groupId", null, OffsetResetStrategy.EARLIEST);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(this.vertx, config);
        consumer.handler(record -> {
            context.assertEquals(record.value(), value);
            log.info("Message consumed topic={} partition={} offset={}, key={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            consumer.close();
            async.complete();
        });

        consumer.subscribe(topic, done -> {
            if (!done.succeeded()) {
                context.fail(done.cause());
            }
        });
    }
}
