package io.strimzi.kafka.bridge.example;

import io.strimzi.kafka.bridge.config.KafkaConfigProperties;
import io.strimzi.kafka.bridge.http.HttpBridgeConfigProperties;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.Properties;

public class HttpBridgeReceiver {

    private Vertx vertx;

    private HttpBridgeConfigProperties bridgeConfigProperties;

    private KafkaConsumer<String , byte[]> consumer;

    private HttpServer httpServer;

    private String topic;

    private String groupId;

    private HttpServerResponse httpServerResponse;

    private int fetchCount;

    //private long lastRecordOffset;

    private int messageCount = 0;

    private StringBuffer response;

    private int totalResponseLength=0;

    private ArrayList<KafkaConsumerRecord<String, byte[]>> records;

    public static void main(String[] args) {

        HttpBridgeReceiver httpBridgeReceiver = new HttpBridgeReceiver();
        httpBridgeReceiver.vertx = Vertx.vertx();
        httpBridgeReceiver.bridgeConfigProperties= new HttpBridgeConfigProperties();

        httpBridgeReceiver.httpServer = httpBridgeReceiver.vertx.createHttpServer(httpBridgeReceiver.configureServer());

        httpBridgeReceiver.httpServer.requestHandler(request -> {
            httpBridgeReceiver.topic = request.getHeader("topic");
            httpBridgeReceiver.groupId = request.getHeader("consumerid");
            httpBridgeReceiver.fetchCount = Integer.parseInt(request.getHeader("fetchcount"));
            httpBridgeReceiver.initConsumer();
            httpBridgeReceiver.httpServerResponse = request.response();
            httpBridgeReceiver.receiveMessagesFromTopic(httpBridgeReceiver.topic);
        }).listen(httpServerAsyncResult -> {
            if (httpServerAsyncResult.succeeded()){
                System.out.println("running at--> "+ httpBridgeReceiver.httpServer.actualPort());
            }
            else{
                System.out.println("failed to start server");
            }
        });
    }


    private void initConsumer(){
        KafkaConfigProperties consumerConfig = this.bridgeConfigProperties.getKafkaConfigProperties();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerConfig.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, consumerConfig.getConsumerConfig().getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, consumerConfig.getConsumerConfig().getValueDeserializer());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumerConfig.getConsumerConfig().isEnableAutoCommit());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerConfig.getConsumerConfig().getAutoOffsetReset());
        this.consumer = KafkaConsumer.create(this.vertx, props);
    }

    private HttpServerOptions configureServer(){
        HttpServerOptions serverOptions = new HttpServerOptions();
        serverOptions.setPort(this.bridgeConfigProperties.getEndpointConfigProperties().getPort());
        return serverOptions;
    }

    private void receiveMessagesFromTopic(String topic){
        consumer.subscribe(topic);
        records = new ArrayList<>();
        response = new StringBuffer();
        consumer.handler(kafkaConsumerRecord -> {
            records.add(kafkaConsumerRecord);
            messageCount++;
            if (messageCount==fetchCount){
                consumer.pause();
                for(KafkaConsumerRecord<String, byte[]> kafkaRecords : records){
                    response.append(new String(kafkaRecords.value()));
                    response.append("\n");
                }
                sendHttpResponse(response);
                records.clear();
                consumer.close();
                messageCount = 0;
            }
        });

    }

    private void sendHttpResponse(StringBuffer response){
        this.httpServerResponse.putHeader("Content-length",String .valueOf(response.length()));
        this.httpServerResponse.write(response.toString());
    }
}
