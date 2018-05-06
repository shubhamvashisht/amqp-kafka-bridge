package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.http.converter.HttpMessageConverter;
import io.strimzi.kafka.bridge.http.extractors.GetRequestExtractor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.util.Optional;
import java.util.Set;

public class HttpSinkBridgeEndpoint<K,V> extends SinkBridgeEndpoint<K,V>{

    private HttpServerRequest httpServerRequest;

    private HttpConnection httpConnection;

    private HttpMessage httpMessage;

    private HttpMessageConverter httpMessageConverter;

    private HttpServerResponse httpServerResponse;

    public HttpSinkBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties bridgeConfigProperties){
        super(vertx, bridgeConfigProperties);
    }
    @Override
    public void open() {

    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        this.httpServerRequest = (HttpServerRequest) endpoint.get();

        this.httpServerResponse = httpServerRequest.response();

        httpMessage = new GetRequestExtractor().getHttpMessage(this.httpServerRequest);

        this.kafkaTopic = httpMessage.getTopic();

        this.groupId = httpMessage.getGroup_id();

        this.initConsumer();

        this.subscribe();

        this.setPartitionsRevokedHandler(this::partitionsRevokedHandler);
        this.setPartitionsAssignedHandler(this::partitionsAssignedHandler);
        this.setSubscribeHandler(this::subscribeHandler);
        this.setPartitionHandler(this::partitionHandler);
        this.setAssignHandler(this::assignHandler);
        this.setSeekHandler(this::seekHandler);
        this.setReceivedHandler(this::sendHttpResponse);
        this.setCommitHandler(this::commitHandler);

    }

    private void partitionsRevokedHandler(Set<TopicPartition> partitions){
        System.out.println("revoked"+"1");
    }

    private void partitionsAssignedHandler(Set<TopicPartition> partitions){
        System.out.println("revoked"+"2");
    }

    private void subscribeHandler(AsyncResult<Void> subscribeResult){
        System.out.println("revoked"+"3");
    }

    private void partitionHandler(AsyncResult<Optional<PartitionInfo>> partitionResult){
        System.out.println("revoked"+"4");
    }

    private void assignHandler(AsyncResult<Void> assignResult){
        System.out.println("revoked"+"5");
    }

    private void seekHandler(AsyncResult<Void> seekResult){
        System.out.println("revoked"+"6");
    }

    private void sendHttpResponse(KafkaConsumerRecord<K, V> record){
        System.out.println("revoked"+"7");

    }

    private void commitHandler(AsyncResult<Void> seekResult){
        System.out.println("revoked"+"8");
    }

}
