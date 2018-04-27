package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpMessageConverter;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class HttpSourceBridgeEndpoint extends SourceBridgeEndpoint {

    private HttpConnection httpConnection;

    private HttpServerRequest httpServerRequest;

    private HttpMessage httpMessage;

    private MessageConverter httpMessageConverter;

    public HttpSourceBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties bridgeConfigProperties){
        super(vertx, bridgeConfigProperties);
    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        this.httpServerRequest = (HttpServerRequest) endpoint.get();
        this.httpConnection = this.httpServerRequest.connection();

        httpMessage = new MessageExtractor().getHttpMessage(this.httpServerRequest);

        httpMessageConverter = new HttpMessageConverter();

        KafkaProducerRecord<String,byte[]> record = httpMessageConverter.toKafkaRecord(httpMessage.getTopic(),httpMessage);

        this.open();

        this.send(record,null);

    }

   private void processCloseHandler(HttpConnection httpConnection){

    }
}
