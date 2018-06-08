package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class HttpSourceBridgeEndpoint extends SourceBridgeEndpoint {

    private HttpServerRequest httpServerRequest;

    private RequestExtractor requestExtractor;

    private MessageConverter messageConverter;

    private HttpMessage httpMessage;

    private HttpServerResponse httpServerResponse;

    public HttpSourceBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties httpBridgeConfigProperties) {
        super(vertx, httpBridgeConfigProperties);
    }
    @Override
    public void handle(Endpoint<?> endpoint) {
        httpServerRequest = (HttpServerRequest) endpoint.get();
        httpServerResponse = httpServerRequest.response();
        httpMessage = new HttpMessage();
        requestExtractor = new RequestExtractor();
        messageConverter = new HttpDefaultMessageConverter();

        requestExtractor.extract(httpServerRequest);
        httpMessage = requestExtractor.getHttpMessage();

        KafkaProducerRecord record = messageConverter.toKafkaRecord(httpMessage.getTopic(), httpMessage);

        this.send(record,report -> {
            if (report.succeeded()){
                sendHttpResponse(deliveryResponse("success"));
            }
        });
    }

    @Override
    public void consumerCreateHandler(Endpoint<?> endpoint, int cosumerTag) {

    }

    @Override
    public void consumerSubscribeHandler(Endpoint<?> endpoint) {

    }

    @Override
    public void consumerConsumeHandler(Endpoint<?> endpoint) {

    }

    private void sendHttpResponse(String response){
        this.httpServerResponse.putHeader("Content-length", String.valueOf(response.length()))
                .write(response)
                .end();
    }

    private String deliveryResponse(String status){
        JsonObject jsonResponse = new JsonObject();
        jsonResponse.put("status", status);
        return jsonResponse.encode();
    }
}
