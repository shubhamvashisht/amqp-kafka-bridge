package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpMessageConverter;
import io.strimzi.kafka.bridge.http.extractors.PostRequestExtractor;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class HttpSourceBridgeEndpoint extends SourceBridgeEndpoint {

    //private HttpConnection httpConnection;

    private HttpServerRequest httpServerRequest;

    private HttpMessage httpMessage;

    private MessageConverter httpMessageConverter;

    private HttpServerResponse httpServerResponse;

    public HttpSourceBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties bridgeConfigProperties){
        super(vertx, bridgeConfigProperties);
    }

    @Override
    public void handle(Endpoint<?> endpoint) {

        PostRequestExtractor postRequestExtractor = new PostRequestExtractor();

        this.httpServerRequest = (HttpServerRequest) endpoint.get();
        //this.httpConnection = this.httpServerRequest.connection();
        this.httpServerResponse = httpServerRequest.response();

        httpMessage = new PostRequestExtractor().getHttpMessage(this.httpServerRequest);

        httpMessageConverter = new HttpMessageConverter();

        KafkaProducerRecord<String,byte[]> record = httpMessageConverter.toKafkaRecord(httpMessage.getTopic(),httpMessage);

        this.open();

        this.send(record,recordMetadataAsyncResult -> {
            if (recordMetadataAsyncResult.succeeded()){
                String response = "published the message at topic --> "+record.topic()+"";
                httpServerResponse.putHeader("Content-Length",String.valueOf(response.length()));
                httpServerResponse.write(response);
            }
            else{
                log.debug("failed");
            }
        });

    }

   private void processCloseHandler(HttpConnection httpConnection){

    }
}
