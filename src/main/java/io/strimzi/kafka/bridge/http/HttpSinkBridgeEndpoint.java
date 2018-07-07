package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.UUID;

public class HttpSinkBridgeEndpoint<V, K> extends SinkBridgeEndpoint<V, K> {

    private HttpServerRequest httpServerRequest;

    private String consumerName;

    HttpSinkBridgeEndpoint(Vertx vertx, HttpBridgeConfigProperties httpBridgeConfigProperties) {
        super(vertx, httpBridgeConfigProperties);
    }

    @Override
    public void open() {

    }

    @Override
    public void handle(Endpoint<?> endpoint) {
        httpServerRequest = (HttpServerRequest) endpoint.get();

        String requestPath = httpServerRequest.path();

        //request path always starts with "/" which is not required in path parameters
        String [] params = requestPath.substring(1).split("/");
        log.debug("path params are {}", Arrays.toString(params));

        //consumer creation
        // The path is like this: consumers/{consumer-group}
        // spliting this path will return an array of size 2.
        // param[0] = "consumers", param[1] = "{consumer-group}"
        if (httpServerRequest.method() == HttpMethod.POST && params.length == 2) {

            //set group id of consumer
            groupId = params[1];

            httpServerRequest.bodyHandler(buffer -> {

                if (buffer.toJsonObject().containsKey("name")) {
                    consumerName = buffer.toJsonObject().getString("name");
                }
                //in case name is not mentioned by the client explicitly, assign a random name.
                else {
                    consumerName = "kafka-bridge-consumer-";
                    consumerName += UUID.randomUUID().toString();
                }

                //construct consumer instance id
                consumerInstanceId = consumerName;

                //construct base URI for consumer
                String requestUri = httpServerRequest.absoluteURI();
                if (!requestPath.endsWith("/")){
                    requestUri += "/";
                }
                consumerBaseUri = requestUri+"instances/"+consumerInstanceId;

                //create the consumer
                this.initConsumer();

                //send consumer instance id(name) and base URI as response
                sendConsumerCreationResponse(httpServerRequest.response(), consumerInstanceId, consumerBaseUri);
            });
        }

    }

    private void sendConsumerCreationResponse(HttpServerResponse response, String instanceId, String uri){
        JsonObject jsonResponse = new JsonObject();
        jsonResponse.put("instance_id", instanceId);
        jsonResponse.put("base_uri", uri);

        response.putHeader("Content-length", String.valueOf(jsonResponse.toBuffer().length()));
        response.write(jsonResponse.toBuffer());
        response.end();
    }
}
