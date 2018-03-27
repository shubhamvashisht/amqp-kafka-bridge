package io.strimzi.kafka.bridge.http;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;


import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HttpBridge extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(HttpBridge.class);
    private HttpServer httpServer;
    private HttpBridgeConfigProperties httpConfigProperties;

    @Autowired
    public void setBridgeConfigProperties(HttpBridgeConfigProperties bridgeConfigProperties) {
        this.httpConfigProperties = bridgeConfigProperties;
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        //creates http server
        httpServer = vertx.createHttpServer(configureServer());

        httpConfigProperties = new HttpBridgeConfigProperties();


        httpServer.requestHandler((request)->{
            serveRequests(request);
        }).listen((res)->{
            if (res.succeeded()){
                log.info("running at port {}",httpServer.actualPort());
                log.info("Kafka bootstrap servers {}", this.httpConfigProperties.getKafkaConfigProperties().getBootstrapServers());
                log.info(this.httpConfigProperties.getKafkaConfigProperties().getProducerConfig().getValueSerializer());
                log.info(this.httpConfigProperties.getKafkaConfigProperties().getProducerConfig().getKeySerializer());

                startFuture.complete();
            }
            else {
                System.out.println("failed: {}"+res.cause());
                startFuture.fail(res.cause());
            }
        });

    }

    private HttpServerOptions configureServer(){
        HttpServerOptions serverOptions = new HttpServerOptions();
        serverOptions.setPort(this.httpConfigProperties.getEndpointConfigProperties().getPort());
        return serverOptions;
    }

    private void serveRequests(HttpServerRequest request){
        if (request.method().toString().equals("GET")){
            new HttpSinkBridgeEndpoint<>(this.vertx, this.httpConfigProperties).handle(new HttpEndpoint(request));
        }
        else if (request.method().toString().equals("POST")){
            new HttpSourceBridgeEndpoint(this.vertx, this.httpConfigProperties).handle(new HttpEndpoint(request));
        }
    }
}
