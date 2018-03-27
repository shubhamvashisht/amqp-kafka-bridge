package io.strimzi.kafka.bridge.http;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;


import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import org.springframework.stereotype.Component;

@Component
public class HttpBridge extends AbstractVerticle {

    private HttpServer httpServer;
    private HttpBridgeConfigProperties httpConfigProperties;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        //creates http server
        httpServer = vertx.createHttpServer(configureServer());

        httpConfigProperties = new HttpBridgeConfigProperties();

        httpServer.requestHandler((request)->{
            serveRequests(request);
        }).listen((res)->{
            if (res.succeeded()){
                System.out.println("running at port : {}"+httpServer.actualPort());
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
        //System.out.println("lolll"+this.httpConfigProperties.getEndpointConfigProperties().getHttpPort());
        serverOptions.setPort(3000);
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
