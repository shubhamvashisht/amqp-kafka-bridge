package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.config.BridgeConfigProperties;

public class HttpBridgeConfigProperties extends BridgeConfigProperties<HttpConfigProperties> {

   public HttpBridgeConfigProperties(){
       //instanciate the kafka server
       super();
       //set kafka configuration acc. to server config
        this.endpointConfigProperties = new HttpConfigProperties();
    }
}
