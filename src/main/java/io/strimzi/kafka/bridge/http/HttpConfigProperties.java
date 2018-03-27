package io.strimzi.kafka.bridge.http;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "http")
public class HttpConfigProperties {

    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 8080;

    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;

    /**
     * Get the host for AMQP client (to connect) or server (to bind)
     *
     * @return
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Set the host for AMQP client (to connect) or server (to bind)
     *
     * @param host  AMQP host
     * @return  this instance for setter chaining
     */
    public HttpConfigProperties setHost(String host) {
        this.host = host;
        return this;
    }


    public int getPort() {
        return this.port;
    }


    public void setPort(int port) {
        this.port = port;
    }


}
