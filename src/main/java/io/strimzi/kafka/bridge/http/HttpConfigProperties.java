package io.strimzi.kafka.bridge.http;

public class HttpConfigProperties {

    //port to which server will listen
    private static final int HTTP_DEFAULT_PORT = 8080;

    //client host
    private static final String HTTP_DEFAULT_HOST = "localhost";


    private int httpPort = HTTP_DEFAULT_PORT;

    private String httpHost = HTTP_DEFAULT_HOST;

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public String getHttpHost() {
        return httpHost;
    }

    public void setHttpHost(String httpHost) {
        this.httpHost = httpHost;
    }

}
