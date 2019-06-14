package com.epam.ab.join.operation;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class BigTableClientConnectionOptions implements Serializable {

    public enum ClientType {
        REST,
        GRPC
    }

    private ClientType clientType;
    private String clientHost;
    private int clientPort;

    public BigTableClientConnectionOptions(ClientType clientType, String clientHost, int clientPort) {
        this.clientType = clientType;
        this.clientHost = clientHost;
        this.clientPort = clientPort;
    }

    public ClientType getClientType() {
        return clientType;
    }

    public String getClientHost() {
        return clientHost;
    }

    public int getClientPort() {
        return clientPort;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("clientType", clientType)
                .append("clientHost", clientHost)
                .append("clientPort", clientPort)
                .toString();
    }
}
