package com.greencomnetworks.franzmanager.entities;

import java.util.Map;
import java.util.Objects;

public class Broker {
    public String id;
    public String host;
    public Integer port;
    public Integer jmxPort;
    public Map<String, String> configurations;
    public State state;

    public Broker(String id, String host, Integer port, Integer jmxPort, Map<String, String> configurations, State state) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.jmxPort = jmxPort;
        this.configurations = configurations;
        this.state = state;
    }

    @Override
    public String toString() {
        return "Broker: " + id + ", host: " + host + ", port: " + port + ", jmxPort: " + jmxPort + ", configuration: " + (configurations == null ? "null" : configurations.toString()) + ", State: " + state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Broker broker = (Broker) o;
        return broker.host.equals(this.host) && broker.port.equals(this.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    public enum State {
        OK,
        UNSTABLE,
        BROKEN
    }
}
