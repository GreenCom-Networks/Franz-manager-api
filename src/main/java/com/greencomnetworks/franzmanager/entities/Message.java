package com.greencomnetworks.franzmanager.entities;

import org.apache.kafka.common.header.Headers;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class Message {
    public final Map<String, String> headers;
    public final String message;
    public final String key;
    public final Integer partition;
    public final Long offset;
    public final Long timestamp;

    public Message(String message, String key, Integer partition, Long offset, Long timestamp, Headers headers) {
        this.message = message;
        this.key = key;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.headers = new HashMap<>();
        headers.forEach(header -> {
            try {
                this.headers.put(header.key(), new String(header.value(), "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                this.headers.put(header.key(), new String(header.value()));
                e.printStackTrace();
            }
        });
    }

    @Override
    public String toString() {
        return "Message: " + message + ", key:" + key + ", partition:" + partition + ", offset:" + offset + ", timestamp:" + timestamp;
    }
}
