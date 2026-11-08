package com.moveinsync.server.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.encoder.EncoderBase;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** One JSON object per line, including fields useful for server-side log queries. */
public class JsonLogEncoder extends EncoderBase<ILoggingEvent> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] headerBytes() {
        return null;
    }

    @Override
    public byte[] footerBytes() {
        return null;
    }

    @Override
    public byte[] encode(ILoggingEvent event) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("timestamp", Instant.ofEpochMilli(event.getTimeStamp()).toString());
        fields.put("level", event.getLevel().toString());
        fields.put("logger", event.getLoggerName());
        fields.put("thread", event.getThreadName());
        fields.put("message", event.getFormattedMessage());
        if (event.getThrowableProxy() != null) {
            fields.put("exceptionType", event.getThrowableProxy().getClassName());
        }
        try {
            byte[] json = mapper.writeValueAsBytes(fields);
            byte[] line = java.util.Arrays.copyOf(json, json.length + 1);
            line[json.length] = '\n';
            return line;
        } catch (JsonProcessingException e) {
            return "{\"level\":\"ERROR\",\"message\":\"Log serialization failed\"}\n"
                    .getBytes(StandardCharsets.UTF_8);
        }
    }
}
