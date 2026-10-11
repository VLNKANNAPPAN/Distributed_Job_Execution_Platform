package com.example.jobplatform.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.TreeMap;
import org.springframework.stereotype.Component;

@Component
public class RequestFingerprint {

    private final ObjectMapper objectMapper;

    public RequestFingerprint(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String calculate(String type, int priority, JsonNode payload) {
        return calculate(type, priority, payload, null);
    }

    public String calculate(String type, int priority, JsonNode payload, Instant runAt) {
        return calculate(type, priority, payload, runAt, null);
    }

    public String calculate(
            String type,
            int priority,
            JsonNode payload,
            Instant runAt,
            Integer executionTimeoutSeconds) {
        ObjectNode normalized = objectMapper.createObjectNode();
        normalized.put("fingerprintVersion", 3);
        normalized.put("priority", priority);
        normalized.set("payload", canonicalize(payload));
        if (runAt == null) {
            normalized.putNull("runAt");
        } else {
            normalized.put("runAt", runAt.toString());
        }
        if (executionTimeoutSeconds == null) {
            normalized.putNull("executionTimeoutSeconds");
        } else {
            normalized.put("executionTimeoutSeconds", executionTimeoutSeconds);
        }
        normalized.put("type", type);
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(normalized);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes));
        } catch (JsonProcessingException | NoSuchAlgorithmException exception) {
            throw new IllegalStateException("Unable to fingerprint submission", exception);
        }
    }

    private JsonNode canonicalize(JsonNode node) {
        if (node.isObject()) {
            ObjectNode result = objectMapper.createObjectNode();
            TreeMap<String, JsonNode> sorted = new TreeMap<>();
            node.fields().forEachRemaining(entry -> sorted.put(entry.getKey(), entry.getValue()));
            sorted.forEach((key, value) -> result.set(key, canonicalize(value)));
            return result;
        }
        if (node.isArray()) {
            ArrayNode result = objectMapper.createArrayNode();
            node.forEach(element -> result.add(canonicalize(element)));
            return result;
        }
        return node.deepCopy();
    }
}
