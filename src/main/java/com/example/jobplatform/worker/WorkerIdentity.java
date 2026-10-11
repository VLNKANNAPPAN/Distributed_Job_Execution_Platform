package com.example.jobplatform.worker;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class WorkerIdentity {

    private final String value;

    public WorkerIdentity(WorkerProperties properties) {
        String configured = properties.getId();
        this.value = configured == null || configured.isBlank()
                ? generatedIdentity()
                : validate(configured.strip());
    }

    public String value() {
        return value;
    }

    private static String generatedIdentity() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException exception) {
            host = "unknown";
        }
        String process = ManagementFactory.getRuntimeMXBean().getName().split("@", 2)[0];
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String candidate = host + "-" + process + "-" + suffix;
        return candidate.length() <= 64 ? candidate : candidate.substring(0, 64);
    }

    private static String validate(String candidate) {
        if (candidate.isBlank() || candidate.length() > 64) {
            throw new IllegalArgumentException("worker identity must contain 1 to 64 characters");
        }
        return candidate;
    }
}
