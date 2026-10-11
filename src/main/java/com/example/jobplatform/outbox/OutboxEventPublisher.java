package com.example.jobplatform.outbox;

public interface OutboxEventPublisher {

    void publish(OutboxClaim claim) throws Exception;
}
