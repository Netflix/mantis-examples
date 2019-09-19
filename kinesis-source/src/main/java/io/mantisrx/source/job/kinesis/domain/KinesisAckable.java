package io.mantisrx.source.job.kinesis.domain;

import rx.subjects.PublishSubject;

import java.util.Optional;

/**
 * Domain object allowing us to wrap a Kinesis payload
 * in something we can acknowledge has passed processing.
 */
public class KinesisAckable {
    private final String payload;
    private final String sequenceNumber;
    private final Optional<PublishSubject<String>> acks;

    public KinesisAckable(String payload) {
        this.payload = payload;
        this.sequenceNumber = "";
        this.acks = Optional.empty();
    }

    public KinesisAckable(String payload, String sequenceNumber, PublishSubject<String> acks) {
        this.payload = payload;
        this.sequenceNumber = sequenceNumber;
        this.acks = Optional.of(acks);
    }

    public String getPayload() {
        return payload;
    }

    /**
     * Acknowledges that this data has been processed and is eligible to be checkpointed.
     */
    public void ack() {
        this.acks.map(a -> {
            a.onNext(this.sequenceNumber);
            return this.sequenceNumber;
        });
    }
}
