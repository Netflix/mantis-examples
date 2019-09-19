/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
