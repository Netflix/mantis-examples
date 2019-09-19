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

package io.mantisrx.source.job.kinesis.core;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import io.mantisrx.source.job.kinesis.domain.KinesisAckable;

import java.util.Queue;

/**
 * Factory for record processors.
 */
public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

  private final Queue<KinesisAckable> queue;
  private final Integer checkpointInterval;

  public KinesisRecordProcessorFactory(Queue<KinesisAckable> q, Integer checkpointInterval) {
    this.queue = q;
    this.checkpointInterval = checkpointInterval;
  }

  /**
   * Constructs and returns an instance of KinesisRecordProcessor using
   * the specified queue and checkpoint interval when this factory was constructed.
   */
  public IRecordProcessor createProcessor() {
    return new KinesisRecordProcessor(this.queue, this.checkpointInterval);
  }
}
