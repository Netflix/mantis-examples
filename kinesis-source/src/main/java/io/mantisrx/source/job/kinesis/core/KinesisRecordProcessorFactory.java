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
