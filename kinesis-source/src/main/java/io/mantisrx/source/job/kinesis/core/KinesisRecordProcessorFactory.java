package io.mantisrx.source.job.kinesis.core;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import io.mantisrx.source.job.kinesis.domain.KinesisAckable;

import java.util.Queue;

public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

  private final Queue<KinesisAckable> queue;
  private final Integer checkpointInterval;

  public KinesisRecordProcessorFactory(Queue<KinesisAckable> q, Integer checkpointInterval) {
    this.queue = q;
    this.checkpointInterval = checkpointInterval;
  }

  public IRecordProcessor createProcessor() {
    return new KinesisRecordProcessor(this.queue, this.checkpointInterval);
  }
}
