package io.mantisrx.source.job.kinesis;

import io.mantisrx.runtime.lifecycle.LifecycleNoOp;
import io.mantisrx.source.job.kinesis.core.QueryRequestPostProcessor;
import io.mantisrx.source.job.kinesis.core.QueryRequestPreProcessor;
import io.mantisrx.source.job.kinesis.sources.KinesisSource;
import io.mantisrx.source.job.kinesis.sinks.TaggedDataSourceSink;
import io.mantisrx.source.job.kinesis.stages.TaggingStage;
import io.mantisrx.source.job.kinesis.domain.JobParams;
import io.mantisrx.source.job.kinesis.domain.KinesisAckable;
import io.mantisrx.runtime.*;
import io.mantisrx.runtime.codec.JacksonCodecs;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.source.job.kinesis.domain.TaggedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisSourceJob extends MantisJobProvider<TaggedData> {

  private String getEnvOrElse(String name, String orElse) {
      String result = System.getenv(name);
      return result == null ? orElse : result;
  }

  private final String ENV_PREFIX = "JOB_PARAM_";
  private final String APPLICATION_NAME = getEnvOrElse("MANTIS_JOB_NAME", "local-application");
  private final String STREAM_NAME = getEnvOrElse(ENV_PREFIX + JobParams.STREAM, "fake-stream");
  private final Integer QUEUE_LENGTH = Integer.parseInt(getEnvOrElse(ENV_PREFIX + JobParams.QUEUE_LENGTH, "1000"));
  private final Integer POLL_INTERVAL = Integer.parseInt(getEnvOrElse(ENV_PREFIX + JobParams.POLL_INTERVAL, "100"));
  private final Integer CHECKPOINT_INTERVAL = Integer.parseInt(getEnvOrElse(ENV_PREFIX + JobParams.CHECKPOINT_INTERVAL, "300"));
  private final Integer WORKER_INDEX = Integer.parseInt(getEnvOrElse("MANTIS_WORKER_INDEX", "1"));
  private final String WORKER_ID = APPLICATION_NAME + "_" + WORKER_INDEX;

  private static final Logger logger = LoggerFactory.getLogger(KinesisSourceJob.class);

  public Job<TaggedData> getJobInstance() {

    logger.info("JOB ENVIRONMENT: ");
    for (String varName : System.getenv().keySet()) {
      logger.info("env: " + varName + " = " + System.getenv(varName));
    }

    String jobPropertiesFilename = "job.properties";

    return MantisJob.source(new KinesisSource(APPLICATION_NAME, STREAM_NAME, WORKER_ID, QUEUE_LENGTH, POLL_INTERVAL, CHECKPOINT_INTERVAL))
            .stage(new TaggingStage(this.APPLICATION_NAME), new ScalarToScalar.Config<KinesisAckable, TaggedData>()
                    .codec(JacksonCodecs.pojo(TaggedData.class)))
            .sink(new TaggedDataSourceSink(new QueryRequestPreProcessor(), new QueryRequestPostProcessor()))
            .lifecycle(new LifecycleNoOp())
            .parameterDefinition(new StringParameter()
                    .name(JobParams.STREAM)
                    .description("The name of the Kinesis stream to which this job should connect.")
                    .validator(Validators.notNullOrEmpty())
                    .required()
                    .build())
            .parameterDefinition(new IntParameter()
                    .name(JobParams.QUEUE_LENGTH)
                    .description("The size of the queue to buffer from Kinesis. The source job will apply backpressure when the queue is full.")
                    .defaultValue(1000)
                    .validator(Validators.range(1, 100000))
                    .build())
            .parameterDefinition(new IntParameter()
                    .name(JobParams.POLL_INTERVAL)
                    .description("The interval on which the source should drain the queue into the job pipeline in milliseconds.")
                    .defaultValue(100)
                    .validator(Validators.range(1, 1000 * 60))
                    .build())
            .parameterDefinition(new IntParameter()
                    .name(JobParams.CHECKPOINT_INTERVAL)
                    .description("The interval on which the worker should checkpoint in seconds. The KCL documentation recommends 5 minutes for steady state applications.")
                    .defaultValue(5 * 60)
                    .validator(Validators.range(10, 60 * 60))
                    .build())
            .create();
  }

  public static void main(String[] args) {
    LocalJobExecutorNetworked.execute(new KinesisSourceJob().getJobInstance(),
            new Parameter(JobParams.APPLICATION,"local-applications"),
            new Parameter(JobParams.STREAM, "stream"));
  }
}
