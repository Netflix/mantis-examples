package io.mantisrx.source.job.kinesis.sources;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import io.mantisrx.source.job.kinesis.core.KinesisRecordProcessorFactory;
import io.mantisrx.source.job.kinesis.domain.KinesisAckable;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import org.apache.log4j.Logger;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Serves as the source component of a Mantis job which can consume from a Kinesis
 * stream.
 */
public class KinesisSource implements Source<KinesisAckable> {

    private static Logger logger = Logger.getLogger(KinesisSource.class);

    private final String applicationName;
    private final String streamName;
    private final String workerId;
    private final AWSCredentialsProvider credentialsProvider = InstanceProfileCredentialsProvider.getInstance();
    private final KinesisClientLibConfiguration config;
    private final IRecordProcessorFactory recordProcessorFactory;
    private final Worker worker;
    private final BlockingQueue<KinesisAckable> queue;
    private final Integer pollingInterval;
    private final Integer checkpointInterval;

    /**
     * A Mantis source for Kinesis streams.
     * TODO: Consider the performance of different methods for draining the queue to the pipeline.
     * TODO: Explore newer APIs for draining the queue.
     * @param applicationName The name of the application, KCL will use this for checkpointing.
     * @param streamName The name of the Kinesis stream from which the job should read.
     * @param workerId The worker ID for this worker, used for checkpointing and sharding.
     * @param queueLength The length of the queue on which the worker should drop messages.
     * @param pollingInterval The interval on which the source should drain the queue into an Observable for the job
     *                        pipeline.
     */
    public KinesisSource(String applicationName, String streamName, String workerId, Integer queueLength,
        Integer pollingInterval, Integer checkpointInterval) {
        this.applicationName = applicationName;
        this.streamName = streamName;
        this.workerId = workerId;
        this.queue  = new LinkedBlockingQueue<>(queueLength);
        this.pollingInterval = pollingInterval;
        this.checkpointInterval = checkpointInterval;

       config = new KinesisClientLibConfiguration(this.applicationName,
                this.streamName,
                this.credentialsProvider,
                this.workerId);

       recordProcessorFactory = new KinesisRecordProcessorFactory(queue, this.checkpointInterval);

       worker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(config)
                .build();
    }

    /**
     * Method is intended to be called by the Mantis runtime with both parameters being provided.
     * @param context The Mantis context for this execution.
     * @param index the Mantis index for this execution.
     * @return An Observable of the source data from the specific Kinesis stream.
     */
    public Observable<Observable<KinesisAckable>> call(Context context, Index index) {
        // Run the Kinesis worker in a separate thread.
        Observable.just("worker").subscribeOn(Schedulers.newThread()).forEach(x -> {
            worker.run();
        });

        return Observable.interval(pollingInterval, TimeUnit.MILLISECONDS).map(x -> {
            ArrayList<KinesisAckable> sink = new ArrayList<>(queue.size());
            queue.drainTo(sink);
            return Observable.from(sink);
        });
    }
}
