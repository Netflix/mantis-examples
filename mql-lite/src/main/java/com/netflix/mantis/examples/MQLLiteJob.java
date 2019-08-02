package com.netflix.mantis.examples;

import java.util.concurrent.TimeUnit;

import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.connectors.job.source.JobSource;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.lifecycle.LifecycleNoOp;
import io.mantisrx.runtime.sink.Sinks;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import rx.Observable;


/**
 * This sample job demonstrates the use mantis java client to connect to a source job.
 * Source Jobs are job that connect to production servers directly and fetch streaming data
 * on behalf of client jobs
 */
public class MQLLiteJob extends MantisJobProvider<String> {
    @Override
    public Job<String> getJobInstance() {
        String jobPropertiesFilename = "job.properties";

        // Source Job emits data of type MantisServerSentEvent
        return MantisJob
        	// Built in connector to connect to another mantis job (typically source job)	
            .source(new JobSource())
                .stage(new MQLStage(), MQLStage.config())
            .sink(Sinks.eagerSubscribe(Sinks.sse((String data) -> data)))
                .lifecycle(new LifecycleNoOp())
            .create();
    }

    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new MQLLiteJob().getJobInstance());
    }

}
