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

package com.netflix.mantis.examples;

import io.mantisrx.connectors.job.source.JobSource;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.lifecycle.LifecycleNoOp;
import io.mantisrx.runtime.sink.Sinks;


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
            .source(new JobSource())
                .stage(new MQLStage(), MQLStage.config())
            .sink(Sinks.eagerSubscribe(Sinks.sse((String data) -> data)))
                .lifecycle(new LifecycleNoOp())
            .create();
    }
}
