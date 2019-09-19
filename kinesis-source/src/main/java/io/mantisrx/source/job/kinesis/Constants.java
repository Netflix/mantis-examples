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

package io.mantisrx.source.job.kinesis;

/**
 * Collection site for constants used within the Kinesis Source Job.
 */
public final class Constants {

    private Constants() {
    }

    /** Query parameter for subscription ids. */
    public static final String SUBSCRIPTION_ID = "subscriptionId";

    /** Query parameter for client ids. */
    public static final String CLIENT_ID = "clientId";

    /** Query parameter for MQL query. */
    public static final String CRITERION = "criterion";


    /** Job Parameter for the Kinesis stream name. */
    public static final String STREAM = "stream";

    /** Job Parameter describing the application name. */
    public static final String APPLICATION = "application";

    /** Job Parameter for polling queue length. */
    public static final String QUEUE_LENGTH = "queueLength";

    /** Job Parameter for Kinesis polling interval. */
    public static final String POLL_INTERVAL = "pollInterval";

    /** Job Parameter for message parser type. */
    public static final String PARSER_TYPE = "messageParserType";

    /** Job Parameter for the checkpoint interval. */
    public static final String CHECKPOINT_INTERVAL = "checkpointInterval";
}
