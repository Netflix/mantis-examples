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

import java.util.List;
import java.util.Map;

import io.mantisrx.runtime.Context;
import io.mantisrx.source.job.kinesis.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func2;


/**
 * Handles any pre-processing tasks for a query after all clients have disconnected.
 */
public class QueryRequestPreProcessor  implements Func2<Map<String, List<String>>, Context, Void> {

    private final Logger logger = LoggerFactory.getLogger(QueryRequestPreProcessor.class);
    public QueryRequestPreProcessor() {
    }

    @Override
    public Void call(Map<String, List<String>> queryParams, Context context) {

        logger.info("KafkaRequestPreProcessor:queryParams: " + queryParams);

        if (queryParams != null) {

            if (queryParams.containsKey(Constants.SUBSCRIPTION_ID) && queryParams.containsKey(Constants.CRITERION)) {

                final String subId = queryParams.get(Constants.SUBSCRIPTION_ID).get(0);
                final String query = queryParams.get(Constants.CRITERION).get(0);
                final String clientId = queryParams.get(Constants.CLIENT_ID).get(0);

                if (subId != null && query != null) {

                    try {
                        logger.info("Registering query ");
                        if (clientId != null && !clientId.isEmpty()) {
                            registerQuery(clientId + "_" + subId, query);
                        } else {
                            registerQuery(subId, query);
                        }
                    } catch (Exception e) {
                        logger.error("Error registering query " + e.getMessage());
                    }
                    //propagateSubscriptionCriterion("localhost",8080,subId, query);
                }
            }
        }
        return null;
    }

    private static synchronized void registerQuery(String subId, String query) {
        QueryRefCountMap.INSTANCE.addQuery(subId, query);
    }
}
