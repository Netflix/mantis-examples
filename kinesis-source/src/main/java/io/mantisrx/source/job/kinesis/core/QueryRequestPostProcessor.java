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
 * Handles any post-processing tasks for a query after all clients have disconnected.
 */
public class QueryRequestPostProcessor  implements Func2<Map<String, List<String>>, Context, Void> {

    private final Logger logger = LoggerFactory.getLogger(QueryRequestPostProcessor.class);

    public QueryRequestPostProcessor() {

    }

    @Override
    public Void call(Map<String, List<String>> queryParams, Context context) {

        logger.info("RequestPostProcessor:queryParams: " + queryParams);

        if (queryParams != null) {

            if (queryParams.containsKey(Constants.SUBSCRIPTION_ID) && queryParams.containsKey(Constants.CRITERION)) {

                final String subId = queryParams.get(Constants.SUBSCRIPTION_ID).get(0);

                final String query = queryParams.get(Constants.CRITERION).get(0);

                final String clientId = queryParams.get("clientId").get(0);

                if (subId != null && query != null) {

                    try {
                        if (clientId != null && !clientId.isEmpty()) {
                            deregisterQuery(clientId + "_" + subId);
                        } else {
                            deregisterQuery(subId);
                        }
                    } catch (Exception e) {
                        logger.error("Error propagating unsubscription notification " + e.getMessage());
                    }
                    //propagateSubscriptionCriterion("localhost",8080,subId, query);
                }
            }
        }
        return null;
    }

    private void deregisterQuery(String subId) {
        QueryRefCountMap.INSTANCE.removeQuery(subId);
    }



}
