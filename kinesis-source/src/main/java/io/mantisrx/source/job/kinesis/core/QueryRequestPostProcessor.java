package io.mantisrx.source.job.kinesis.core;

import java.util.List;
import java.util.Map;

import io.mantisrx.runtime.Context;
import io.mantisrx.source.job.kinesis.Constants;
import io.mantisrx.source.job.kinesis.mql.MQLQueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func2;


public class QueryRequestPostProcessor  implements Func2<Map<String, List<String>>, Context,Void> {

    private static final Logger logger = LoggerFactory.getLogger(QueryRequestPostProcessor.class);

    public QueryRequestPostProcessor() {

    }

    @Override
    public Void call(Map<String, List<String>> queryParams, Context context) {

        logger.info("RequestPostProcessor:queryParams: " + queryParams);

        if (queryParams != null) {

            if(queryParams.containsKey(Constants.SUBSCRIPTION_ID) && queryParams.containsKey(Constants.CRITERION)) {

                final String subId = queryParams.get(Constants.SUBSCRIPTION_ID).get(0);

                final String query = queryParams.get(Constants.CRITERION).get(0);

                final String clientId = queryParams.get("clientId").get(0);

                if(subId != null && query != null) {

                    try {
                        if(clientId != null && !clientId.isEmpty()) {
                            deregisterQuery(clientId + "_" + subId);
                        } else {
                            deregisterQuery(subId);
                        }
                    } catch(Exception e) {
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
