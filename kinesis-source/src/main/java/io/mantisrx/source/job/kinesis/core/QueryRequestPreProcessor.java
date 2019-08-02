package io.mantisrx.source.job.kinesis.core;

import java.util.List;
import java.util.Map;

import io.mantisrx.runtime.Context;
import io.mantisrx.source.job.kinesis.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func2;


public class QueryRequestPreProcessor  implements Func2<Map<String, List<String>>, Context,Void> {

    private static final Logger logger = LoggerFactory.getLogger(QueryRequestPreProcessor.class);
    public QueryRequestPreProcessor() { }

    @Override
    public Void call(Map<String, List<String>> queryParams, Context context) {

        logger.info("KafkaRequestPreProcessor:queryParams: " + queryParams);

        if (queryParams != null) {

            if(queryParams.containsKey(Constants.SUBSCRIPTION_ID) && queryParams.containsKey(Constants.CRITERION)) {

                final String subId = queryParams.get(Constants.SUBSCRIPTION_ID).get(0);
                final String query = queryParams.get(Constants.CRITERION).get(0);
                final String clientId = queryParams.get(Constants.CLIENT_ID).get(0);

                if(subId != null && query != null) {

                    try {
                        logger.info("Registering query ");
                        if(clientId != null && !clientId.isEmpty()) {
                            registerQuery(clientId + "_" + subId, query);
                        } else {
                            registerQuery(subId, query);
                        }
                    } catch(Exception e) {
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
