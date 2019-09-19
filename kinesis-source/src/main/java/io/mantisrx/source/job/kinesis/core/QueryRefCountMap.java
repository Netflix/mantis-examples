package io.mantisrx.source.job.kinesis.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.mantisrx.source.job.kinesis.mql.MQLQueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs accounting for multiple queries. This allows us to deduplicate queries
 * as well as perform accounting when many shards of the same client connect.
 */
public final class QueryRefCountMap {
    private final Logger logger = LoggerFactory.getLogger(QueryRefCountMap.class);

    /** Implements singleton interface. */
    public static final QueryRefCountMap INSTANCE = new QueryRefCountMap();
    private final ConcurrentHashMap<String, AtomicInteger> refCntMap = new ConcurrentHashMap<String, AtomicInteger>();

    private QueryRefCountMap() {

    }

    /**
     * Adds a query to the given subscriptionId.
     *
     * @param subId The subscription id for which to account the query
     * @param query The MQL query in question.
     */
    public void addQuery(String subId, String query) {
        logger.info("adding query " + subId + " query " + query);
        if (refCntMap.containsKey(subId)) {
            int newVal = refCntMap.get(subId).incrementAndGet();
            logger.info("query exists already incrementing refcnt to " + newVal);
        } else {
            MQLQueryManager.getInstance().registerQuery(subId, query);
            refCntMap.putIfAbsent(subId, new AtomicInteger(1));
            logger.info("new query registering it");
        }
    }

    /**
     * Removes a subscription, cleaning up if the query
     * was the last.
     */
    public void removeQuery(String subId) {
        if (refCntMap.containsKey(subId)) {
            AtomicInteger refCnt = refCntMap.get(subId);
            int currVal = refCnt.decrementAndGet();

            if (currVal == 0) {
                MQLQueryManager.getInstance().deregisterQuery(subId);
                refCntMap.remove(subId);
                logger.info("All references to query are gone removing query");
            } else {
                logger.info("References to query still exist. decremeting refcnt to " + currVal);
            }
        } else {
            logger.warn("No query with subscriptionId " + subId);
        }
    }

    /**
     * Computes the number of unique queries.
     * @return The total count of unique queries.
     */
    public int getUniqueSubscriptionsCount() {
        return refCntMap.size();
    }

    /**
     * For testing.
     * @param subId The subscription id in question.
     * @return The count for the provided subscription id.
     */
    int getQueryRefCount(String subId) {
        if (refCntMap.containsKey(subId)) {
            return refCntMap.get(subId).get();
        } else {
            return 0;
        }
    }

}
