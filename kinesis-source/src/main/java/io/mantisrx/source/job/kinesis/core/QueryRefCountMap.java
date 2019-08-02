package io.mantisrx.source.job.kinesis.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.mantisrx.source.job.kinesis.mql.MQLQueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryRefCountMap {
    private static final Logger logger = LoggerFactory.getLogger(QueryRefCountMap.class);
    public static final QueryRefCountMap INSTANCE = new QueryRefCountMap();
    private final ConcurrentHashMap<String, AtomicInteger> refCntMap = new ConcurrentHashMap<String, AtomicInteger>();

    private QueryRefCountMap() {

    }

    public void addQuery(String subId, String query) {
        logger.info("adding query " + subId + " query " + query);
        if(refCntMap.containsKey(subId)) {
            int newVal = refCntMap.get(subId).incrementAndGet();
            logger.info("query exists already incrementing refcnt to " + newVal);
        } else {
            MQLQueryManager.getInstance().registerQuery(subId, query);
            refCntMap.putIfAbsent(subId, new AtomicInteger(1));
            logger.info("new query registering it");
        }
    }

    public void removeQuery(String subId) {
        if(refCntMap.containsKey(subId)) {
            AtomicInteger refCnt = refCntMap.get(subId);
            int currVal = refCnt.decrementAndGet();

            if(currVal == 0) {
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

    public int getUniqueSubscriptionsCount() {
        return refCntMap.size();
    }

    /**
     * For testing
     * @param subId
     * @return
     */
    int getQueryRefCount(String subId) {
        if(refCntMap.containsKey(subId)) {
            return refCntMap.get(subId).get();
        } else {
            return 0;
        }
    }

}
