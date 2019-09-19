package io.mantisrx.source.job.kinesis.mql;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import io.mantisrx.mql.jvm.core.Query;


/**
 * Singleton class implementing query management functionality for MQL.
 * The objective is to provide a single source of truth for active queries
 * running in the application.
 */
public final class MQLQueryManager {

    private static class LazyHolder {
        private static final MQLQueryManager INSTANCE = new MQLQueryManager();
    }

    private ConcurrentHashMap<String, Query> queries = new ConcurrentHashMap<>();

    public static MQLQueryManager getInstance() {
        return LazyHolder.INSTANCE;
    }

    private MQLQueryManager() {
    }

    /**
     * Registers a query with this query manager.
     * Overwrites existing queries if the same id registers with a new query.
     * @param id The id of the query to register.
     * @param query The MQL query to register under the given id.
     */
    public void registerQuery(String id, String query) {
        query = MQL.transformLegacyQuery(query);
        Query q = MQL.makeQuery(id, query);
        queries.put(id, q);
    }

    /**
     * De-registers a query with the given id from this manager.
     * @param id The query id to deregister.
     */
    public void deregisterQuery(String id) {
        queries.remove(id);
    }

    /**
     * Fetches the list of registered queries.
     * @return A Collection of MQL query strings currently registered with this manager.
     */
    public Collection<Query> getRegisteredQueries() {
        return queries.values();
    }

    /**
     * Completely clears the state of the query manager.
     */
    public void clear() {
        queries.clear();
    }
}
