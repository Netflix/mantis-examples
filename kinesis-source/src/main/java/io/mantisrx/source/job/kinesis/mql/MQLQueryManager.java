package io.mantisrx.source.job.kinesis.mql;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import io.mantisrx.mql.jvm.core.Query;


public class MQLQueryManager {

    private static class LazyHolder {
        private static final MQLQueryManager INSTANCE = new MQLQueryManager();
    }

    private ConcurrentHashMap<String, Query> queries = new ConcurrentHashMap<>();

    public static MQLQueryManager getInstance() {
        return LazyHolder.INSTANCE;
    }

    private MQLQueryManager() { }

    public void registerQuery(String id, String query) {
        query = MQL.transformLegacyQuery(query);
        Query q = MQL.makeQuery(id, query);
        queries.put(id, q);
    }

    public void deregisterQuery(String id) {
        queries.remove(id);
    }

    public Collection<Query> getRegisteredQueries() {
        return queries.values();
    }

    public void clear() {
        queries.clear();
    }

    public static void main(String[] args) throws Exception {
        MQLQueryManager qm = getInstance();
        String query = "SELECT * WHERE true SAMPLE {\"strategy\":\"RANDOM\",\"threshold\":1}";
        qm.registerQuery("fake2", query);
        System.out.println(MQL.parses(MQL.transformLegacyQuery(query)));
        System.out.println(MQL.getParseError(MQL.transformLegacyQuery(query)));
        System.out.println(qm.getRegisteredQueries());
    }
}
