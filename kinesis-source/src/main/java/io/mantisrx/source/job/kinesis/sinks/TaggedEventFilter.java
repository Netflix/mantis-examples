package io.mantisrx.source.job.kinesis.sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.mantisrx.source.job.kinesis.domain.TaggedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func1;


/**
 * Filters events in the Sink down to the correct subscriber.
 */
public class TaggedEventFilter implements Func1<Map<String, List<String>>, Func1<TaggedData, Boolean>> {

    private static final String SUBSCRIPTION_ID_PARAM_NAME = "subscriptionId";
    private static final String CLIENT_ID_PARAMETER_NAME = "clientId";

    private static final Logger LOGGER = LoggerFactory.getLogger(TaggedEventFilter.class);

    @Override
    public Func1<TaggedData, Boolean> call(Map<String, List<String>> parameters) {
        Func1<TaggedData, Boolean> filter = new Func1<TaggedData, Boolean>() {
            @Override
            public Boolean call(TaggedData t1) {
                return true;
            }
        };
        if (parameters != null) {
            if (parameters.containsKey(SUBSCRIPTION_ID_PARAM_NAME)) {
                String subId = parameters.get(SUBSCRIPTION_ID_PARAM_NAME).get(0);
                String clientId = parameters.get(CLIENT_ID_PARAMETER_NAME).get(0);
                List<String> terms = new ArrayList<String>();
                if (clientId != null && !clientId.isEmpty()) {
                    terms.add(clientId + "_" + subId);
                } else {
                    terms.add(subId);
                }
                filter = new SourceEventFilter(terms);
            }
            return filter;
        }
        return filter;
    }

    private static class SourceEventFilter implements Func1<TaggedData, Boolean> {
        private String jobId = "UNKNOWN";
        private String jobName = "UNKNOWN";
        private List<String> terms = new ArrayList<String>();

        SourceEventFilter(List<String> terms) {
            this.terms = terms;
            String jId = System.getenv("JOB_ID");
            if (jId != null && !jId.isEmpty()) {
                jobId = jId;
            }

            String jName = System.getenv("JOB_NAME");
            if (jName != null && !jName.isEmpty()) {
                jobName = jName;
            }
            LOGGER.info("Created SourceEventFilter! for subId "
                + terms.toString() + " in Job : " + jobName + " with Id " + jobId);
        }
        @Override
        public Boolean call(TaggedData data) {
            boolean match = true;
            for (String term: terms) {
                if (!data.matchesClient(term)) {
                    match = false;
                    break;
                }
            }
            return match;
        }
    }
}
