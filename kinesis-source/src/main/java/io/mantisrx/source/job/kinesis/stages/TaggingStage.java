package io.mantisrx.source.job.kinesis.stages;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.source.job.kinesis.domain.KinesisAckable;
import io.mantisrx.mql.jvm.core.Query;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.source.job.kinesis.domain.TaggedData;
import io.mantisrx.source.job.kinesis.mql.MQLQueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.nio.charset.Charset;
import java.util.*;

public class TaggingStage implements ScalarComputation<KinesisAckable, TaggedData> {

    public static final String MANTIS_META_SOURCE_NAME = "mantis.meta.sourceName";
    public static final String MANTIS_META_SOURCE_TIMESTAMP = "mantis.meta.timestamp";
    public static final String MANTIS_META = "mantis.meta";

    private static final Logger logger = LoggerFactory.getLogger(TaggingStage.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final String jobName;

    public TaggingStage(String jobName) {
        this.jobName = jobName;
    }

    // no op
    protected Map<String, Object> preProcess(Map<String, Object> rawData) {
        return rawData;
    }

    protected  Map<String,Object> applyPreMapping(final Context context, final Map<String,Object> rawData) {
        return rawData;
    }

    protected Map<String, Object> processAndAck(final Context context, KinesisAckable ackable) {
        Map<String, Object> eventData = new HashMap<>();
        try {
            eventData = objectMapper.readValue(ackable.getPayload().getBytes(Charset.forName("UTF-8")), new TypeReference<Map<String, Object>>() {});
        } catch (JsonParseException jpe) {
            logger.error("Error parsing payload {}", jpe);
        } catch (Exception e) {
            logger.error("caught unexpected exception", e);
        } finally {
            // ack processing completed
            ackable.ack();
        }
        return eventData;
    }

    private boolean isMetaEvent(Map<String, Object> event) {
        return event.containsKey(MANTIS_META);
    }

    protected List<TaggedData> tagData(Map<String, Object> d, Context context) {
        List<TaggedData> taggedDataList = new ArrayList<>();
        boolean metaEvent = isMetaEvent(d);
        Collection<Query> queries = MQLQueryManager.getInstance().getRegisteredQueries();
        Iterator<Query> it = queries.iterator();
        while (it.hasNext()) {
            Query query = it.next();
            if (metaEvent) {
                TaggedData tg = new TaggedData(d);
                tg.addMatchedClient(query.getSubscriptionId());
                taggedDataList.add(tg);
            } else if (query.matches(d)) {
                Map<String, Object> projected = query.project(d);
                projected.put(MANTIS_META_SOURCE_NAME, this.jobName);
                projected.put(MANTIS_META_SOURCE_TIMESTAMP, System.currentTimeMillis());
                TaggedData tg = new TaggedData(projected);
                tg.addMatchedClient(query.getSubscriptionId());
                taggedDataList.add(tg);
            }
        }
        return taggedDataList;
    };

    public Observable<TaggedData> call(Context context, Observable<KinesisAckable> data) {
        return data
                .map(datum -> preProcess(processAndAck(context, datum)))
                .filter(Map::isEmpty)
                .map(mapData -> applyPreMapping(context, mapData))
                .filter(Map::isEmpty)
                .map(d -> tagData(d, context))
                .filter(List::isEmpty)
                .flatMap(Observable::from);
    }
}
