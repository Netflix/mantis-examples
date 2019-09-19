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

/**
 * Primary logic stage for a source job, checks active queries against incoming data
 * and tags it with the correct.
 */
public class TaggingStage implements ScalarComputation<KinesisAckable, TaggedData> {

    /** JSON property name for source job. */
    public static final String MANTIS_META_SOURCE_NAME = "mantis.meta.sourceName";
    /** JSON property name for time stamp. */
    public static final String MANTIS_META_SOURCE_TIMESTAMP = "mantis.meta.timestamp";
    /** JSON property for Mantis metadata. */
    public static final String MANTIS_META = "mantis.meta";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Logger logger = LoggerFactory.getLogger(TaggingStage.class);

    private final String jobName;

    public TaggingStage(String jobName) {
        this.jobName = jobName;
    }

    /**
     * No-op preprocessor. Implements identity function for events.
     * @param rawData An event to be pre-processed.
     * @return The provided rawData event unchanged.
     */
    protected Map<String, Object> preProcess(Map<String, Object> rawData) {
        return rawData;
    }

    /**
     * No-op pre-mapper. Implements identity function for events.
     * @param context The mantis context for this execution.
     * @param rawData An event to be pre-mapped.
     * @return The provided event, unchanged.
     */
    protected  Map<String, Object> applyPreMapping(final Context context, final Map<String, Object> rawData) {
        return rawData;
    }

    /**
     * Deserializes data and acknowledges such that the source can check point.
     * @param context The Mantis context for this execution.
     * @param ackable A KinesisAckable which will be deserialized and then acked so as to be checkpointable.
     */
    protected Map<String, Object> processAndAck(final Context context, KinesisAckable ackable) {
        Map<String, Object> eventData = new HashMap<>();
        try {
            eventData = OBJECT_MAPPER.readValue(ackable.getPayload().getBytes(Charset.forName("UTF-8")),
                new TypeReference<Map<String, Object>>() { });
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

    /**
     * Tags an event with any matching queries.
     * @param d An event to be tagged.
     * @param context The Mantis context provided by the runtime.
     * @return A list of events tagged with subscription ids.
     */
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
    }

    /**
     * Intended to be called by the Mantis runtime which will provide context and data.
     *
     * @param context The Mantis context.
     * @param data An Observable of raw data to be tagged with matching queries.
     * @return An Observable of TaggedData for the sink to hand out to clients.
     */
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
