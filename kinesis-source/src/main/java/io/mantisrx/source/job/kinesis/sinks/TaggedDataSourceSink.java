package io.mantisrx.source.job.kinesis.sinks;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.sink.ServerSentEventsSink;
import io.mantisrx.runtime.sink.Sink;
import io.mantisrx.runtime.sink.predicate.Predicate;
import io.mantisrx.source.job.kinesis.domain.TaggedData;
import rx.Observable;
import rx.functions.Func2;


/**
 * A Mantis Sink for TaggedData which is the primary domain object for this job.
 */
public class TaggedDataSourceSink implements Sink<TaggedData> {

    /** preprocessor for connections to this sink. */
    private Func2<Map<String, List<String>>, Context, Void> preProcessor = new NoOpProcessor();
    /** postprocessor for connections to this sink. */
    private Func2<Map<String, List<String>>, Context, Void> postProcessor = new NoOpProcessor();

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * A no op function which serves as the default pre/post processor.
     */
    static class NoOpProcessor implements Func2<Map<String, List<String>>, Context, Void> {
        @Override
        public Void call(Map<String, List<String>> t1, Context t2) {
            return null;
        }
    }

    public TaggedDataSourceSink() {
    }

    public TaggedDataSourceSink(Func2<Map<String, List<String>>, Context, Void> preProcessor,
                                Func2<Map<String, List<String>>, Context, Void> postProcessor) {
        this.postProcessor = postProcessor;
        this.preProcessor = preProcessor;
    }

    @Override
    public void call(Context context, PortRequest portRequest,
                     Observable<TaggedData> observable) {
        observable = observable
                .filter((t1) -> {
                    return !t1.getPayload().isEmpty();
                });
        ServerSentEventsSink<TaggedData> sink = new ServerSentEventsSink.Builder<TaggedData>()
                .withEncoder((data) -> {
                    try {
                        String json = mapper.writeValueAsString(data.getPayload());
                        return json;
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        return "{\"error\":" + e.getMessage() + "}";
                    }
                })
                .withPredicate(new Predicate<TaggedData>("description", new TaggedEventFilter()))
                .withRequestPreprocessor(preProcessor)
                .withRequestPostprocessor(postProcessor)
                .build();

        observable.subscribe();
        sink.call(context, portRequest, observable);
    }
}
