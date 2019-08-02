package com.netflix.mantis.examples;

import java.util.HashMap;
import java.util.Map;

import com.mantisrx.common.utils.JsonUtility;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.ScalarComputation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class MQLStage implements ScalarComputation<MantisServerSentEvent, String> {
    private static Logger logger = LoggerFactory.getLogger(MQLLiteJob.class);

    @Override
    public void init(Context context) {
    }
    
    @Override
    public Observable<String> call(Context context, Observable<MantisServerSentEvent> eventObs) {
        HashMap<String, Observable<Map<String, Object>>> mqlContext = new HashMap<>();

        mqlContext.put("stream", eventObs.map(MantisServerSentEvent::getEventAsString)
                .map(JsonUtility::jsonToMap));

        return io.mantisrx.mql.jvm.Core.evalMql("select * from stream where tick > 15 && tick < 30", mqlContext)
                .map(Object::toString);
    }

    public static ScalarToScalar.Config<MantisServerSentEvent,String> config() {
        // This is a simple scalar stage that transforms MantisServerSentEvent -> String
        return new ScalarToScalar.Config<MantisServerSentEvent,String>()
            // codec specifies how to encode the data while sending it across the wire
            .codec(Codecs.string())
            // concurrentInput allows incoming messages to be processed in parallel using rxJava observeOn operator
            .concurrentInput();
    }
}
