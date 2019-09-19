/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.mantis.examples;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mantisrx.common.utils.JsonUtility;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.ScalarComputation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * Performs MQL processing as a Mantis stage.
 */
public class MQLStage implements ScalarComputation<MantisServerSentEvent, String> {
    private static Logger logger = LoggerFactory.getLogger(MQLLiteJob.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void init(Context context) {
    }

    @Override
    public Observable<String> call(Context context, Observable<MantisServerSentEvent> eventObs) {
        HashMap<String, Observable<Map<String, Object>>> mqlContext = new HashMap<>();

        mqlContext.put("stream", eventObs.map(MantisServerSentEvent::getEventAsString)
                .map(JsonUtility::jsonToMap));

        String query = context.getParameters().get("query", "select * from stream").toString();

        return io.mantisrx.mql.jvm.Core.evalMql(query, mqlContext)
                .map(event -> {
                    try {
                        return OBJECT_MAPPER.writeValueAsString(event);
                    } catch (Exception ex) {
                        logger.error("Error serializing output: {}", ex.getMessage());
                        return "";
                    }
                })
                .filter(event -> event != null && !event.toString().isEmpty());
    }

    /**
     * Provides the Mantis configuration for this stage.
     * @return The Mantis config for this stage.
     */
    public static ScalarToScalar.Config<MantisServerSentEvent, String> config() {
        return new ScalarToScalar.Config<MantisServerSentEvent, String>()
            .codec(Codecs.string())
            .concurrentInput();
    }
}
