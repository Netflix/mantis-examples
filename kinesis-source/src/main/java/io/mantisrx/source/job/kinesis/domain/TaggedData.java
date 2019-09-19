package io.mantisrx.source.job.kinesis.domain;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.runtime.codec.JsonType;


/**
 * Sink data type for the source job.
 * Represents a payload paired with a list of matched clients.
 */
public class TaggedData implements JsonType {
    private final Set<String> matchedClients = new HashSet<String>();
    private Map<String, Object> payLoad;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public TaggedData(@JsonProperty("data") Map<String, Object> data) {
        this.payLoad = data;
    }

    public Set<String> getMatchedClients() {
        return matchedClients;
    }


    /**
     * Checks if the payload has been tagged for a specified client id.
     * @param clientId The clientId to check.
     * @return A boolean indicating whether clientId is included in the list of tagged clients.
     */
    public boolean matchesClient(String clientId) {
        return matchedClients.contains(clientId);
    }

    /**
     * Adds a client id to the list of matched clients for this payload.
     * @param clientId The client id to be added to the list of matched clients.
     */
    public void addMatchedClient(String clientId) {
        matchedClients.add(clientId);
    }

    public Map<String, Object> getPayload() {
        return this.payLoad;
    }

    public void setPayload(Map<String, Object> newPayload) {
        this.payLoad = newPayload;
    }
}
