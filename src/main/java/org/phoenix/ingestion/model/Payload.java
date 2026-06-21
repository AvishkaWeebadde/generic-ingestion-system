package org.phoenix.ingestion.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Payload(
        Long amount,
        String currency,
        String userId
) {
}
