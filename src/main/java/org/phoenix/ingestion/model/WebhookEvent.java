package org.phoenix.ingestion.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record WebhookEvent(
        String eventId,
        String eventType,
        String timestamp,
        Payload payload
) { }
