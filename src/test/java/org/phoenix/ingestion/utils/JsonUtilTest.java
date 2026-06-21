package org.phoenix.ingestion.utils;

import org.junit.jupiter.api.Test;
import org.phoenix.ingestion.model.WebhookEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class JsonUtilTest {

    @Test
    void should_ParseSnakeCaseJson_Into_ImmutableRecords() {
        String json = """
        {
          "event_id": "evt_987654321",
          "event_type": "user.payment_processed",
          "timestamp": "2026-06-21T07:00:00Z",
          "payload": {
            "amount": 4900,
            "currency": "usd",
            "user_id": "usr_12345"
          }
        }
        """;

        WebhookEvent event = JsonUtil.fromJson(json, WebhookEvent.class);
        assertNotNull(event);
        assertEquals("evt_987654321", event.eventId());
        assertEquals("user.payment_processed", event.eventType());

        assertNotNull(event.payload());
        assertEquals(4900, event.payload().amount());
        assertEquals("usr_12345", event.payload().userId());

    }
}
