package org.phoenix.ingestion.worker;

import org.junit.jupiter.api.Test;
import org.phoenix.ingestion.model.Payload;
import org.phoenix.ingestion.model.ProcessingResult;
import org.phoenix.ingestion.model.WebhookEvent;

import static org.junit.jupiter.api.Assertions.*;

public class WebHookProcessorTest {

    private final WebHookProcessor processor = new WebHookProcessor();

    @Test
    void should_pass_for_valid_event() {
        ProcessingResult result = processor.process(eventWithAmount(4900L));
        assertTrue(result.success());
        assertTrue(result.durationMs() >= 50);
    }

    @Test
    void should_fail_when_amount_is_zero() {
        ProcessingResult result = processor.process(eventWithAmount(0L));
        assertFalse(result.success());
        assertTrue(result.reason().contains("amount"));
    }

    @Test
    void should_fail_when_amount_is_null() {
        ProcessingResult result = processor.process(eventWithAmount(null));
        assertFalse(result.success());
        assertTrue(result.reason().contains("amount"));
    }

    private WebhookEvent eventWithAmount(Long amount) {
        return new WebhookEvent("evt_1", "user.signup", "2026-06-21T07:00:00Z",
                new Payload(amount, "usd", "usr_99"));
    }

}
