package org.phoenix.ingestion.worker;

import org.phoenix.ingestion.exception.ValidationException;
import org.phoenix.ingestion.model.ProcessingResult;
import org.phoenix.ingestion.model.WebhookEvent;
import org.phoenix.ingestion.utils.ValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class WebHookProcessor {

    private static final Logger log = LoggerFactory.getLogger(WebHookProcessor.class);


    public ProcessingResult process(WebhookEvent event) {
        if (event == null) {
            return ProcessingResult.fail("Received null webhook event container", 0);
        }

        long startTime = System.currentTimeMillis();
        try {
            validateEvent(event);

            var payload = event.payload();
            if (payload.amount() <= 0) {
                return ProcessingResult.fail("Invalid business data: amount must be greater than zero", System.currentTimeMillis() - startTime);
            }

            long simulatedLatency = ThreadLocalRandom.current().nextLong(50, 151);
            Thread.sleep(simulatedLatency);

            long durationMs = System.currentTimeMillis() - startTime;
            return ProcessingResult.success(durationMs);

        } catch (ValidationException ex) {
            log.warn("Payload validation failed for event_id [{}]: {}", event.eventId(), ex.getMessage());
            return ProcessingResult.fail(ex.getMessage(), System.currentTimeMillis() - startTime);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return ProcessingResult.fail("Processing thread was interrupted abnormally", System.currentTimeMillis() - startTime);
        }
    }

    private void validateEvent(WebhookEvent event) throws ValidationException {
        ValidationUtil.checkNotNullOrEmpty(event.eventId(), "event_id");
        ValidationUtil.checkNotNullOrEmpty(event.eventType(), "event_type");
        ValidationUtil.checkNotNull(event.payload(), "payload");
        ValidationUtil.checkNotNull(event.payload().amount(), "payload.amount");
        ValidationUtil.checkNotNullOrEmpty(event.payload().userId(), "payload.user_id");
        ValidationUtil.checkNotNullOrEmpty(event.payload().currency(), "payload.currency");
    }
}