package org.phoenix.ingestion.worker;

import org.phoenix.ingestion.model.ConsumeSummary;
import org.phoenix.ingestion.model.InboundMessage;
import org.phoenix.ingestion.model.ProcessingResult;
import org.phoenix.ingestion.model.WebhookEvent;
import org.phoenix.ingestion.utils.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;

public class WebHookConsumer {

    private static final Logger log = LoggerFactory.getLogger(WebHookConsumer.class);

    private final MessageSource source;
    private final WebHookProcessor processor;

    public WebHookConsumer(MessageSource source, WebHookProcessor processor) {
        this.source = source;
        this.processor = processor;
    }

    public ConsumeSummary consumeAll() {
        int succeeded = 0;
        int failed = 0;
        while (true) {
            List<InboundMessage> messages = source.receive();

            if (messages == null || messages.isEmpty()) {
                log.info("Message queue fully drained. Exiting consumer loop cleanly.");
                break;
            }

            for (InboundMessage message : messages) {
                MDC.put("message_id", message.id());
                try {
                    WebhookEvent event = JsonUtil.fromJson(message.body(), WebhookEvent.class);
                    MDC.put("event_id", event.eventId());
                    MDC.put("event_type", event.eventType());

                    ProcessingResult result = processor.process(event);

                    if (result.success()) {
                        MDC.put("duration_ms", String.valueOf(result.durationMs()));
                        log.info("Successfully processed webhook event");
                        source.delete(message);
                        succeeded++;
                    } else {
                        log.warn("Rejected webhook event. Reason: {}", result.reason());
                        failed++;
                    }
                } catch (Exception ex) {
                    log.error("Poison message intercepted, skipping", ex);
                    failed++;
                } finally {
                    MDC.clear();
                }
            }
        }
        return new ConsumeSummary(succeeded, failed);
    }
}
