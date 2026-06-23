package org.phoenix.ingestion.worker;

import org.phoenix.ingestion.model.ConsumeSummary;
import org.phoenix.ingestion.model.InboundMessage;
import org.phoenix.ingestion.model.ProcessingResult;
import org.phoenix.ingestion.model.WebhookEvent;
import org.phoenix.ingestion.utils.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        while(true) {
            List<InboundMessage> messages = source.receive();

            if (messages == null || messages.isEmpty()) {
                log.info("Message queue fully drained. Exiting consumer loop cleanly.");
                break;
            }

            for(InboundMessage message: messages) {
                try {
                    WebhookEvent event = JsonUtil.fromJson(message.body(), WebhookEvent.class);
                    ProcessingResult result = processor.process(event);

                    if (result.success()) {
                        log.info("successfully processed {}", event.eventId());
                        source.delete(message);
                        succeeded++;
                    } else {
                        log.warn("rejected {}: {}", event.eventId(), result.reason());
                        failed++;
                    }
                } catch (Exception ex) {
                    log.warn("skipping poison message {}", message.id(), ex);
                    failed++;
                }
            }
        }
        return new ConsumeSummary(succeeded, failed);
    }
}
