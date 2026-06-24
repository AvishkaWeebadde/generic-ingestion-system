package org.phoenix.ingestion;

import org.phoenix.ingestion.model.ConsumeSummary;
import org.phoenix.ingestion.worker.LocalFileMessageSource;
import org.phoenix.ingestion.worker.WebHookConsumer;
import org.phoenix.ingestion.worker.WebHookProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {

        final Logger log = LoggerFactory.getLogger(Main.class);
        String filePath = args.length > 0 ? args[0] : "webhooks.txt";

        try {
            WebHookConsumer consumer = new WebHookConsumer(
                    new LocalFileMessageSource(Path.of(filePath)),
                    new WebHookProcessor());
            ConsumeSummary summary = consumer.consumeAll();

            log.info("Done. succeeded={}, failed={}", summary.succeeded(), summary.failed());

        } catch (Exception e) {
            log.error("Error reading the file {} ", filePath, e);
            System.exit(1);
        }
    }
}