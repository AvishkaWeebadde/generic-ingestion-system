package org.phoenix.ingestion.worker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.phoenix.ingestion.model.ConsumeSummary;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WebHookConsumerTest {

    @TempDir
    Path tempDir;   // JUnit creates this fresh per test and cleans it up afterwards

    @Test
    void consumeAll_processesValid_countsFailures_andSurvivesPoison() throws IOException {
        // One valid, then a POISON line in the middle, then a rejected (amount=0), then another valid.
        // The poison-in-the-middle is deliberate: if the loop bailed on a bad message,
        // valid2 would never be counted and succeeded would be 1, not 2.
        String valid1   = "{\"event_id\":\"evt_1\",\"event_type\":\"user.signup\",\"timestamp\":\"2026-06-21T07:00:00Z\",\"payload\":{\"amount\":4900,\"currency\":\"usd\",\"user_id\":\"usr_1\"}}";
        String poison   = "this is not valid json {{{";
        String rejected = "{\"event_id\":\"evt_2\",\"event_type\":\"user.signup\",\"timestamp\":\"2026-06-21T07:00:00Z\",\"payload\":{\"amount\":0,\"currency\":\"usd\",\"user_id\":\"usr_2\"}}";
        String valid2   = "{\"event_id\":\"evt_3\",\"event_type\":\"user.signup\",\"timestamp\":\"2026-06-21T07:00:00Z\",\"payload\":{\"amount\":5000,\"currency\":\"usd\",\"user_id\":\"usr_3\"}}";

        Path file = tempDir.resolve("events.jsonl");
        Files.write(file, List.of(valid1, poison, rejected, valid2));

        WebHookConsumer consumer = new WebHookConsumer(
                new LocalFileMessageSource(file),
                new WebHookProcessor());

        ConsumeSummary summary = consumer.consumeAll();

        assertEquals(2, summary.succeeded(), "both valid events should process");
        assertEquals(2, summary.failed(), "rejected (amount=0) + poison line should both count as failed");
    }
}
