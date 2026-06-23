package org.phoenix.ingestion.worker;

import org.phoenix.ingestion.model.InboundMessage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class LocalFileMessageSource implements MessageSource{

    private static final int MAX_BATCH = 10;   // SQS hands out up to 10 per call;

    private final Queue<InboundMessage> inbox = new ArrayDeque<>();

    public LocalFileMessageSource(Path file) {
        try {
            List<String> lines = Files.readAllLines(file);
            long counter = 0;
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }
                String id = "msg-" + (++counter);
                inbox.add(new InboundMessage(id, line));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read message file: " + file, e);
        }
    }

    @Override
    public List<InboundMessage> receive() {
        List<InboundMessage> batch = new ArrayList<>();
        while (batch.size() < MAX_BATCH && !inbox.isEmpty()) {
            batch.add(inbox.poll());     // poll() removes AND returns the front item
        }
        return batch;
    }

    @Override
    public void delete(InboundMessage message) {
        // No-op: a file message is already removed from the inbox in receive().
    }
}
