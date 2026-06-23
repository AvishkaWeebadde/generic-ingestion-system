package org.phoenix.ingestion.worker;

import org.phoenix.ingestion.model.InboundMessage;

import java.util.List;

public interface MessageSource {
    List<InboundMessage> receive();
    void delete(InboundMessage message);
}
