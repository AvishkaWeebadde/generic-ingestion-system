package org.phoenix.ingestion.model;

public record ProcessingResult(boolean success, String reason, long durationMs) {
    public static ProcessingResult success(long durationMs) {
        return new ProcessingResult(true, "PROCESSED", durationMs);
    }

    public static ProcessingResult fail(String reason, long duration) {
        return new ProcessingResult(false, reason, duration);
    }
}

