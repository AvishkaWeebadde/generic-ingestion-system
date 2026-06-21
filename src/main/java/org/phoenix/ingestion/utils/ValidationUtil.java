package org.phoenix.ingestion.utils;

import org.phoenix.ingestion.exception.ValidationException;

import java.util.Objects;

public final class ValidationUtil {
    private ValidationUtil() {
    }

    public static void checkNotNull(Object obj, String fieldName) {
        if (obj == null) {
            throw new ValidationException(fieldName + " must not be null");
        }
    }

    public static void checkNotNullOrEmpty(String str, String fieldName) {
        if (str == null || str.trim().isEmpty()) {
            throw new ValidationException(fieldName + " must not be null or empty");
        }
    }
}
