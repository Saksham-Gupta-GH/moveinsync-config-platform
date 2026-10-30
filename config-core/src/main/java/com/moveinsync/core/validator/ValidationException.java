package com.moveinsync.core.validator;

/**
 * Domain exception thrown when configuration input fails validation constraints.
 */
public class ValidationException extends RuntimeException {
    private final String key;

    public ValidationException(String key, String message) {
        super("Validation failed for [" + key + "]: " + message);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}

