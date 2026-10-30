package com.moveinsync.core.model;

import java.util.List;
import java.util.Set;

/**
 * Encapsulates validation constraints for a configuration setting.
 */
public record ValidationRule(
        Double minNumber,
        Double maxNumber,
        Integer minDurationMinutes,
        Integer maxDurationMinutes,
        Set<String> allowedEnumValues,
        boolean requireNonEmptyList
) {
    public static ValidationRule none() {
        return new ValidationRule(null, null, null, null, null, false);
    }

    public static ValidationRule durationRange(int minMinutes, int maxMinutes) {
        return new ValidationRule(null, null, minMinutes, maxMinutes, null, false);
    }

    public static ValidationRule numberRange(double min, double max) {
        return new ValidationRule(min, max, null, null, null, false);
    }

    public static ValidationRule enumValues(Set<String> allowed) {
        return new ValidationRule(null, null, null, null, allowed, false);
    }
}
