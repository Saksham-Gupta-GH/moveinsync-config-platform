package com.moveinsync.core.validator;

import com.moveinsync.core.model.SettingDefinition;
import com.moveinsync.core.model.SettingType;
import com.moveinsync.core.model.TimeWindow;
import com.moveinsync.core.model.ValidationRule;

import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Map;

/**
 * Validates setting values against their definitions and business constraints at the system boundary.
 */
public class SettingValidator {

    public static void validate(SettingDefinition definition, Object value) {
        if (value == null) {
            throw new ValidationException(definition.key(), "Setting value cannot be null");
        }

        SettingType type = definition.type();
        ValidationRule rule = definition.validationRule();

        switch (type) {
            case NUMBER -> validateNumber(definition.key(), value, rule);
            case DURATION_MINUTES -> validateDuration(definition.key(), value, rule);
            case TIME_WINDOW -> validateTimeWindow(definition.key(), value);
            case ENUM -> validateEnum(definition.key(), value, rule);
            case BOOLEAN -> validateBoolean(definition.key(), value);
            case STRING_LIST -> validateList(definition.key(), value, rule);
        }
    }

    private static void validateNumber(String key, Object value, ValidationRule rule) {
        if (!(value instanceof Number number)) {
            throw new ValidationException(key, "Expected numeric value, received: " + value.getClass().getSimpleName());
        }
        double val = number.doubleValue();
        if (rule.minNumber() != null && val < rule.minNumber()) {
            throw new ValidationException(key, "Value " + val + " is below minimum allowed: " + rule.minNumber());
        }
        if (rule.maxNumber() != null && val > rule.maxNumber()) {
            throw new ValidationException(key, "Value " + val + " exceeds maximum allowed: " + rule.maxNumber());
        }
    }

    private static void validateDuration(String key, Object value, ValidationRule rule) {
        if (!(value instanceof Number number)) {
            throw new ValidationException(key, "Duration must be an integer (minutes)");
        }
        int minutes = number.intValue();
        // Negative cut-offs (e.g. -30 min) strictly rejected
        if (minutes < 0) {
            throw new ValidationException(key, "Duration cannot be negative, received: " + minutes);
        }
        if (rule.minDurationMinutes() != null && minutes < rule.minDurationMinutes()) {
            throw new ValidationException(key, "Duration " + minutes + "m is below minimum allowed " + rule.minDurationMinutes() + "m");
        }
        if (rule.maxDurationMinutes() != null && minutes > rule.maxDurationMinutes()) {
            throw new ValidationException(key, "Duration " + minutes + "m exceeds maximum allowed " + rule.maxDurationMinutes() + "m");
        }
    }

    @SuppressWarnings("unchecked")
    private static void validateTimeWindow(String key, Object value) {
        String startStr = null;
        String endStr = null;

        if (value instanceof TimeWindow tw) {
            startStr = tw.start();
            endStr = tw.end();
        } else if (value instanceof Map<?, ?> map) {
            Object s = map.get("start");
            Object e = map.get("end");
            if (s != null) startStr = s.toString();
            if (e != null) endStr = e.toString();
        } else {
            throw new ValidationException(key, "Time window must be an object with 'start' and 'end' time strings (HH:mm)");
        }

        if (startStr == null || endStr == null) {
            throw new ValidationException(key, "Both 'start' and 'end' are required for time window");
        }

        try {
            LocalTime start = LocalTime.parse(startStr);
            LocalTime end = LocalTime.parse(endStr);
            if (start.equals(end)) {
                throw new ValidationException(key, "Window start and end cannot be identical (" + startStr + ")");
            }
        } catch (DateTimeParseException ex) {
            throw new ValidationException(key, "Invalid time format. Expected ISO-8601 HH:mm, received start=" + startStr + ", end=" + endStr);
        }
    }

    private static void validateEnum(String key, Object value, ValidationRule rule) {
        String strVal = value.toString();
        if (rule.allowedEnumValues() != null && !rule.allowedEnumValues().isEmpty()) {
            if (!rule.allowedEnumValues().contains(strVal)) {
                throw new ValidationException(key, "Value '" + strVal + "' is not permitted. Allowed: " + rule.allowedEnumValues());
            }
        }
    }

    private static void validateBoolean(String key, Object value) {
        if (!(value instanceof Boolean)) {
            if (!"true".equalsIgnoreCase(value.toString()) && !"false".equalsIgnoreCase(value.toString())) {
                throw new ValidationException(key, "Expected boolean value (true/false), received: " + value);
            }
        }
    }

    private static void validateList(String key, Object value, ValidationRule rule) {
        if (!(value instanceof Collection<?> col)) {
            throw new ValidationException(key, "Expected collection/list value");
        }
        if (rule.requireNonEmptyList() && col.isEmpty()) {
            throw new ValidationException(key, "List cannot be empty");
        }
    }
}

