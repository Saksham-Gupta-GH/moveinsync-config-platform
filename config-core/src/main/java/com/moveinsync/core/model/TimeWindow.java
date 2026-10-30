package com.moveinsync.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * Value object representing a time window (e.g., Night Window: 20:00 to 06:00).
 */
public record TimeWindow(
        @JsonProperty("start") String start,
        @JsonProperty("end") String end
) {
    @JsonCreator
    public TimeWindow {
        Objects.requireNonNull(start, "start time cannot be null");
        Objects.requireNonNull(end, "end time cannot be null");
    }

    public LocalTime getStartTime() {
        return LocalTime.parse(start);
    }

    public LocalTime getEndTime() {
        return LocalTime.parse(end);
    }

    public boolean isValid() {
        try {
            LocalTime s = getStartTime();
            LocalTime e = getEndTime();
            // Start and end cannot be identical
            return !s.equals(e);
        } catch (DateTimeParseException ex) {
            return false;
        }
    }
}

