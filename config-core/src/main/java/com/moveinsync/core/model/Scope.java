package com.moveinsync.core.model;

/**
 * Hierarchical resolution tiers.
 * Precedence order (highest to lowest): SHIFT -> OFFICE -> CLIENT -> GLOBAL
 */
public enum Scope {
    DEFAULT(0),
    GLOBAL(1),
    CLIENT(2),
    OFFICE(3),
    SHIFT(4);

    private final int priority;

    Scope(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }
}

