package ru.mail.polis.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

final class Cell implements Comparable<Cell> {

    @NotNull
    private final ByteBuffer key;
    @NotNull
    private final Value value;

    Cell(@NotNull final ByteBuffer key, @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    @NotNull
    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    @NotNull
    public Value getValue() {
        return value;
    }

    @Override
    public int compareTo(@NotNull final Cell cell) {
        final int cmp = key.compareTo(cell.getKey());
        return cmp == 0 ? Long.compare(cell.getValue().getTimestamp(), value.getTimestamp()) : cmp;
    }
}
