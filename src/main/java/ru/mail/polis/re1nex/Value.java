package ru.mail.polis.re1nex;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Optional;

final class Value implements Comparable<Value> {
    private final long timestamp;
    @NotNull
    private final Optional<ByteBuffer> data;

    Value(long timestamp, @Nullable ByteBuffer data) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.data = Optional.of(data);
    }

    Value(long timestamp) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.data = Optional.empty();
    }

    boolean isTombstone() {
        return data.isEmpty();
    }

    ByteBuffer getData() {
        assert !isTombstone();
        return data.orElseThrow().asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    long getTimestamp() {
        return timestamp;
    }
}
