package ru.mail.polis.re1nex;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

final class MemTable implements Table {

    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long sizeInBytes = 720L;

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(element -> new Cell(element.getKey(), element.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        if (map.containsKey(key)) {
            sizeInBytes += value.remaining() + Long.BYTES;
        } else {
            sizeInBytes += value.remaining() + key.remaining() + Long.BYTES;
        }
        map.put(key, new Value(System.currentTimeMillis(), value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (map.containsKey(key)) {
            if (!map.get(key).isTombstone()) {
                sizeInBytes -= map.get(key).getData().remaining();
            }
        } else {
            sizeInBytes += key.remaining() + Long.BYTES;
        }
        map.put(key, new Value(System.currentTimeMillis()));
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }

    int size() {
        return map.size();
    }

    long sizeInBytes() {
        return sizeInBytes;
    }
}
