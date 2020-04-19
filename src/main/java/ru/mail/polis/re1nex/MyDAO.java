package ru.mail.polis.re1nex;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.TreeMap;

public class MyDAO implements DAO {

    private final TreeMap<ByteBuffer,ByteBuffer> map=new TreeMap<>();
    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(element -> Record.of(element.getKey(), element.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }
}
