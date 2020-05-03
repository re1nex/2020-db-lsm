package ru.mail.polis.re1nex;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class NewDAO implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    private final File storage;
    private final long flushThreshold;

    //Data
    private MemTable memTable;
    private final NavigableMap<Integer, Table> ssTables;

    //State
    private int ver = 0;

    /**
     * Realization of LSMDAO.
     * @param storage - SSTable storage directory
     * @param flushThreshold - max size of MemTable
     */
    public NewDAO(@NotNull final File storage, final long flushThreshold) {
        assert flushThreshold > 0L;
        this.flushThreshold = flushThreshold;
        this.storage = storage;
        this.ssTables = new TreeMap<>();
        this.memTable = new MemTable();
        ver = -1;
        final File[] list = storage.listFiles((dir1, name) -> name.endsWith(SUFFIX));
        assert list != null;
        Arrays.stream(list)
                .filter(currentFile -> !currentFile.isDirectory())
                .forEach(f -> {
                            final String name = f.getName();
                            final int gen =
                                    Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                            try {
                                ssTables.put(gen, new SSTable(f));
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                            if (gen > ver) {
                                ver = gen;
                            }
                        }
                );
        ver++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iterators = new ArrayList<>(ssTables.size() + 1);
        iterators.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(t -> {
            try {
                iterators.add(t.iterator(from));
            } catch (IOException e) {
                throw new RuntimeException("Error", e);
            }
        });
        final Iterator<Cell> mergedIterator = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        final Iterator<Cell> fresh = Iters.collapseEquals(mergedIterator, Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(fresh, e -> !e.getValue().isTombstone());
        return Iterators.transform(alive, e -> Record.of(e.getKey(), e.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
        //Dump memTable
        final File file = new File(storage, ver + TEMP);
        SSTable.serialize(file, memTable.iterator(ByteBuffer.allocate(0)));
        final File dst = new File(storage, ver + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        //Switch
        memTable = new MemTable();
        ssTables.put(ver, new SSTable(dst));
        ver++;
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }
        for (int i = 0; i < ssTables.size(); i++) {
            ssTables.get(i).close();
        }
    }
}
