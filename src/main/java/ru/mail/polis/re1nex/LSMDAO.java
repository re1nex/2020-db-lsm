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
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Stream;

public class LSMDAO implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    private File storage;
    private long flushThreshold;

    //Data
    private MemTable memTable;
    private final NavigableMap<Integer,Table> ssTables;

    //State
    private int generation = 0;


    public LSMDAO(@NotNull final File storage,final long flushThreshold) throws IOException {
        assert flushThreshold>0L;
        this.flushThreshold = flushThreshold;
        this.storage = storage;
        this.ssTables=new TreeMap<>();
        this.memTable = new MemTable();
        generation = -1;
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
                            if (gen > generation) {
                                generation = gen;
                            }
                        }
                );
        generation++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iterators = new ArrayList<>(ssTables.size()+1);
        iterators.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(t-> {
            try {
                iterators.add(t.iterator(from));
            } catch (IOException e) {
                throw new RuntimeException("Error",e);
            }
        });
        final Iterator<Cell> mergedIterator = Iterators.mergeSorted(iterators,Cell.COMPARATOR);
        final Iterator<Cell> fresh = Iters.collapseEquals(mergedIterator,Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(fresh,e->!e.getValue().isTombstone());
        return Iterators.transform(alive,e->Record.of(e.getKey(),e.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key,value);
        if(memTable.sizeInBytes()>flushThreshold){
            flush();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
        if(memTable.sizeInBytes()>flushThreshold){
            flush();
        }
    }

    private void flush() throws IOException{
        //Dump memTable
        final File file = new File(storage,generation+TEMP);
        SSTable.serialize(file,memTable.iterator(ByteBuffer.allocate(0)),memTable.size());
        final File dst = new File(storage,generation+SUFFIX);
        Files.move(file.toPath(),dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        //Switch
        memTable=new MemTable();
        ssTables.put(generation,new SSTable(dst));
        generation++;
    }
    @Override
    public void close() throws IOException {
        if(memTable.size()>0){
            flush();
        }
        for (int i = 0; i < ssTables.size() ; i++) {
            ssTables.get(i).close();
        }
    }
}
