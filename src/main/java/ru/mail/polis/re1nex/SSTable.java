package ru.mail.polis.re1nex;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;

final class SSTable implements Table {

    private FileChannel channel;
    private int numRows;
    private long sizeData;

    SSTable(@NotNull final File file) throws IOException {
        channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final long sizeFile = channel.size();
        channel.position(sizeFile - Integer.BYTES);
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buf);
        numRows = buf.rewind().getInt();
        sizeData = sizeFile - (numRows + 1) * Integer.BYTES;
    }

    private int getOffset(int numRow) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buf, sizeData + numRow * Integer.BYTES);
        return buf.rewind().getInt();
    }

    @NotNull
    private ByteBuffer key(final int row) throws IOException {
        int offset = getOffset(row);
        ByteBuffer keySize = ByteBuffer.allocate(Integer.BYTES);
        channel.read(keySize, offset);
        ByteBuffer key = ByteBuffer.allocate(keySize.rewind().getInt());
        channel.read(key, offset + Integer.BYTES);
        return key.rewind();
    }

    @NotNull
    private Cell cell(final int row) throws IOException {
        int offset = getOffset(row);
        ByteBuffer key = key(row);
        offset += key.remaining() + Integer.BYTES;
        ByteBuffer timestamp = ByteBuffer.allocate(Long.BYTES);
        channel.read(timestamp, offset);
        offset += Long.BYTES;
        ByteBuffer tombstone = ByteBuffer.allocate(1);
        channel.read(tombstone, offset);
        offset += 1;
        if (tombstone.get() == 1) {
            return new Cell(key, new Value(timestamp.rewind().getLong()));
        } else {
            ByteBuffer valueSize = ByteBuffer.allocate(Integer.BYTES);
            channel.read(valueSize, offset);
            ByteBuffer value = ByteBuffer.allocate(valueSize.rewind().getInt());
            offset += Integer.BYTES;
            channel.read(value, offset);
            return new Cell(key, new Value(timestamp.rewind().getLong(), value.rewind()));
        }
    }

    private int binarySearch(@NotNull final ByteBuffer from) throws IOException {
        int l = 0;
        int r = numRows - 1;
        while (l <= r) {
            final int med = (l + r) / 2;
            final int cmp = key(med).compareTo(from);
            if (cmp < 0) {
                l = med + 1;
            } else if (cmp > 0) {
                r = med - 1;
            } else {
                return med;
            }
        }
        return l;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return new Iterator<Cell>() {
            int pos = binarySearch(from);

            @Override
            public boolean hasNext() {
                return pos < numRows;
            }

            @Override
            public Cell next() {
                try {
                    return cell(pos);
                } catch (IOException e) {
                    return null;
                }
            }
        };
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException("Immutable");
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        throw new UnsupportedOperationException("Immutable");
    }


    /*
    keySize(Integer)|key|timestamp(Long)|tombstone(Byte)||valueSize(Integer)|value||
    offsets
    n
     */
    static void serialize(File file, Iterator<Cell> iterator, int size) throws IOException {
        try (FileChannel fileChannel = new FileOutputStream(file).getChannel()) {

            ArrayList<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (iterator.hasNext()) {
                offsets.add(offset);
                Cell buf = iterator.next();
                ByteBuffer key = buf.getKey();
                Value value = buf.getValue();
                Integer keySize = key.remaining();
                offset += Integer.BYTES + keySize + Long.BYTES + 1;
                fileChannel.write(ByteBuffer.allocate(Integer.BYTES)
                        .putInt(keySize)
                        .rewind());
                fileChannel.write(key);
                fileChannel.write(ByteBuffer.allocate(Long.BYTES)
                        .putLong(value.getTimestamp())
                        .rewind());
                if (value.isTombstone()) {
                    fileChannel.write(ByteBuffer.allocate(1)
                            .put((byte) 1)
                            .rewind());
                } else {
                    fileChannel.write(ByteBuffer.allocate(1)
                            .put((byte) 0)
                            .rewind());
                    ByteBuffer data = value.getData();
                    Integer valueSize = data.remaining();
                    offset += Integer.BYTES + valueSize;
                    fileChannel.write(ByteBuffer.allocate(Integer.BYTES)
                            .putInt(valueSize)
                            .rewind());
                    fileChannel.write(data);
                }
            }
            Integer offsetSize = offsets.size();
            for (final Integer off : offsets) {
                fileChannel.write(ByteBuffer.allocate(Integer.BYTES)
                        .putInt(off)
                        .rewind());
            }
            fileChannel.write(ByteBuffer.allocate(Integer.BYTES)
                    .putInt(offsetSize)
                    .rewind());
        }
    }

    public void close() throws IOException {
        channel.close();
    }
}

