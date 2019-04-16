package org.apache.ignite.internal.processors.transfer;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class FileSegment implements AutoCloseable {
    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** The default region size to transfer data. */
    @GridToStringExclude
    private static final int REGION_SIZE = 1024 * 1024;

    /** The underlying java file representation. */
    private final File fileCfg;

    /** The unique file name to identify transfer to.*/
    private final String name;

    /** The position offest to start at. */
    private final long position;

    /** The number of bytes to send. */
    private final long expect;

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /** The number of bytes successfully sent or received. */
    private long transferred;

    /**
     * Create a new instance of segmented file to transfer using the given java {@link File} representation.
     *
     * @param fileCfg The java {@link File} representation which should be transfered.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param expect The number of bytes to expect of transfer.
     */
    public FileSegment(File fileCfg, String name, long position, long expect) {
        assert position >= 0 : "The file position must be non-negative";
        assert expect >= 0 : "The number of bytes to sent must be positive";

        this.fileCfg = Objects.requireNonNull(fileCfg);
        this.name = Objects.requireNonNull(name);
        this.position = position;
        this.expect = expect;
    }

    /**
     * @return {@code true} if the has an open file-descriptor.
     */
    public boolean isOpen() {
        return fileIo != null;
    }

    /**
     * Explicitly open the underlying file if not done yet.
     */
    public void open() throws IOException {
        if (!isOpen())
            fileIo = dfltIoFactory.create(fileCfg);
    }

    /**
     * @return The offset in the file where the transfer began.
     */
    public long position() {
        return position;
    }

    /**
     * @return The bytes which was transfered already.
     */
    public long transferred() {
        return transferred;
    }

    /**
     * @return The number of bytes to transfer.
     */
    public long count() {
        return expect;
    }

    /**
     * @return The underlying {@link File} is used to.
     */
    public File fileCfg() {
        return fileCfg;
    }

    /**
     * @return The string representation file name.
     */
    public String name() {
        return name;
    }

    /**
     * @param target Destination channel of the transfer operation.
     * @return Count of bytes which was successfully transferred.
     * @throws IOException If fails.
     */
    public long transferTo(WritableByteChannel target) throws IOException {
        assert position >= 0;
        assert transferred >= 0;

        if (!isOpen())
            throw new IOException("The file is not open to transfer data to: " + this);

        long sent = fileIo.transferTo(position + transferred, REGION_SIZE, target);

        if (sent > 0)
            transferred += sent;

        return sent;
    }

    /**
     * @param source The source channel of the transfer operation.
     * @throws IOException If fails.
     */
    public long transferFrom(ReadableByteChannel source) throws IOException {
        assert position >= 0;
        assert transferred >= 0;

        if (!isOpen())
            throw new IOException("The file is not open to transfer data to: " + this);

        long got = fileIo.transferFrom(source, position + transferred, REGION_SIZE);

        if (got > 0)
            transferred += got;

        return got;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        U.closeQuiet(fileIo);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileSegment.class, this);
    }
}
