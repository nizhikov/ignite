package org.apache.ignite.internal.processors.transmit.chunk;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;
import org.apache.ignite.internal.processors.transmit.FileTarget;
import org.apache.ignite.internal.processors.transmit.stream.RemoteTransmitException;
import org.apache.ignite.internal.processors.transmit.stream.TransmitAbstractChannel;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
abstract class AbstractChunkedIo<T> implements ChunkedIo<T> {
    /** The default region size to transfer data. */
    public static final int DFLT_SEGMENT_SIZE = 1024 * 1024;

    /** The destination object to transfer data into. */
    protected final FileTarget<T> obj;

    /** The unique input name to identify particular transfer part.*/
    protected final String name;

    /** The position offest to start at. */
    protected final long position;

    /** The total number of bytes to send. */
    protected final long count;

    /** The size of segment for the read. */
    protected final int segmentSize;

    /** The number of bytes successfully transferred druring iteration. */
    protected long transferred;

    /**
     * @param obj The destination object to transfer into.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     * @param segmentSize The size of segmented the read.
     */
    protected AbstractChunkedIo(FileTarget<T> obj, String name, long position, long count, int segmentSize) {
        assert position >= 0 : "The file position must be non-negative";
        assert count >= 0 : "The number of bytes to sent must be positive";

        this.obj = Objects.requireNonNull(obj);
        this.name = Objects.requireNonNull(name);
        this.position = position;
        this.count = count;
        this.segmentSize = segmentSize;
    }

    /**
     * @param obj The destination object to transfer into.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     */
    protected AbstractChunkedIo(FileTarget<T> obj, String name, long position, long count) {
        this(obj, name, position, count, DFLT_SEGMENT_SIZE);
    }

    /** {@inheritDoc} */
    @Override public long postition() {
        return position;
    }

    /** {@inheritDoc} */
    @Override public long transferred() {
        return transferred;
    }

    /** {@inheritDoc} */
    @Override public long count() {
        return count;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /**
     * @param io The file object to check.
     * @throws IOException If the check fails.
     */
    public void checkChunkedIoEOF(ChunkedIo<?> io, TransmitAbstractChannel ch) throws IOException {
        if (io.transferred() < io.count()) {
            throw new RemoteTransmitException("The connection is lost. The file expected to be fully transferred, " +
                "but didn't [count=" + io.count() + ", transferred=" + io.transferred() +
                ", remoteId=" + ch.igniteSocket().id().remoteId() + ", index=" + ch.igniteSocket().id().idx() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public boolean endOfTransmit() {
        return transferred == count;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractChunkedIo.class, this);
    }
}
