package org.apache.ignite.internal.processors.transmit.chunk;

import java.io.File;
import java.io.IOException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.transmit.stream.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.stream.TransmitOutputChannel;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class ChunkedFileIo extends AbstractChunkedIo<File> {
    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param fileCfg The java {@link File} representation which should be transfered.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     */
    public ChunkedFileIo(File fileCfg, String name, long position, long count) {
        super(fileCfg, name, position, count);
    }

    /**
     * Explicitly open the underlying file if not done yet.
     */
    public void open() throws IOException {
        if (fileIo == null)
            fileIo = dfltIoFactory.create(obj);
    }

    /** {@inheritDoc} */
    @Override public File readFrom(TransmitInputChannel channel) throws IOException {
        open();

        long batchSize = Math.min(segmentSize, count - transferred);

        long readed = channel.readInto(fileIo, position + transferred, batchSize);

        if (readed > 0)
            transferred += readed;
        else if (readed < 0)
            checkTransferEOF();

        return obj;
    }

    /** {@inheritDoc} */
    @Override public void writeInto(TransmitOutputChannel channel) throws IOException {
        open();

        long batchSize = Math.min(segmentSize, count - transferred);

        long sent = channel.writeFrom(position + transferred, batchSize, fileIo);

        if (sent > 0)
            transferred += sent;
        else if (sent < 0)
            checkTransferEOF();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        U.closeQuiet(fileIo);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChunkedFileIo.class, this);
    }
}
