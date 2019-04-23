package org.apache.ignite.internal.processors.transmit.chunk;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.transmit.FileTarget;
import org.apache.ignite.internal.processors.transmit.stream.TransmitInputChannel;
import org.apache.ignite.internal.processors.transmit.stream.TransmitOutputChannel;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class ChunkedBufferIo extends AbstractChunkedIo<ByteBuffer> {
    /**
     * @param buff The buff to read into.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     */
    public ChunkedBufferIo(FileTarget<ByteBuffer> buff, String name, long position, long count) {
        super(buff, name, position, count);
    }

    /** {@inheritDoc} */
    @Override public FileTarget<ByteBuffer> readFrom(TransmitInputChannel channel) throws IOException {
        long readed = channel.readInto(obj.target());

        if (readed > 0)
            transferred.add(readed);
        else if (readed < 0)
            checkChunkedIoEOF(this, channel);

        return obj;
    }

    /** {@inheritDoc} */
    @Override public void writeInto(TransmitOutputChannel channel) throws IOException {
        long written = channel.writeFrom(obj.target());

        if (written > 0)
            transferred.add(written);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {

    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChunkedBufferIo.class, this);
    }
}
