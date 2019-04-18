package org.apache.ignite.internal.processors.transfer;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
class SegmentedBufferIo extends SegmentedAbstractIo<ByteBuffer> {
    /**
     * @param buff The buff to read into.
     * @param name The unique file name within transfer process.
     * @param position The position from which the transfer should start to.
     * @param count The number of bytes to expect of transfer.
     */
    public SegmentedBufferIo(ByteBuffer buff, String name, long position, long count) {
        super(buff, name, position, count);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer readFrom(FileIoChannel channel) throws IOException {
        long readed = channel.readInto(obj);

        if (readed > 0)
            transferred += readed;
        else if (readed < 0)
            checkTransferEOF();

        return obj;
    }

    /** {@inheritDoc} */
    @Override public void writeInto(FileIoChannel channel) throws IOException {
        long written = channel.writeFrom(obj);

        if (written > 0)
            transferred += written;
        else if (written < 0)
            checkTransferEOF();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {

    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SegmentedBufferIo.class, this);
    }
}
