package org.apache.ignite.internal.util.nio.channel;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketOptions;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * A channel configuration for the {@link IgniteSocketChannel}.
 */
public final class IgniteSocketChannelConfig {
    /** */
    private final SocketChannel channel;

    /** */
    private final Socket socket;

    /**
     * @param channel The socket channel to create configuration from.
     */
    public IgniteSocketChannelConfig(SocketChannel channel) {
        this.channel = channel;
        this.socket = channel.socket();
    }

    /**
     * Gets the {@link AbstractSelectableChannel#isBlocking()} mode.
     */
    public boolean isBlocking() {
        return channel.isBlocking();
    }

    /**
     * Sets channel's blocking mode by {@link AbstractSelectableChannel#configureBlocking(boolean)} .
     */
    public IgniteSocketChannelConfig configureBlocking(boolean blocking) {
        try {
            channel.configureBlocking(blocking);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        return this;
    }

    /**
     * Gets the {@link SocketOptions#SO_TIMEOUT} option.
     */
    public int getSoTimeout() {
        try {
            return socket.getSoTimeout();
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Sets the {@link SocketOptions#SO_TIMEOUT} option.
     */
    public IgniteSocketChannelConfig setSoTimeout(int connectTimeoutMillis) {
        try {
            socket.setSoTimeout(connectTimeoutMillis);
        }
        catch (SocketException e) {
            throw new IgniteException(e);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteSocketChannelConfig.class, this);
    }
}
