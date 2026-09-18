/*
 * Copyright 2022 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.cloudburstmc.netty.channel.raknet;

import io.netty.channel.*;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.cloudburstmc.netty.channel.raknet.config.DefaultChannelToServerProxyMetrics;
import org.cloudburstmc.netty.channel.raknet.config.DefaultRakSessionConfig;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelConfig;
import org.cloudburstmc.netty.handler.codec.raknet.common.*;
import org.cloudburstmc.netty.handler.codec.raknet.server.RakChildDatagramHandler;
import org.cloudburstmc.netty.handler.codec.raknet.server.RakServerOnlineInitialHandler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.util.function.Consumer;

public class RakChildChannel extends AbstractChannel implements RakChannel {

    private static final InternalLogger log = InternalLoggerFactory.getInstance(RakChildChannel.class);
    private static final ChannelMetadata metadata = new ChannelMetadata(true);
    // User-defined writability indexes are 1..31. Reserve the last index for the internal parent-loop handoff.
    private static final int PARENT_HANDOFF_WRITABILITY_INDEX = 31;

    private final RakChannelConfig config;
    private final InetSocketAddress remoteAddress;
    private final InetSocketAddress localAddress;
    private final InetSocketAddress clientAddress;
    private final DefaultChannelPipeline rakPipeline;
    private final RakChildWriteHandoff writeHandoff;
    private final Promise<Void> parentCleanupPromise;
    private volatile boolean open = true;
    private volatile boolean active;

    RakChildChannel(InetSocketAddress remoteAddress, InetSocketAddress localAddress, InetSocketAddress clientAddress, RakServerChannel parent, long guid, int mtu, Consumer<RakChannel> childConsumer) {
        super(parent);
        this.remoteAddress = remoteAddress;
        this.localAddress = localAddress;
        this.clientAddress = clientAddress;
        this.config = new DefaultRakSessionConfig(this, new DefaultChannelToServerProxyMetrics(parent, this));
        this.config.setGuid(guid);
        this.config.setMtu(mtu);
        this.parentCleanupPromise = parent.eventLoop().newPromise();
        // Allow user to configure the child channel before we initialize pipeline
        // This is not the same as bootstrap.childOption() as Bootstrap does not allow setting options per channel
        if (childConsumer != null) {
            childConsumer.accept(this);
        }
        // Create an internal pipeline for RakNet session logic to take place. We use the parent channel to ensure
        // this all occurs on the parent event loop so the connection is not slowed down by any user code.
        // (compression, encryption, etc.)
        this.rakPipeline = new RakChannelPipeline(parent, this);
        this.rakPipeline.addLast(RakChildDatagramHandler.NAME, new RakChildDatagramHandler(this));

        // Setup session/online phase
        RakSessionCodec sessionCodec = new RakSessionCodec(this);
        this.rakPipeline.addLast(RakDatagramCodec.NAME, new RakDatagramCodec());
        this.rakPipeline.addLast(RakAcknowledgeHandler.NAME, new RakAcknowledgeHandler(sessionCodec));
        this.rakPipeline.addLast(RakSessionCodec.NAME, sessionCodec);
        // This handler auto-removes once ConnectionRequest is received
        this.rakPipeline.addLast(ConnectedPingHandler.NAME, new ConnectedPingHandler());
        this.rakPipeline.addLast(ConnectedPongHandler.NAME, new ConnectedPongHandler(sessionCodec));
        this.rakPipeline.addLast(DisconnectNotificationHandler.NAME, DisconnectNotificationHandler.INSTANCE);
        this.rakPipeline.addLast(RakServerOnlineInitialHandler.NAME, new RakServerOnlineInitialHandler(this));
        this.rakPipeline.addLast(RakUnhandledMessagesQueue.NAME, new RakUnhandledMessagesQueue(this));
        this.writeHandoff = new RakChildWriteHandoff(parent.eventLoop(), message -> {
            this.rakPipeline.write(message, parent.voidPromise());
            return null;
        }, this.rakPipeline::flush,
                () -> this.open && this.active && parent.isOpen(),
                () -> this.config.getWriteBufferLowWaterMark(),
                () -> this.config.getWriteBufferHighWaterMark(),
                () -> this.config.getMaxQueuedBytes(),
                this::setParentHandoffWritable,
                this::handleRakWriteFailure);
        this.rakPipeline.fireChannelRegistered();
        this.rakPipeline.fireChannelActive();
    }

    @Override
    public ChannelPipeline rakPipeline() {
        return rakPipeline;
    }

    @Override
    public SocketAddress localAddress0() {
        return this.localAddress;
    }

    @Override
    public SocketAddress remoteAddress0() {
        return this.clientAddress;
    }

    @Override
    public InetSocketAddress localAddress() {
        return (InetSocketAddress) super.localAddress();
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return (InetSocketAddress) super.remoteAddress();
    }

    public InetSocketAddress remoteOrProxyAddress() {
        return remoteAddress;
    }

    @Override
    public RakChannelConfig config() {
        return this.config;
    }

    @Override
    public ChannelMetadata metadata() {
        return metadata;
    }

    @Override
    protected void doBind(SocketAddress socketAddress) throws Exception {
        throw new UnsupportedOperationException("Can not bind child channel!");
    }

    @Override
    protected void doBeginRead() throws Exception {
        // Ignore
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        if (!this.open) {
            throw new ClosedChannelException();
        } else if (!active) {
            throw new NonWritableChannelException();
        }
        if (this.parent().eventLoop().inEventLoop()) {
            this.writeDirect(in);
            return;
        }
        ClosedChannelException exception = null;
        for (; ; ) {
            Object msg = in.current();
            if (msg == null) {
                break;
            }
            try {
                if (this.parent().isOpen()) {
                    this.writeHandoff.enqueue(msg);
                    in.remove();
                } else {
                    if (exception == null) {
                        exception = new ClosedChannelException();
                    }
                    in.remove(exception);
                }
            } catch (Throwable cause) {
                in.remove(cause);
            }
        }
    }

    private void writeDirect(ChannelOutboundBuffer in) {
        ClosedChannelException exception = null;
        for (; ; ) {
            Object msg = in.current();
            if (msg == null) {
                break;
            }
            try {
                if (this.parent().isOpen()) {
                    this.rakPipeline.write(ReferenceCountUtil.retain(msg), this.parent().voidPromise());
                    in.remove();
                } else {
                    if (exception == null) {
                        exception = new ClosedChannelException();
                    }
                    in.remove(exception);
                }
            } catch (Throwable cause) {
                in.remove(cause);
            }
        }
        this.rakPipeline.flush();
    }

    private void handleRakWriteFailure(Throwable cause) {
        log.error("Parent-event-loop write failed for {}, closing", this.remoteAddress(), cause);
        this.pipeline().fireExceptionCaught(cause);
        this.close();
    }

    @Override
    public int pendingRakNetOutboundBytes() {
        return this.writeHandoff.pendingBytes();
    }

    private void setParentHandoffWritable(boolean writable) {
        ChannelOutboundBuffer outboundBuffer = this.unsafe().outboundBuffer();
        if (outboundBuffer != null) {
            outboundBuffer.setUserDefinedWritability(PARENT_HANDOFF_WRITABILITY_INDEX, writable);
        }
    }

    Future<Void> parentCleanupFuture() {
        return this.parentCleanupPromise;
    }

    void completeParentCleanup(Throwable cause) {
        if (cause == null) {
            this.parentCleanupPromise.trySuccess(null);
        } else {
            this.parentCleanupPromise.tryFailure(cause);
        }
    }

    void releaseRakNetResourcesAfterParentTermination() {
        Throwable failure = null;
        RakSessionCodec sessionCodec = this.rakPipeline.get(RakSessionCodec.class);
        if (sessionCodec != null) {
            try {
                sessionCodec.closeAfterEventLoopTermination();
            } catch (Throwable throwable) {
                failure = throwable;
            }
        }
        RakUnhandledMessagesQueue unhandled = this.rakPipeline.get(RakUnhandledMessagesQueue.class);
        if (unhandled != null) {
            try {
                unhandled.closeAfterEventLoopTermination();
            } catch (Throwable throwable) {
                if (failure == null) {
                    failure = throwable;
                } else {
                    failure.addSuppressed(throwable);
                }
            }
        }
        if (failure != null) {
            throw new ChannelException("Failed to release RakNet child resources after parent termination", failure);
        }
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    protected void doDisconnect() throws Exception {
        this.close();
    }

    @Override
    protected void doClose() throws Exception {
        this.open = false;
        this.writeHandoff.close();
    }

    @Override
    public boolean isActive() {
        return this.isOpen() && this.active;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    protected boolean isCompatible(EventLoop eventLoop) {
        return true;
    }

    @Override
    protected AbstractUnsafe newUnsafe() {
        return new AbstractUnsafe() {
            @Override
            public void connect(SocketAddress socketAddress, SocketAddress socketAddress1, ChannelPromise channelPromise) {
                throw new UnsupportedOperationException("Can not connect child channel!");
            }
        };
    }
}
