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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.PromiseCombiner;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.cloudburstmc.netty.channel.proxy.ProxyChannel;
import org.cloudburstmc.netty.channel.raknet.config.DefaultRakServerConfig;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelOption;
import org.cloudburstmc.netty.channel.raknet.config.RakServerChannelConfig;
import org.cloudburstmc.netty.channel.raknet.config.RakServerCookieMode;
import org.cloudburstmc.netty.handler.codec.raknet.common.UnconnectedPongEncoder;
import org.cloudburstmc.netty.handler.codec.raknet.server.RakProxyServerHandler;
import org.cloudburstmc.netty.handler.codec.raknet.server.RakServerOfflineHandler;
import org.cloudburstmc.netty.handler.codec.raknet.server.RakServerRateLimiter;
import org.cloudburstmc.netty.handler.codec.raknet.server.RakServerRouteHandler;
import org.cloudburstmc.netty.handler.codec.raknet.server.RakServerTailHandler;
import org.cloudburstmc.netty.util.RakUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RakServerChannel extends ProxyChannel<DatagramChannel> implements ServerChannel {

    private static final InternalLogger log = InternalLoggerFactory.getInstance(RakServerChannel.class);

    private final RakServerChannelConfig config;
    private final Map<SocketAddress, RakChildChannel> childChannelMap = new ConcurrentHashMap<>();
    private final Set<RakChildChannel> childrenPendingCleanup = ConcurrentHashMap.newKeySet();
    private final Consumer<RakChannel> childConsumer;

    private boolean pipelineInitialized;
    private ExpiringMap<InetSocketAddress, InetSocketAddress> clientAddresses = null;

    public RakServerChannel(DatagramChannel channel) {
        this(channel, null);
    }

    public RakServerChannel(DatagramChannel channel, Consumer<RakChannel> childConsumer) {
        super(channel);
        this.childConsumer = childConsumer;
        this.config = new DefaultRakServerConfig(this);

        channel.closeFuture().addListener(future -> {
            if (!future.isSuccess()) {
                log.warn("RakServerChannel closed unsuccessfully", future.cause());
            } else if (future.cause() != null) {
                log.warn("RakServerChannel closed with cause", future.cause());
            }
        });
    }

    @Override
    public ChannelPipeline pipeline() {
        if (!this.pipelineInitialized) {
            this.pipelineInitialized = true;
            initPipeline();
        }
        return super.pipeline();
    }

    protected void initPipeline() {
        this.clientAddresses = this.config().getProxyProtocol()
                ? ExpiringMap.builder()
                  .expiration(RakConstants.SESSION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                  .expirationPolicy(ExpirationPolicy.ACCESSED).build()
                : null;
        if (this.config().getProxyProtocol()) {
            this.pipeline().addLast(RakProxyServerHandler.NAME, new RakProxyServerHandler(this));
        }
        this.pipeline().addLast(UnconnectedPongEncoder.NAME, UnconnectedPongEncoder.INSTANCE);
        if (this.config().getPacketLimit() > 0) { // No point in enabling this.
            this.pipeline().addLast(RakServerRateLimiter.NAME, new RakServerRateLimiter(this));
        }
        this.pipeline().addLast(RakServerOfflineHandler.NAME, new RakServerOfflineHandler(this));
        this.pipeline().addLast(RakServerRouteHandler.NAME, new RakServerRouteHandler(this));
        this.pipeline().addLast(RakServerTailHandler.NAME, RakServerTailHandler.INSTANCE);
    }

    /**
     * Create new child channel assigned to remote address.
     *
     * @param address         remote address of new connection.
     * @param protocolVersion RakNet protocol version from the handshake cookie, or 0 if not available.
     * @return RakChildChannel instance of new channel, or {@code null} if a non-replaceable channel already exists.
     */
    public RakChildChannel createChildChannel(InetSocketAddress address, InetSocketAddress localAddress, long clientGuid, int mtu, int protocolVersion) {
        RakChildChannel existingChannel = this.childChannelMap.get(address);
        if (this.config().getCookieMode() != RakServerCookieMode.INVALID &&
                this.config().getCookieMode() != RakServerCookieMode.OFF && existingChannel != null) {
            // We know this player is coming from this IP address due to the cookie, so we can safely close the existing channel.
            existingChannel.close();
        } else if (existingChannel != null) {
            // Could be spoofed, so we don't close the existing channel.
            return null;
        }

        InetSocketAddress clientAddress = this.getClientAddress(address);
        if (this.config().getThrottle() != null && !this.config().getThrottle().accept(clientAddress)) {
            return null;
        }

        RakChildChannel channel = new RakChildChannel(address, localAddress, clientAddress, this, clientGuid, mtu, childConsumer);
        // A cookie-authenticated replacement displaces the old address mapping before its asynchronous close
        // cleanup necessarily reaches the parent loop. Keep cleanup ownership independent of address identity so
        // parent shutdown can wait for both the replacement and every displaced child.
        this.childrenPendingCleanup.add(channel);
        channel.closeFuture().addListener((GenericFutureListener<ChannelFuture>) this::onChildClosed);
        // Set before fireChannelRead because initChannel runs async on the child worker thread.
        if (protocolVersion != 0) {
            channel.config().setOption(RakChannelOption.RAK_PROTOCOL_VERSION, protocolVersion);
        }
        // Fire channel thought ServerBootstrap,
        // register to eventLoop, assign default options and attributes
        this.pipeline().fireChannelRead(channel).fireChannelReadComplete();
        this.childChannelMap.put(address, channel);

        if (this.config().getMetrics() != null) {
            this.config().getMetrics().channelOpen(clientAddress);
        }
        return channel;
    }

    public RakChildChannel getChildChannel(SocketAddress address) {
        return this.childChannelMap.get(address);
    }

    private void onChildClosed(ChannelFuture channelFuture) {
        RakChildChannel channel = (RakChildChannel) channelFuture.channel();
        if (!this.eventLoop().inEventLoop()) {
            // Serialize teardown after the child's terminal parent-handoff drain. This prevents a retained message
            // already owned by the parent event loop from entering a pipeline destroyed on the child event loop.
            try {
                this.eventLoop().execute(() -> this.finishChildCloseAndComplete(channel));
            } catch (Throwable throwable) {
                // Rejection means the parent loop is terminating. Its termination is the final ownership fence for
                // any drain task that was already running when the child closed. Parent-backed pipeline events can
                // no longer be scheduled, so release the two stateful handlers directly after that fence.
                this.eventLoop().terminationFuture().addListener(ignored ->
                        this.finishChildCloseAfterParentTermination(channel, throwable));
            }
            return;
        }
        this.finishChildCloseAndComplete(channel);
    }

    private void finishChildCloseAndComplete(RakChildChannel channel) {
        Throwable failure = null;
        try {
            failure = this.finishChildClose(channel);
            if (failure != null) {
                log.error("Failed to clean up RakNet child {}", channel.remoteAddress(), failure);
            }
        } catch (Throwable unexpected) {
            failure = unexpected;
            log.error("Unexpected failure cleaning up RakNet child {}", channel.remoteAddress(), unexpected);
        } finally {
            // Completed-before-remove is the ordering guarantee used by onCloseTriggered: a parent snapshot either
            // observes this child and its promise, or observes that cleanup has already completed.
            try {
                channel.completeParentCleanup(failure);
            } finally {
                this.childrenPendingCleanup.remove(channel);
            }
        }
    }

    private Throwable finishChildClose(RakChildChannel channel) {
        Throwable failure = null;
        this.childChannelMap.remove(channel.remoteOrProxyAddress(), channel);

        if (this.config().getMetrics() != null) {
            try {
                this.config().getMetrics().channelClose(channel.remoteAddress());
            } catch (Throwable throwable) {
                failure = appendFailure(failure, throwable);
            }
        }

        try {
            channel.rakPipeline().fireChannelInactive();
        } catch (Throwable throwable) {
            failure = appendFailure(failure, throwable);
        }
        try {
            channel.rakPipeline().fireChannelUnregistered();
        } catch (Throwable throwable) {
            failure = appendFailure(failure, throwable);
        }
        try {
            // Need to use reflection to destroy pipeline because
            // DefaultChannelPipeline.destroy() is only called when channel.isOpen() is false,
            // but the method is called on parent channel, and there is no other way to destroy pipeline.
            RakUtils.destroyChannelPipeline(channel.rakPipeline());
        } catch (Throwable throwable) {
            failure = appendFailure(failure, throwable);
        }

        if (this.config().getThrottle() != null) {
            try {
                this.config().getThrottle().closed(channel.remoteAddress());
            } catch (Throwable throwable) {
                failure = appendFailure(failure, throwable);
            }
        }
        return failure;
    }

    private void finishChildCloseAfterParentTermination(RakChildChannel channel, Throwable schedulingFailure) {
        Throwable failure = schedulingFailure;
        this.childChannelMap.remove(channel.remoteOrProxyAddress(), channel);
        try {
            channel.releaseRakNetResourcesAfterParentTermination();
        } catch (Throwable throwable) {
            failure = appendFailure(failure, throwable);
        }
        if (this.config().getMetrics() != null) {
            try {
                this.config().getMetrics().channelClose(channel.remoteAddress());
            } catch (Throwable throwable) {
                failure = appendFailure(failure, throwable);
            }
        }
        if (this.config().getThrottle() != null) {
            try {
                this.config().getThrottle().closed(channel.remoteAddress());
            } catch (Throwable throwable) {
                failure = appendFailure(failure, throwable);
            }
        }
        if (failure != schedulingFailure || failure.getSuppressed().length > 0) {
            log.error("Failed terminal RakNet child cleanup for {}", channel.remoteAddress(), failure);
        }
        try {
            channel.completeParentCleanup(failure);
        } finally {
            this.childrenPendingCleanup.remove(channel);
        }
    }

    @Override
    public void onCloseTriggered(ChannelPromise promise) {
        if (log.isTraceEnabled()) {
            log.trace("Closing RakServerChannel: {}", Thread.currentThread().getName(), new Throwable());
        }
        PromiseCombiner combiner = new PromiseCombiner(this.eventLoop());
        // The parent-backed session pipeline must be inactive and destroyed before the parent event loop is allowed
        // to terminate. Waiting only for each child closeFuture leaves that cleanup task behind on the parent tail.
        new ArrayList<>(this.childrenPendingCleanup).forEach(channel -> {
            combiner.add(channel.parentCleanupFuture());
            channel.close();
        });

        ChannelPromise combinedPromise = this.newPromise();
        combinedPromise.addListener(future -> super.onCloseTriggered(promise));
        combiner.finish(combinedPromise);
    }

    private static Throwable appendFailure(Throwable existing, Throwable additional) {
        if (existing == null) {
            return additional;
        }
        existing.addSuppressed(additional);
        return existing;
    }

    public boolean tryBlockAddress(InetSocketAddress address, long time, TimeUnit unit) {
        RakServerRateLimiter rateLimiter = this.pipeline().get(RakServerRateLimiter.class);
        if (rateLimiter != null) {
            return rateLimiter.blockAddress(address, time, unit);
        }
        return false;
    }

    @Override
    public RakServerChannelConfig config() {
        return this.config;
    }

    public InetSocketAddress getClientAddress(InetSocketAddress address) {
        return this.clientAddresses != null ? this.clientAddresses.get(address) : address;
    }

    public void setClientAddress(InetSocketAddress address, InetSocketAddress clientAddress) {
        this.clientAddresses.put(address, clientAddress);
    }
}
