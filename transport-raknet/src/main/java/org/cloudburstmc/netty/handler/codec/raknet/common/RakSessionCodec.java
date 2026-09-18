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

package org.cloudburstmc.netty.handler.codec.raknet.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.cloudburstmc.netty.channel.raknet.*;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelConfig;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelMetrics;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelOption;
import org.cloudburstmc.netty.channel.raknet.config.RakDatagramSendType;
import org.cloudburstmc.netty.channel.raknet.packet.EncapsulatedPacket;
import org.cloudburstmc.netty.channel.raknet.packet.RakDatagramPacket;
import org.cloudburstmc.netty.channel.raknet.packet.RakMessage;
import org.cloudburstmc.netty.util.*;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ObjIntConsumer;

import static org.cloudburstmc.netty.channel.raknet.RakConstants.*;

public class RakSessionCodec extends ChannelDuplexHandler {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(RakSessionCodec.class);
    public static final String NAME = "rak-session-codec";

    private final RakChannel channel;
    private final LongSupplier clock;
    private RakSessionTicker.Registration tickRegistration;

    private volatile RakState state;

    private volatile long lastTouched;
    private volatile long lastFlush;

    // Reliability, Ordering, Sequencing and datagram indexes
    private RakSlidingWindow slidingWindow;
    private RakSlidingWindow.ModelSendState originalSendRollbackState;
    private RakBoundedRecovery boundedRecovery;
    private Queue<PendingRetransmission> pendingRetransmissions;
    private long datagramSendOrdinal;
    private int splitIndex;
    private int datagramReadIndex;
    private int datagramWriteIndex;
    private int reliabilityReadIndex;
    private int reliabilityWriteIndex;
    private int[] orderReadIndex;
    private int[] orderWriteIndex;

    private RoundRobinArray<SplitPacketHelper> splitPackets;
    private BitQueue reliableDatagramQueue;

    private FastWeightedFairQueue<EncapsulatedPacket> outgoingPackets;
    private long[] outgoingPacketNextWeights;
    private FastBinaryMinHeap<EncapsulatedPacket>[] orderingHeaps;
    private long currentPingTime = -1;
    private long lastPingTime = -1;
    private long lastPongTime = -1;
    private IntObjectMap<RakDatagramPacket> sentDatagrams;
    private IntObjectMap<RakSlidingWindow.ModelDatagramSample> modelDatagramSamples;
    private Queue<ModelSampleExpiry> modelSampleExpiries;
    private Queue<IntRange> incomingAcks;
    private Queue<IntRange> incomingNaks;
    private Queue<IntRange> outgoingAcks;
    private Queue<IntRange> outgoingNaks;

    private int queuedBytes = 0;
    private boolean terminalDisconnectWrite;
    private final RakRecoveryMetrics recoveryMetrics = new RakRecoveryMetrics();
    // Payload bytes retained by inbound split reassembly and ordered-reorder buffering respectively.
    private long splitQueuedBytes = 0;
    private long orderingQueuedBytes = 0;

    public RakSessionCodec(RakChannel channel) {
        this(channel, System::currentTimeMillis);
    }

    RakSessionCodec(RakChannel channel, LongSupplier clock) {
        this.channel = channel;
        this.clock = clock;
        this.lastTouched = clock.getAsLong();
        this.setState(RakState.UNCONNECTED);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.setState(RakState.CONNECTED);
        int mtu = this.getMtu();

        int flushInterval = captureFlushInterval(this.channel.config());
        this.slidingWindow = new RakSlidingWindow(mtu, flushInterval);
        this.originalSendRollbackState = this.slidingWindow.newModelSendState();
        this.boundedRecovery = new RakBoundedRecovery(this.clock);
        this.recoveryMetrics.initialize(this.getMetrics(), this.slidingWindow, this.currentTimeMillis());

        this.outgoingPacketNextWeights = new long[RakPriority.values().length];
        this.initOutgoingPacketWeights();

        int maxChannels = this.channel.config().getOption(RakChannelOption.RAK_ORDERING_CHANNELS);
        this.orderReadIndex = new int[maxChannels];
        this.orderWriteIndex = new int[maxChannels];

        // Noinspection unchecked
        this.orderingHeaps = new FastBinaryMinHeap[maxChannels];
        for (int i = 0; i < maxChannels; i++) {
            orderingHeaps[i] = new FastBinaryMinHeap<>(64);
        }

        this.outgoingPackets = new FastWeightedFairQueue<>(RakPriority.values().length);
        this.sentDatagrams = new IntObjectHashMap<>();
        this.modelDatagramSamples = new IntObjectHashMap<>();
        this.modelSampleExpiries = new PriorityQueue<>();

        this.incomingAcks = new ArrayDeque<>();
        this.incomingNaks = new ArrayDeque<>();
        this.outgoingAcks = new ArrayDeque<>();
        this.outgoingNaks = new ArrayDeque<>();
        this.pendingRetransmissions = new ArrayDeque<>();

        this.reliableDatagramQueue = new BitQueue(512);
        this.splitPackets = new RoundRobinArray<>(256);

        // After session is fully initialized, start the configured auto-flush cadence or the 10 ms maintenance tick.
        this.tickRegistration = RakSessionTicker.register(
                ctx.channel().eventLoop(), flushInterval, this::tryTick);

        ctx.fireChannelActive(); // fire channel active on rakPipeline()
    }

    static int captureFlushInterval(RakChannelConfig config) {
        // channelActive captures one value for both the fixed-rate task and controller. Later option changes do not
        // reschedule the task and therefore must not change the controller's accounting quantum either.
        return config.isAutoFlush() ? config.getFlushInterval() : 10;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        this.closeSession();
    }

    /** Releases session-owned state after the parent event loop has terminated and can no longer fire lifecycle. */
    public void closeAfterEventLoopTermination() {
        this.closeSession();
    }

    private void closeSession() {
        if (this.state == RakState.DISCONNECTED && this.tickRegistration == null) {
            // Already deinitialized
            return;
        }
        Throwable failure = null;
        try {
            this.setState(RakState.DISCONNECTED);
        } catch (Throwable throwable) {
            failure = throwable;
        }
        if (this.tickRegistration != null) {
            try {
                this.tickRegistration.cancel();
            } catch (Throwable throwable) {
                failure = appendFailure(failure, throwable);
            } finally {
                this.tickRegistration = null;
            }
        }

        try {
            RakChannelMetrics metrics = this.getMetrics();
            if (metrics != null && this.slidingWindow != null) {
                this.recoveryMetrics.close(metrics, this.slidingWindow, this.currentTimeMillis());
            }
        } catch (Throwable throwable) {
            failure = appendFailure(failure, throwable);
        } finally {
            try {
                this.releaseSessionResources();
            } catch (Throwable throwable) {
                failure = appendFailure(failure, throwable);
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("RakNet Session ({} => {}) closed!", this.channel.localAddress(), this.getRemoteAddress());
        }
        if (failure != null) {
            throwUnchecked(failure);
        }
    }

    private void releaseSessionResources() {
        if (this.boundedRecovery != null) {
            this.boundedRecovery.close();
            this.boundedRecovery = null;
        }
        if (this.pendingRetransmissions != null) {
            this.pendingRetransmissions.clear();
            this.pendingRetransmissions = null;
        }

        RoundRobinArray<SplitPacketHelper> splitPackets = this.splitPackets;
        this.splitPackets = null;
        if (splitPackets != null) {
            for (SplitPacketHelper helper : splitPackets) {
                if (helper != null) {
                    helper.release();
                }
            }
        }

        IntObjectMap<RakDatagramPacket> sentDatagrams = this.sentDatagrams;
        this.sentDatagrams = null;
        if (sentDatagrams != null) {
            for (RakDatagramPacket packet : sentDatagrams.values()) {
                packet.release();
            }
        }
        if (this.modelDatagramSamples != null) {
            this.modelDatagramSamples.clear();
            this.modelDatagramSamples = null;
        }
        if (this.modelSampleExpiries != null) {
            this.modelSampleExpiries.clear();
            this.modelSampleExpiries = null;
        }

        FastBinaryMinHeap<EncapsulatedPacket>[] orderingHeaps = this.orderingHeaps;
        this.orderingHeaps = null;
        if (orderingHeaps != null) {
            for (FastBinaryMinHeap<EncapsulatedPacket> orderingHeap : orderingHeaps) {
                EncapsulatedPacket packet;
                while ((packet = orderingHeap.poll()) != null) {
                    packet.release();
                }
                orderingHeap.release();
            }
        }

        FastWeightedFairQueue<EncapsulatedPacket> outgoingPackets = this.outgoingPackets;
        this.outgoingPackets = null;
        if (outgoingPackets != null) {
            EncapsulatedPacket packet;
            while ((packet = outgoingPackets.poll()) != null) {
                packet.release();
            }
            outgoingPackets.release();
        }

        this.queuedBytes = 0;
        this.splitQueuedBytes = 0;
        this.orderingQueuedBytes = 0;
        if (this.slidingWindow != null) {
            this.slidingWindow.close();
        }
        if (log.isTraceEnabled()) {
            log.trace("RakNet Session ({} => {}) closed!", this.channel.localAddress(), this.getRemoteAddress());
        }
        this.originalSendRollbackState = null;
    }

    private void initOutgoingPacketWeights() {
        for (int priorityLevel = 0; priorityLevel < this.outgoingPacketNextWeights.length; priorityLevel++) {
            this.outgoingPacketNextWeights[priorityLevel] = (1 << priorityLevel) * priorityLevel + priorityLevel;
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!this.channel.parent().eventLoop().inEventLoop()) {
            // Make sure this runs on correct thread
            log.error("Tried to write packet from wrong thread: {}", Thread.currentThread().getName(), new Throwable());
            final Object finalMsg = msg;
            this.channel.parent().eventLoop().execute(() -> this.write(ctx, finalMsg, promise));
            return;
        }
        if (msg instanceof ByteBuf) {
            msg = new RakMessage((ByteBuf) msg);
        } else if (!(msg instanceof RakMessage)) {
            throw new IllegalArgumentException("Message must be a ByteBuf or RakMessage");
        }

        try {
            this.send(ctx, (RakMessage) msg);
            promise.setSuccess(null);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (!this.channel.config().isAutoFlush()) {
            this.internalFlush(ctx);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            if (!(msg instanceof RakDatagramPacket)) {
                // We don't want to let anything through that isn't RakNet related.
                return;
            }
            RakDatagramPacket packet = (RakDatagramPacket) msg;
            if (this.state == RakState.UNCONNECTED) {
                log.debug("{} received message from inactive channel: {}", this.getRemoteAddress(), packet);
            } else {
                this.handleDatagram(ctx, packet);
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        this.disconnect0(RakDisconnectReason.DISCONNECTED).addListener(future -> {
            if (future.cause() == null) {
                promise.trySuccess();
            } else {
                promise.tryFailure(future.cause());
            }
        });
    }

    private void send(ChannelHandlerContext ctx, RakMessage message) {
        if (this.state == RakState.UNCONNECTED) {
            throw new IllegalStateException("Can not send RakMessage to inactive channel");
        }

        int packetId = message.content().getUnsignedByte(message.content().readerIndex());
        if (packetId == 0xc0) {
            throw new IllegalArgumentException();
        }

        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null) {
            metrics.encapsulatedOut(1);
        }
        EncapsulatedPacket[] packets = this.createEncapsulated(message);
        boolean terminalDisconnect = this.terminalDisconnectWrite
                && packetId == ID_DISCONNECTION_NOTIFICATION;
        if (message.priority() == RakPriority.IMMEDIATE && terminalDisconnect) {
            // disconnect0 closes the session from this write's success listener, while ordinary write promises mean
            // "accepted by the session". This one terminal control datagram must therefore be handed to the channel
            // before success; the session closes immediately, so it cannot become a sustained cwnd bypass.
            this.sendImmediate(ctx, packets);
            return;
        }

        this.queueOutgoingPackets(packets, message.priority());
        if (message.priority() == RakPriority.IMMEDIATE) {
            // Immediate traffic requests a flush but remains inside the shared congestion window and pacer.
            this.internalFlush(ctx);
        }
    }

    private void queueOutgoingPackets(EncapsulatedPacket[] packets, RakPriority priority) {
        int priorityLevel = priority.ordinal();
        for (EncapsulatedPacket packet : packets) {
            long weight = this.getNextWeight(priority);
            this.outgoingPackets.insert(weight, priorityLevel, packet);
            this.queuedBytes += packet.getBuffer().readableBytes();
        }
    }

    private void handleDatagram(ChannelHandlerContext ctx, RakDatagramPacket packet) {
        this.touch();
        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null) {
            metrics.rakDatagramsIn(1);
        }

        this.slidingWindow.onPacketReceived(packet.getSendTime());

        int prevSequenceIndex = this.datagramReadIndex;
        if (prevSequenceIndex <= packet.getSequenceIndex()) {
            this.datagramReadIndex = packet.getSequenceIndex() + 1;
        }

        int missedDatagrams = packet.getSequenceIndex() - prevSequenceIndex;
        if (missedDatagrams > 0) {
            this.outgoingNaks.offer(new IntRange(packet.getSequenceIndex() - missedDatagrams, packet.getSequenceIndex() - 1));
        }

        this.outgoingAcks.offer(new IntRange(packet.getSequenceIndex(), packet.getSequenceIndex()));

        for (final EncapsulatedPacket encapsulated : packet.getPackets()) {
            if (encapsulated.getReliability().isReliable()) {
                int missed = encapsulated.getReliabilityIndex() - this.reliabilityReadIndex;
                if (missed > 0) {
                    if (missed < this.reliableDatagramQueue.size()) {
                        if (this.reliableDatagramQueue.get(missed)) {
                            this.reliableDatagramQueue.set(missed, false);
                        } else {
                            // Duplicate packet
                            continue;
                        }
                    } else {
                        int count = (missed - this.reliableDatagramQueue.size());
                        for (int i = 0; i < count; i++) {
                            this.reliableDatagramQueue.add(true);
                        }

                        this.reliableDatagramQueue.add(false);
                    }
                } else if (missed == 0) {
                    this.reliabilityReadIndex++;
                    if (!this.reliableDatagramQueue.isEmpty()) {
                        this.reliableDatagramQueue.poll();
                    }
                } else {
                    // Duplicate packet
                    continue;
                }

                while (!this.reliableDatagramQueue.isEmpty() && !this.reliableDatagramQueue.peek()) {
                    this.reliableDatagramQueue.poll();
                    ++this.reliabilityReadIndex;
                }
            }

            if (encapsulated.isSplit()) {
                final EncapsulatedPacket reassembled = this.getReassembledPacket(encapsulated, ctx.alloc());
                if (reassembled == null) {
                    // Not reassembled
                    continue;
                }
                if (metrics != null) {
                    metrics.encapsulatedIn(1);
                }
                try {
                    this.checkForOrdered(ctx, reassembled);
                } finally {
                    reassembled.release();
                }
            } else {
                if (metrics != null) {
                    metrics.encapsulatedIn(1);
                }
                this.checkForOrdered(ctx, encapsulated);
            }
        }
    }

    private void checkForOrdered(ChannelHandlerContext ctx, EncapsulatedPacket packet) {
        if (packet.getReliability().isOrdered()) {
            this.onOrderedReceived(ctx, packet);
        } else {
            ctx.fireChannelRead(packet.retain());
        }
    }

    private void onOrderedReceived(ChannelHandlerContext ctx, EncapsulatedPacket packet) {
        FastBinaryMinHeap<EncapsulatedPacket> binaryHeap = this.orderingHeaps[packet.getOrderingChannel()];
        if (this.orderReadIndex[packet.getOrderingChannel()] < packet.getOrderingIndex()) {
            // Not next in line so add to queue.
            binaryHeap.insert(packet.getOrderingIndex(), packet.retain());
            this.orderingQueuedBytes += packet.getBuffer().readableBytes();
            return;
        } else if (this.orderReadIndex[packet.getOrderingChannel()] > packet.getOrderingIndex()) {
            // We already have this
            return;
        }
        this.orderReadIndex[packet.getOrderingChannel()]++;

        // Can be handled
        ctx.fireChannelRead(packet.retain());

        EncapsulatedPacket queuedPacket;
        while ((queuedPacket = binaryHeap.peek()) != null) {
            if (queuedPacket.getOrderingIndex() == this.orderReadIndex[packet.getOrderingChannel()]) {
                try {
                    // We got the expected packet
                    binaryHeap.remove();
                    this.orderingQueuedBytes -= queuedPacket.getBuffer().readableBytes();
                    this.orderReadIndex[packet.getOrderingChannel()]++;
                    ctx.fireChannelRead(queuedPacket.retain());
                } finally {
                    queuedPacket.release();
                }
            } else {
                // Found a gap. Wait till we start receive another ordered packet.
                break;
            }
        }
    }

    private EncapsulatedPacket getReassembledPacket(EncapsulatedPacket splitPacket, ByteBufAllocator alloc) {
        this.checkForClosed();

        int partId = splitPacket.getPartId();
        SplitPacketHelper helper = this.splitPackets.get(partId);
        if (helper != null && !helper.matches(splitPacket)) {
            // Part IDs are only unique modulo the size of this array, so an unrelated split packet may
            // hold the slot. Drop the part rather than corrupt a reassembly that is still in progress.
            if (!helper.expired()) {
                return null;
            }
            helper = null; // Released below when set() overwrites it.
        }

        if (helper == null) {
            this.splitPackets.set(partId, helper = new SplitPacketHelper(partId, splitPacket.getPartCount()));
        }

        // Try reassembling the packet, tracking how many bytes this session now retains for split reassembly.
        int sizeBefore = helper.getReassembledSize();
        EncapsulatedPacket result = helper.add(splitPacket, alloc);
        this.splitQueuedBytes += helper.getReassembledSize() - sizeBefore;
        if (result != null) {
            // Packet reassembled. Remove the helper and reclaim the bytes it held.
            this.splitQueuedBytes -= helper.getReassembledSize();
            this.splitPackets.remove(partId, helper);
        }

        return result;
    }

    /**
     * Drops split reassemblies that have been waiting too long for their remaining parts. Without this a peer can
     * hold parts in reassembly indefinitely: {@link SplitPacketHelper#expired()} has otherwise never been called.
     */
    private void evictExpiredSplitPackets() {
        Iterator<SplitPacketHelper> iterator = this.splitPackets.iterator();
        while (iterator.hasNext()) {
            SplitPacketHelper helper = iterator.next();
            if (helper != null && helper.expired()) {
                this.splitQueuedBytes -= helper.getReassembledSize();
                iterator.remove();
            }
        }
    }

    private void tryTick() {
        try {
            this.onTick();
        } catch (Throwable t) {
            log.error("[{}] Error while ticking RakSessionCodec state={} channelActive={}", this.getRemoteAddress(), this.state, this.channel.isActive(), t);
            this.channel.close();
        }
    }

    private void onTick() {
        long curTime = this.currentTimeMillis();

        int maxQueuedBytes = this.channel.config().getOption(RakChannelOption.RAK_MAX_QUEUED_BYTES);

        int totalQueuedBytes = totalQueuedBytes(this.queuedBytes, this.channel.pendingRakNetOutboundBytes());

        if (maxQueuedBytes > 0 && totalQueuedBytes > maxQueuedBytes) {
            this.disconnect(RakDisconnectReason.QUEUE_TOO_LONG);
            return;
        }

        // Drop stale incomplete reassemblies before enforcing the inbound split budget, so a peer that
        // legitimately abandons a split packet does not count against a peer that is deliberately holding one.
        this.evictExpiredSplitPackets();

        int maxSplitQueuedBytes = this.channel.config().getOption(RakChannelOption.RAK_MAX_SPLIT_QUEUED_BYTES);
        if (maxSplitQueuedBytes > 0 && this.splitQueuedBytes > maxSplitQueuedBytes) {
            this.disconnect(RakDisconnectReason.SPLIT_QUEUE_TOO_LONG);
            return;
        }

        int maxOrderingQueuedBytes = this.channel.config().getOption(RakChannelOption.RAK_MAX_ORDERING_QUEUED_BYTES);
        if (maxOrderingQueuedBytes > 0 && this.orderingQueuedBytes > maxOrderingQueuedBytes) {
            this.disconnect(RakDisconnectReason.ORDERING_QUEUE_TOO_LONG);
            return;
        }

        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null) {
            metrics.queuedPacketBytes(totalQueuedBytes);
        }

        if (this.state == RakState.UNCONNECTED) {
            if (this.isTimedOut(curTime)) {
                this.close(RakDisconnectReason.TIMED_OUT);
            }
            return;
        }

        if (this.isTimedOut(curTime)) {
            this.disconnect(RakDisconnectReason.TIMED_OUT);
            return;
        }

        ChannelHandlerContext ctx = ctx();

        if (this.currentPingTime + 2000L < curTime) {
            ByteBuf buffer = ctx.alloc().ioBuffer(9);
            buffer.writeByte(ID_CONNECTED_PING);
            buffer.writeLong(curTime);
            this.currentPingTime = curTime;
            this.write(ctx, new RakMessage(buffer, RakReliability.UNRELIABLE, RakPriority.IMMEDIATE), ctx.voidPromise());
        }

         this.internalFlush(ctx);
    }

    static int totalQueuedBytes(int sessionQueuedBytes, int handoffQueuedBytes) {
        return (int) Math.min(Integer.MAX_VALUE,
                Math.max(0L, (long) sessionQueuedBytes) + Math.max(0L, (long) handoffQueuedBytes));
    }

    private void internalFlush(ChannelHandlerContext ctx) {
        long curTime = this.currentTimeMillis();
        if (this.lastFlush == curTime) {
            return; // do not flush multiple times within one ms
        }
        this.lastFlush = curTime;

        this.handleIncomingAcknowledge(ctx, curTime, this.incomingAcks, false);
        this.handleIncomingAcknowledge(ctx, curTime, this.incomingNaks, true);
        this.expireModelDatagramSamples(curTime);

        // Send our know outgoing acknowledge packets.
        int mtuSize = this.getMtu();
        int ackMtu = mtuSize - RAKNET_DATAGRAM_HEADER_SIZE;
        int writtenAcks = 0;
        int writtenNacks = 0;

        // if (this.slidingWindow.shouldSendAcks(curTime)) {
        while (!this.outgoingAcks.isEmpty()) {
            ByteBuf buffer = ctx.alloc().ioBuffer(ackMtu);
            buffer.writeByte(FLAG_VALID | FLAG_ACK);
            writtenAcks += RakUtils.writeAckEntries(buffer, this.outgoingAcks, ackMtu - 1);
            ctx.write(buffer, ctx.voidPromise());
            this.slidingWindow.onSendAck();
        }
        // }

        while (!this.outgoingNaks.isEmpty()) {
            ByteBuf buffer = ctx.alloc().ioBuffer(ackMtu);
            buffer.writeByte(FLAG_VALID | FLAG_NACK);
            writtenNacks += RakUtils.writeAckEntries(buffer, this.outgoingNaks, ackMtu - 1);
            ctx.write(buffer, ctx.voidPromise());
        }

        // Send packets that are stale first
        int resendCount = this.sendBoundedRecovery(ctx, curTime, mtuSize);
        // Now send usual packets
        if (this.pendingRetransmissions.isEmpty()
                && !this.slidingWindow.isModelPersistentCongestion()) {
            this.sendDatagrams(ctx, curTime, mtuSize);
        }
        if (this.outgoingPackets.isEmpty() && this.pendingRetransmissions.isEmpty()) {
            this.slidingWindow.onSenderIdle();
        }
        // Finally flush channel
        ctx.flush();

        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null) {
            metrics.nackOut(writtenNacks);
            metrics.ackOut(writtenAcks);
            metrics.rakStaleDatagrams(resendCount);
            this.recoveryMetrics.reportState(metrics, this.slidingWindow, curTime);
        }
    }

    private void handleIncomingAcknowledge(ChannelHandlerContext ctx, long curTime, Queue<IntRange> queue, boolean nack) {
        if (queue.isEmpty()) {
            return;
        }

        IntRange range;
        while ((range = queue.poll()) != null) {
            if (range.end < range.start || range.end >= this.datagramWriteIndex) {
                if (log.isDebugEnabled()) {
                    log.debug("Received {} with out-of-range indices [{}, {}] from {} (write index: {})",
                            nack ? "NACK" : "ACK", range.start, range.end, this.getRemoteAddress(), this.datagramWriteIndex);
                }
                continue;
            }

            for (int i = range.start; i <= range.end; i++) {
                RakDatagramPacket datagram = nack ? this.sentDatagrams.get(i) : this.sentDatagrams.remove(i);
                if (datagram != null) {
                    if (nack) {
                        this.onIncomingNack(ctx, datagram, curTime);
                    } else {
                        this.onIncomingAck(datagram, curTime);
                    }
                } else {
                    RakSlidingWindow.ModelDatagramSample sample = this.modelDatagramSamples.remove(i);
                    if (sample != null) {
                        if (nack) {
                            this.slidingWindow.onUnreliableLoss(sample);
                        } else {
                            this.slidingWindow.onUnreliableAck(sample, curTime);
                            this.boundedRecovery.onAcknowledgementProgress(
                                    this.slidingWindow, null, this.sentDatagrams.values());
                        }
                    }
                }
            }
        }
    }

    private void onIncomingAck(RakDatagramPacket datagram, long curTime) {
        try {
            PendingRetransmission pending = this.findPendingRetransmission(datagram.getSequenceIndex());
            if (pending != null) {
                this.pendingRetransmissions.remove(pending);
            }
            this.slidingWindow.onAck(curTime, datagram, this.datagramReadIndex);
            this.boundedRecovery.onAcknowledgementProgress(this.slidingWindow, datagram,
                    this.sentDatagrams.values());
            RakChannelMetrics metrics = this.getMetrics();
            this.recoveryMetrics.onAcknowledgementProgress(metrics, this.slidingWindow, datagram, curTime);
        } finally {
            datagram.release();
        }
    }

    private void onIncomingNack(ChannelHandlerContext ctx, RakDatagramPacket datagram, long curTime) {
        if (log.isTraceEnabled()) {
            log.trace("NAK'ed datagram {} from {}", datagram.getSequenceIndex(), this.getRemoteAddress());
        }

        if (this.boundedRecovery.scheduleNack(datagram)) {
            // A NACK is an explicit request for the missing datagram. Retransmit at the next admission opportunity;
            // do not impose a reordering timer on latency-sensitive reliable-ordered traffic. The model aggregates
            // loss evidence before reducing the congestion window, so one packet loss does not slow the path.
            this.slidingWindow.onBoundedNackLoss(datagram, this.datagramSendOrdinal - 1L);
            this.pendingRetransmissions.offer(new PendingRetransmission(datagram.getSequenceIndex()));
        }
    }

    private int sendBoundedRecovery(ChannelHandlerContext ctx, long curTime, int mtuSize) {
        if (!this.boundedRecovery.isProbeDue(this.slidingWindow)) {
            return this.drainBoundedRetransmissions(ctx, curTime,
                    RakBoundedRecovery.nackFlushBudget(mtuSize));
        }

        RakDatagramPacket timeoutCandidate = this.boundedRecovery.getProbeAnchor();
        if (timeoutCandidate == null) {
            this.boundedRecovery.onProbeDeferred(this.slidingWindow, this.sentDatagrams.values());
            return this.drainBoundedRetransmissions(ctx, curTime,
                    RakBoundedRecovery.nackFlushBudget(mtuSize));
        }

        PendingRetransmission pending = this.findPendingRetransmission(timeoutCandidate.getSequenceIndex());

        // A PTO is evidence for only its selected oldest attempt. Do not bulk-retire every blackholed physical
        // attempt: that would manufacture recovery capacity and recreate the retransmission storm this policy bounds.
        if (this.boundedRecovery.getPtoBackoff() >= 2) {
            // Multiple exponentially backed-off probes without intervening ACK progress are a duration-based
            // persistent-congestion signal. A single NACK or PTO never triggers this collapse.
            this.slidingWindow.onPersistentCongestion();
        }
        // PTO elicits ACK progress but does not by itself prove network congestion. Explicit NACK recovery remains
        // loss-classifying; multiple backed-off PTOs are handled by the persistent-congestion path.
        this.slidingWindow.onBoundedPtoExpired(timeoutCandidate);
        int size = timeoutCandidate.getSize();
        boolean probe = !this.slidingWindow.canSendBoundedRecovery(size, curTime);
        if ((probe && !this.slidingWindow.canSendBoundedProbe(size))
                || !RakBoundedRecovery.ptoFlushBudget(mtuSize).canConsume(size)) {
            // Timeout selection is ephemeral: never put it into the NACK FIFO. A deferred timeout therefore cannot
            // block a later NACK or newly admitted traffic while the one-probe exception is occupied.
            this.boundedRecovery.onProbeDeferred(this.slidingWindow, this.sentDatagrams.values());
            return this.drainBoundedRetransmissions(ctx, curTime,
                    RakBoundedRecovery.nackFlushBudget(mtuSize));
        }

        this.retransmitBoundedDatagram(ctx, timeoutCandidate, pending, curTime,
                RakDatagramSendType.TIMEOUT_RETRANSMISSION, probe);
        return 1;
    }

    private PendingRetransmission findPendingRetransmission(int sequenceIndex) {
        for (PendingRetransmission pending : this.pendingRetransmissions) {
            if (pending.sequenceIndex == sequenceIndex) {
                return pending;
            }
        }
        return null;
    }

    private void expireModelDatagramSamples(long curTime) {
        ModelSampleExpiry expiry;
        while ((expiry = this.modelSampleExpiries.peek()) != null && expiry.expiresAtMillis <= curTime) {
            this.modelSampleExpiries.poll();
            if (this.modelDatagramSamples.get(expiry.sequenceIndex) == expiry.sample) {
                this.modelDatagramSamples.remove(expiry.sequenceIndex);
                this.slidingWindow.onUnreliableLoss(expiry.sample);
            }
        }
    }

    private int drainBoundedRetransmissions(ChannelHandlerContext ctx, long curTime,
                                            RakBoundedRecovery.FlushBudget budget) {
        while (true) {
            PendingRetransmission pending = this.pendingRetransmissions.peek();
            if (pending == null) {
                break;
            }

            RakDatagramPacket datagram = this.sentDatagrams.get(pending.sequenceIndex);
            if (datagram == null || !datagram.isRetransmissionPending()
                    || datagram.getSequenceIndex() != pending.sequenceIndex) {
                this.pendingRetransmissions.poll();
                continue;
            }

            int size = datagram.getSize();
            if (!budget.canConsume(size)) {
                break;
            }

            boolean probe = !this.slidingWindow.canSendBoundedRecovery(size, curTime);
            if (probe) {
                break;
            }

            this.retransmitBoundedDatagram(ctx, datagram, pending, curTime,
                    RakDatagramSendType.NACK_RETRANSMISSION, false);
            budget.consume(size);
        }
        return budget.getDatagrams();
    }

    private void retransmitBoundedDatagram(ChannelHandlerContext ctx, RakDatagramPacket datagram,
                                           PendingRetransmission pending, long curTime,
                                           RakDatagramSendType sendType, boolean probe) {
        int oldSequenceIndex = datagram.getSequenceIndex();
        long oldSendTime = datagram.getSendTime();
        long oldNextSend = datagram.getNextSend();
        long oldSendOrdinal = datagram.getSendOrdinal();
        int oldRetransmissionCount = datagram.getRetransmissionCount();
        int oldWriteIndex = this.datagramWriteIndex;
        long oldSendOrdinalCounter = this.datagramSendOrdinal;
        RakRecoveryMetrics.SendState oldRecoveryMetricsState = this.recoveryMetrics.captureSendState();
        RakSlidingWindow.ModelSendState oldModelSendState = this.slidingWindow.captureModelSendState(datagram);
        boolean wasPending = datagram.isRetransmissionPending();

        this.sentDatagrams.remove(oldSequenceIndex);
        datagram.setRetransmissionPending(false);
        this.slidingWindow.onBoundedRetransmit(datagram, probe, curTime,
                this.outgoingPackets.isEmpty() && this.pendingRetransmissions.size() <= (pending == null ? 0 : 1));
        try {
            this.sendDatagram(ctx, datagram, curTime, this.sentDatagrams, sendType, probe);
        } catch (RuntimeException | Error throwable) {
            RakDatagramPacket retained = this.sentDatagrams.remove(datagram.getSequenceIndex());
            if (retained != null) {
                retained.release();
            }
            if (datagram.getRetransmissionCount() != oldRetransmissionCount) {
                datagram.restoreRetransmissionCount(oldRetransmissionCount);
            }
            this.recoveryMetrics.restoreSendState(oldRecoveryMetricsState);
            this.slidingWindow.onBoundedRetransmitFailed(datagram);
            this.slidingWindow.restoreModelSendState(datagram, oldModelSendState);
            datagram.setSequenceIndex(oldSequenceIndex);
            datagram.setSendTime(oldSendTime);
            datagram.setNextSend(oldNextSend);
            datagram.setSendOrdinal(oldSendOrdinal);
            datagram.setRetransmissionPending(wasPending);
            this.datagramWriteIndex = oldWriteIndex;
            this.datagramSendOrdinal = oldSendOrdinalCounter;
            this.sentDatagrams.put(oldSequenceIndex, datagram);
            this.boundedRecovery.refreshProbeDeadline(this.slidingWindow, this.sentDatagrams.values());
            throw throwable;
        }

        if (pending != null) {
            this.pendingRetransmissions.remove(pending);
        }
        if (sendType == RakDatagramSendType.TIMEOUT_RETRANSMISSION) {
            this.boundedRecovery.onProbeSent(this.slidingWindow, datagram, this.sentDatagrams.values());
        } else {
            this.boundedRecovery.onNackRetransmission(this.slidingWindow, datagram,
                    this.sentDatagrams.values());
        }
    }

    private void sendDatagrams(ChannelHandlerContext ctx, long curTime, int mtuSize) {
        if (this.outgoingPackets.isEmpty()) {
            return;
        }

        int transmissionBandwidth = this.slidingWindow.getTransmissionBandwidth(curTime);
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        datagram.setSendTime(curTime);
        EncapsulatedPacket packet;

        while ((packet = this.outgoingPackets.peek()) != null) {
            int size = packet.getSize();
            boolean reliabilityBoundary = !datagram.getPackets().isEmpty()
                    && datagram.getPackets().get(0).getReliability().isReliable()
                    != packet.getReliability().isReliable();
            boolean startsDatagram = datagram.getPackets().isEmpty()
                    || reliabilityBoundary
                    || datagram.getSize() + size > mtuSize - RAKNET_DATAGRAM_HEADER_SIZE;
            int chargedSize = size;
            if (startsDatagram) {
                chargedSize += RAKNET_DATAGRAM_HEADER_SIZE;
            }
            if (transmissionBandwidth < chargedSize) {
                break;
            }

            transmissionBandwidth -= chargedSize;
            this.outgoingPackets.remove();
            this.queuedBytes -= packet.getBuffer().readableBytes();

            // Datagram retransmission is whole-datagram. Keep reliable and unreliable encapsulated packets in
            // separate datagrams so loss recovery cannot duplicate nominally unreliable application traffic.
            if (reliabilityBoundary || !datagram.tryAddPacket(packet, mtuSize)) {
                this.sendDatagram(ctx, datagram, curTime, this.sentDatagrams,
                        RakDatagramSendType.ORIGINAL, false);

                datagram = RakDatagramPacket.newInstance();
                datagram.setSendTime(curTime);
                if (!datagram.tryAddPacket(packet, mtuSize)) {
                    throw new IllegalArgumentException("Packet too large to fit in MTU (size: " + packet.getSize() + ", MTU: " + mtuSize + ")");
                }
            }
        }

        if (!datagram.getPackets().isEmpty()) {
            this.sendDatagram(ctx, datagram, curTime, this.sentDatagrams,
                    RakDatagramSendType.ORIGINAL, false);
        } else {
            // Bounded recovery may intentionally leave traffic queued behind cwnd. Do not leak the empty pooled
            // builder allocated for this flush while waiting for ACK progress.
            datagram.release();
        }
    }

    private void sendImmediate(ChannelHandlerContext ctx, EncapsulatedPacket[] packets) {
        long curTime = this.currentTimeMillis();
        for (EncapsulatedPacket packet : packets) {
            RakDatagramPacket datagram = RakDatagramPacket.newInstance();
            datagram.setSendTime(curTime);
            if (!datagram.tryAddPacket(packet, this.getMtu())) {
                throw new IllegalArgumentException("Packet too large to fit in MTU (size: " + packet.getSize() + ", MTU: " + this.getMtu() + ")");
            }
            this.sendDatagram(ctx, datagram, curTime, this.sentDatagrams,
                    RakDatagramSendType.ORIGINAL, false);
        }
        ctx.flush();
    }

    private void sendDatagram(ChannelHandlerContext ctx, RakDatagramPacket datagram, long time,
                              IntObjectMap<RakDatagramPacket> sent, RakDatagramSendType sendType,
                              boolean recoveryProbe) {
        if (datagram.getPackets().isEmpty()) {
            throw new IllegalArgumentException("RakNetDatagram with no packets");
        }

        RakChannelMetrics metrics = this.getMetrics();

        int oldIndex = datagram.getSequenceIndex();
        int oldWriteIndex = this.datagramWriteIndex;
        long oldSendOrdinalCounter = this.datagramSendOrdinal;
        RakRecoveryMetrics.SendState oldRecoveryMetricsState = sendType == RakDatagramSendType.ORIGINAL
                ? null : this.recoveryMetrics.captureSendState();
        RakSlidingWindow.ModelSendState originalModelSendState = null;
        if (oldIndex == -1) {
            if (this.originalSendRollbackState == null) {
                // Test harnesses and pre-activation integration paths may install the window directly.
                this.originalSendRollbackState = this.slidingWindow.newModelSendState();
            }
            originalModelSendState = this.originalSendRollbackState;
            this.slidingWindow.captureModelSendState(datagram, originalModelSendState);
        }
        datagram.setSequenceIndex(this.datagramWriteIndex++);
        datagram.setSendOrdinal(this.datagramSendOrdinal++);
        datagram.setSendTime(time);
        if (oldIndex != -1 && datagram.isRecoveryProbe() != recoveryProbe) {
            throw new IllegalStateException("Bounded recovery probe accounting mismatch");
        }

        RakSlidingWindow.ModelDatagramSample replaced =
                this.modelDatagramSamples.remove(datagram.getSequenceIndex());
        if (replaced != null) {
            this.slidingWindow.onUnreliableLoss(replaced);
        }

        boolean reliable = false;
        RakSlidingWindow.ModelDatagramSample unreliableSample = null;
        ModelSampleExpiry unreliableExpiry = null;
        for (EncapsulatedPacket packet : datagram.getPackets()) {
            // Check if packet is reliable so it can be resent later if a NAK is received.
            if (packet.getReliability().isReliable()) {
                reliable = true;
                datagram.setNextSend(time + this.slidingWindow.getRtoForRetransmission());
                if (oldIndex == -1) {
                    this.slidingWindow.onReliableSend(datagram, this.isModelAppLimited());
                }
                sent.put(datagram.getSequenceIndex(), datagram.retain()); // Keep for resending
                if (oldIndex == -1) {
                    this.boundedRecovery.onReliableSend(this.slidingWindow, datagram);
                }
                break;
            }
        }
        if (!reliable) {
            unreliableSample = this.slidingWindow.onUnreliableSendTracked(
                    datagram.getSize(), time, this.isModelAppLimited());
            this.modelDatagramSamples.put(datagram.getSequenceIndex(), unreliableSample);
            long expiresAt = time + Math.max(1_000L, this.slidingWindow.getRtoForRetransmission());
            unreliableExpiry = new ModelSampleExpiry(datagram.getSequenceIndex(), expiresAt,
                    unreliableSample);
            this.modelSampleExpiries.offer(unreliableExpiry);
        }
        try {
            if (metrics != null) {
                metrics.rakDatagramsOut(1);
            }
            this.recoveryMetrics.onDatagramSent(metrics, this.slidingWindow, datagram, sendType, time);
            ctx.write(datagram, ctx.voidPromise());
        } catch (RuntimeException | Error throwable) {
            if (oldIndex == -1 && reliable) {
                RakDatagramPacket retained = sent.remove(datagram.getSequenceIndex());
                if (retained != null) {
                    retained.release();
                }
                this.slidingWindow.onReliableSendFailed(datagram, originalModelSendState);
                this.boundedRecovery.refreshProbeDeadline(this.slidingWindow, sent.values());
                if (oldRecoveryMetricsState != null) {
                    this.recoveryMetrics.restoreSendState(oldRecoveryMetricsState);
                }
                this.datagramWriteIndex = oldWriteIndex;
                this.datagramSendOrdinal = oldSendOrdinalCounter;
                if (datagram.refCnt() > 0) {
                    datagram.release();
                }
            } else if (unreliableSample != null
                    && this.modelDatagramSamples.get(datagram.getSequenceIndex()) == unreliableSample) {
                this.modelDatagramSamples.remove(datagram.getSequenceIndex());
                this.modelSampleExpiries.remove(unreliableExpiry);
                this.slidingWindow.onUnreliableSendFailed(unreliableSample);
                if (oldRecoveryMetricsState != null) {
                    this.recoveryMetrics.restoreSendState(oldRecoveryMetricsState);
                }
                this.datagramWriteIndex = oldWriteIndex;
                this.datagramSendOrdinal = oldSendOrdinalCounter;
                if (oldIndex == -1 && datagram.refCnt() > 0) {
                    datagram.release();
                }
            }
            throw throwable;
        }
    }

    private boolean isModelAppLimited() {
        return this.outgoingPackets.isEmpty() && this.pendingRetransmissions.isEmpty()
                && this.channel.pendingRakNetOutboundBytes() == 0;
    }

    private ChannelHandlerContext ctx() {
        return this.channel.rakPipeline().context(RakSessionCodec.NAME);
    }

    private long currentTimeMillis() {
        return this.clock.getAsLong();
    }

    private EncapsulatedPacket[] createEncapsulated(RakMessage rakMessage) {
        int maxLength = this.getMtu() - MAXIMUM_ENCAPSULATED_HEADER_SIZE - RAKNET_DATAGRAM_HEADER_SIZE;

        ByteBuf[] buffers;
        int splitId = 0;
        RakReliability reliability = rakMessage.reliability();
        ByteBuf buffer = rakMessage.content();
        int orderingChannel = rakMessage.channel();

        if (buffer.readableBytes() > maxLength) {
            // Packet requires splitting
            // Adjust reliability
            switch (reliability) {
                case UNRELIABLE:
                    reliability = RakReliability.RELIABLE;
                    break;
                case UNRELIABLE_SEQUENCED:
                    reliability = RakReliability.RELIABLE_SEQUENCED;
                    break;
                case UNRELIABLE_WITH_ACK_RECEIPT:
                    reliability = RakReliability.RELIABLE_WITH_ACK_RECEIPT;
                    break;
            }

            int split = ((buffer.readableBytes() - 1) / maxLength) + 1;
            buffer.retain(split);

            buffers = new ByteBuf[split];
            for (int i = 0; i < split; i++) {
                buffers[i] = buffer.readSlice(Math.min(maxLength, buffer.readableBytes()));
            }
            if (buffer.isReadable()) {
                throw new IllegalStateException("Buffer still has bytes to read!");
            }

            // Allocate split ID
            splitId = this.splitIndex++;
        } else {
            buffers = new ByteBuf[]{buffer.readRetainedSlice(buffer.readableBytes())};
        }

        // Set meta
        // TODO: sequencing
        int orderingIndex = 0;
        if (reliability.isOrdered()) {
            orderingIndex = this.orderWriteIndex[orderingChannel]++;
        }

        // Now create the packets.
        EncapsulatedPacket[] packets = new EncapsulatedPacket[buffers.length];
        for (int i = 0, parts = buffers.length; i < parts; i++) {
            EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
            packet.setBuffer(buffers[i]);
            packet.setNeedsBAS(true);
            packet.setOrderingChannel((short) orderingChannel);
            packet.setOrderingIndex(orderingIndex);
            // packet.setSequenceIndex(sequencingIndex);
            packet.setReliability(reliability);
            if (reliability.isReliable()) {
                packet.setReliabilityIndex(this.reliabilityWriteIndex++);
            }

            if (parts > 1) {
                packet.setSplit(true);
                packet.setPartIndex(i);
                packet.setPartCount(parts);
                packet.setPartId(splitId);
            }

            packets[i] = packet;
        }
        return packets;
    }

    private long getNextWeight(RakPriority priority) {
        int priorityLevel = priority.ordinal();
        long next = this.outgoingPacketNextWeights[priorityLevel];

        if (!this.outgoingPackets.isEmpty()) {
            int headPriorityLevel = this.outgoingPackets.peekPriority();
            long minimumWeight = this.outgoingPackets.peekWeight()
                    - (1L << headPriorityLevel) * headPriorityLevel + headPriorityLevel;
            if (next < minimumWeight) {
                next = minimumWeight + (1L << priorityLevel) * priorityLevel + priorityLevel;
            }
            this.outgoingPacketNextWeights[priorityLevel] = next
                    + (1L << priorityLevel) * (priorityLevel + 1) + priorityLevel;
        } else {
            this.initOutgoingPacketWeights();
        }
        return next;
    }

    public void disconnect() {
        this.disconnect(RakDisconnectReason.DISCONNECTED);
    }

    public void disconnect(RakDisconnectReason reason) {
        // Ensure we disconnect on the right thread
        if (this.channel.parent().eventLoop().inEventLoop()) {
            this.disconnect0(reason);
        } else {
            this.channel.parent().eventLoop().execute(() -> this.disconnect0(reason));
        }
    }

    private ChannelPromise disconnect0(RakDisconnectReason reason) {
        if (this.state == RakState.UNCONNECTED || this.state == RakState.DISCONNECTING) {
            return this.channel.voidPromise();
        }
        this.setState(RakState.DISCONNECTING);

        if (log.isDebugEnabled()) {
            log.debug("Disconnecting RakNet Session ({} => {}) due to {}", this.channel.localAddress(), this.getRemoteAddress(), reason);
        }

        ChannelHandlerContext ctx = this.ctx();

        ByteBuf buffer = ctx.alloc().ioBuffer(1);
        buffer.writeByte(ID_DISCONNECTION_NOTIFICATION);
        RakMessage rakMessage = new RakMessage(buffer, RakReliability.RELIABLE, RakPriority.IMMEDIATE);

        ChannelPromise promise = ctx.newPromise();
        promise.addListener((ChannelFuture future) -> // The channel provided in ChannelFuture is parent channel,
                this.channel.pipeline().fireUserEventTriggered(reason).close()); // but we want RakChannel instead
        this.terminalDisconnectWrite = true;
        try {
            this.write(ctx, rakMessage, promise);
        } finally {
            this.terminalDisconnectWrite = false;
        }
        return promise;
    }

    public void close(RakDisconnectReason reason) {
        if (this.state == RakState.DISCONNECTING) {
            return;
        }
        this.setState(RakState.DISCONNECTING);

        if (log.isDebugEnabled()) {
            log.debug("Closing RakNet Session ({} => {}) due to {}", this.channel.localAddress(), this.getRemoteAddress(), reason);
        }

        this.channel.pipeline().fireUserEventTriggered(reason).close();
    }

    public boolean isClosed() {
        return this.state == RakState.UNCONNECTED;
    }

    private void checkForClosed() {
        if (this.state == RakState.UNCONNECTED) {
            throw new IllegalStateException("RakSession is closed!");
        }
    }

    private void setState(RakState state) {
        if (this.state == state) {
            return;
        }
        this.state = state;

        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null) {
            metrics.stateChange(state);
        }
    }

    private static Throwable appendFailure(Throwable existing, Throwable additional) {
        if (existing == null) {
            return additional;
        }
        existing.addSuppressed(additional);
        return existing;
    }

    private static void throwUnchecked(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        throw new IllegalStateException("RakNet session cleanup failed", throwable);
    }

    public void recalculatePongTime(long pingTime) {
        if (this.currentPingTime == pingTime) {
            this.lastPingTime = this.currentPingTime;
            this.lastPongTime = this.currentTimeMillis();
        }
    }

    private void touch() {
        this.checkForClosed();
        this.lastTouched = this.currentTimeMillis();
    }

    public boolean isStale(long curTime) {
        return curTime - this.lastTouched >= SESSION_STALE_MS;
    }

    public boolean isStale() {
        return this.isStale(this.currentTimeMillis());
    }

    public boolean isTimedOut(long curTime) {
        return curTime - this.lastTouched >= this.channel.config().getOption(RakChannelOption.RAK_SESSION_TIMEOUT);
    }

    public boolean isTimedOut() {
        return this.isTimedOut(this.currentTimeMillis());
    }

    public long getPing() {
        return this.lastPongTime - this.lastPingTime;
    }

    public double getRTT() {
        return this.slidingWindow.getRTT();
    }

    public int getMtu() {
        return this.channel.config().getMtu() - UDP_HEADER_SIZE - (this.getRemoteAddress().getAddress() instanceof Inet6Address ? 40 : 20);
    }

    public RakChannelMetrics getMetrics() {
        return this.channel.config().getMetrics();
    }

    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) this.channel.remoteAddress();
    }

    protected Queue<IntRange> getAcknowledgeQueue(boolean nack) {
        return nack ? this.incomingNaks : this.incomingAcks;
    }

    public Channel getChannel() {
        return channel;
    }

    private static final class PendingRetransmission {
        private final int sequenceIndex;

        private PendingRetransmission(int sequenceIndex) {
            this.sequenceIndex = sequenceIndex;
        }
    }

    private static final class ModelSampleExpiry implements Comparable<ModelSampleExpiry> {
        private final int sequenceIndex;
        private final long expiresAtMillis;
        private final RakSlidingWindow.ModelDatagramSample sample;

        private ModelSampleExpiry(int sequenceIndex, long expiresAtMillis,
                                  RakSlidingWindow.ModelDatagramSample sample) {
            this.sequenceIndex = sequenceIndex;
            this.expiresAtMillis = expiresAtMillis;
            this.sample = sample;
        }

        @Override
        public int compareTo(ModelSampleExpiry other) {
            return Long.compare(this.expiresAtMillis, other.expiresAtMillis);
        }
    }

}
