/*
 * Copyright 2026 CloudburstMC
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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.cloudburstmc.netty.channel.raknet.RakChannel;
import org.cloudburstmc.netty.channel.raknet.RakPriority;
import org.cloudburstmc.netty.channel.raknet.RakReliability;
import org.cloudburstmc.netty.channel.raknet.RakState;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelConfig;
import org.cloudburstmc.netty.channel.raknet.packet.EncapsulatedPacket;
import org.cloudburstmc.netty.channel.raknet.packet.RakMessage;
import org.cloudburstmc.netty.util.FastBinaryMinHeap;
import org.cloudburstmc.netty.util.FastWeightedFairQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RakSessionCodecPriorityTests {
    private static final int MTU = 1_200;

    @Test
    public void derivesPriorityWeightsFromActualQueueHead() throws Exception {
        Harness harness = harness();
        ByteBuf payload = null;
        try {
            payload = harness.enqueue(RakPriority.NORMAL);

            Assertions.assertEquals(10L, harness.outgoing.peekWeight());
            Assertions.assertEquals(RakPriority.NORMAL.ordinal(), harness.outgoing.peekPriority());
            Assertions.assertEquals(7L, nextWeight(harness.codec, RakPriority.HIGH),
                    "a lagging priority advances from the actual root weight");
            Assertions.assertEquals(12L, nextWeight(harness.codec, RakPriority.HIGH),
                    "the per-priority weight advances even when it is already above the root minimum");
        } finally {
            harness.close();
            if (payload != null) {
                Assertions.assertEquals(0, payload.refCnt());
            }
        }
    }

    @Test
    public void periodicHighOrderedTrafficCannotStarveEarlierNormalOrderingIndexes() throws Exception {
        Harness harness = harness();
        List<ByteBuf> payloads = new ArrayList<>();
        Set<Integer> emittedOrderingIndexes = new HashSet<>();
        int initialNormalPackets = 32;
        try {
            for (int i = 0; i < initialNormalPackets; i++) {
                payloads.add(harness.enqueue(RakPriority.NORMAL));
            }

            // Keep the queue permanently backlogged while a HIGH ordered probe and a new NORMAL packet arrive in
            // every service round. A stale HIGH weight must not reset newer NORMAL weights below the old backlog.
            for (int round = 0; round < initialNormalPackets + 8; round++) {
                payloads.add(harness.enqueue(RakPriority.HIGH));
                payloads.add(harness.enqueue(RakPriority.NORMAL));
                Assertions.assertTrue(harness.outgoing.size() >= initialNormalPackets);

                for (int sent = 0; sent < 2; sent++) {
                    EncapsulatedPacket packet = harness.outgoing.poll();
                    Assertions.assertNotNull(packet);
                    emittedOrderingIndexes.add(packet.getOrderingIndex());
                    packet.release();
                }
            }

            for (int orderingIndex = 0; orderingIndex < initialNormalPackets; orderingIndex++) {
                Assertions.assertTrue(emittedOrderingIndexes.contains(orderingIndex),
                        "earlier NORMAL ordering index " + orderingIndex + " was starved by periodic HIGH traffic");
            }
            Assertions.assertFalse(harness.outgoing.isEmpty(), "the test must retain a persistent backlog");
        } finally {
            harness.close();
            for (ByteBuf payload : payloads) {
                Assertions.assertEquals(0, payload.refCnt(), "queued or emitted payload was not reclaimed");
            }
        }
    }

    @Test
    public void splitHighTrafficGetsDistinctWeightsWithoutCorruptingTheQueueHead() throws Exception {
        Harness harness = harness();
        ByteBuf normalPayload = null;
        ByteBuf splitPayload = null;
        try {
            normalPayload = harness.enqueue(RakPriority.NORMAL);
            splitPayload = harness.enqueue(RakPriority.HIGH, 2_000);

            Assertions.assertEquals(3, harness.outgoing.size());
            Assertions.assertEquals(7L, harness.outgoing.peekWeight());
            Assertions.assertEquals(RakPriority.HIGH.ordinal(), harness.outgoing.peekPriority());

            EncapsulatedPacket firstSplitPart = harness.outgoing.poll();
            Assertions.assertEquals(1, firstSplitPart.getOrderingIndex());
            firstSplitPart.release();

            Assertions.assertEquals(10L, harness.outgoing.peekWeight());
            EncapsulatedPacket earlierNormal = harness.outgoing.poll();
            Assertions.assertEquals(0, earlierNormal.getOrderingIndex());
            earlierNormal.release();

            Assertions.assertEquals(12L, harness.outgoing.peekWeight(),
                    "each split fragment receives its own scheduler weight");
            EncapsulatedPacket secondSplitPart = harness.outgoing.poll();
            Assertions.assertEquals(1, secondSplitPart.getOrderingIndex());
            secondSplitPart.release();
        } finally {
            harness.close();
            if (normalPayload != null) {
                Assertions.assertEquals(0, normalPayload.refCnt());
            }
            if (splitPayload != null) {
                Assertions.assertEquals(0, splitPayload.refCnt());
            }
        }
    }

    @Test
    public void lowerWeightSeriesRepairsSingletonRootAndReclaimsElements() {
        FastBinaryMinHeap<ByteBuf> heap = new FastBinaryMinHeap<>(4);
        ByteBuf root = Unpooled.buffer(1).writeByte(0);
        ByteBuf first = Unpooled.buffer(1).writeByte(1);
        ByteBuf second = Unpooled.buffer(1).writeByte(2);
        try {
            heap.insert(10L, RakPriority.NORMAL.ordinal(), root);
            heap.insertSeries(7L, RakPriority.HIGH.ordinal(), new ByteBuf[]{first, second});

            Assertions.assertEquals(7L, heap.peekWeight());
            Assertions.assertEquals(RakPriority.HIGH.ordinal(), heap.peekPriority());
            while (!heap.isEmpty()) {
                heap.poll().release();
            }
            heap.release();

            Assertions.assertEquals(0, root.refCnt());
            Assertions.assertEquals(0, first.refCnt());
            Assertions.assertEquals(0, second.refCnt());
        } finally {
            if (heap.refCnt() > 0) {
                heap.release();
            }
            releaseIfNeeded(root);
            releaseIfNeeded(first);
            releaseIfNeeded(second);
        }
    }

    @Test
    public void releasingNonEmptyHeapDoesNotDoubleRecycleEntries() {
        FastBinaryMinHeap<String> heap = new FastBinaryMinHeap<>(4);
        heap.insert(10L, RakPriority.NORMAL.ordinal(), "normal");
        heap.insert(7L, RakPriority.HIGH.ordinal(), "high");

        Assertions.assertDoesNotThrow(() -> {
            heap.release();
        });
        Assertions.assertEquals(0, heap.refCnt());
    }

    private static long nextWeight(RakSessionCodec codec, RakPriority priority) throws Exception {
        Method method = RakSessionCodec.class.getDeclaredMethod("getNextWeight", RakPriority.class);
        method.setAccessible(true);
        return (Long) method.invoke(codec, priority);
    }

    private static Harness harness() throws Exception {
        ChannelDuplexHandler outboundHandler = new ChannelDuplexHandler();
        EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        embeddedChannel.pipeline().addLast(RakSessionCodec.NAME, outboundHandler);
        ChannelHandlerContext context = embeddedChannel.pipeline().context(outboundHandler);

        RakChannelConfig config = (RakChannelConfig) Proxy.newProxyInstance(
                RakChannelConfig.class.getClassLoader(), new Class<?>[]{RakChannelConfig.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getMtu")) {
                        return MTU;
                    }
                    return defaultValue(method.getReturnType());
                });
        RakChannel channel = (RakChannel) Proxy.newProxyInstance(
                RakChannel.class.getClassLoader(), new Class<?>[]{RakChannel.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("config")) {
                        return config;
                    }
                    if (method.getName().equals("parent")) {
                        return embeddedChannel;
                    }
                    if (method.getName().equals("remoteAddress") || method.getName().equals("localAddress")) {
                        return new InetSocketAddress("127.0.0.1", 19132);
                    }
                    return defaultValue(method.getReturnType());
                });

        RakSessionCodec codec = new RakSessionCodec(channel, () -> 0L);
        FastWeightedFairQueue<EncapsulatedPacket> outgoing = new FastWeightedFairQueue<>(RakPriority.values().length);
        set(codec, "outgoingPackets", outgoing);
        set(codec, "outgoingPacketNextWeights", new long[4]);
        set(codec, "orderWriteIndex", new int[16]);
        set(codec, "state", RakState.CONNECTED);
        Method initWeights = RakSessionCodec.class.getDeclaredMethod("initOutgoingPacketWeights");
        initWeights.setAccessible(true);
        initWeights.invoke(codec);
        return new Harness(codec, outgoing, embeddedChannel, context);
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0.0F;
        }
        if (type == double.class) {
            return 0.0D;
        }
        return '\0';
    }

    private static void releaseIfNeeded(ByteBuf buffer) {
        if (buffer.refCnt() > 0) {
            buffer.release(buffer.refCnt());
        }
    }

    private static final class Harness {
        private final RakSessionCodec codec;
        private final FastWeightedFairQueue<EncapsulatedPacket> outgoing;
        private final EmbeddedChannel channel;
        private final ChannelHandlerContext context;
        private boolean closed;

        private Harness(RakSessionCodec codec, FastWeightedFairQueue<EncapsulatedPacket> outgoing,
                        EmbeddedChannel channel, ChannelHandlerContext context) {
            this.codec = codec;
            this.outgoing = outgoing;
            this.channel = channel;
            this.context = context;
        }

        private ByteBuf enqueue(RakPriority priority) {
            return this.enqueue(priority, 32);
        }

        private ByteBuf enqueue(RakPriority priority, int payloadSize) {
            ByteBuf payload = Unpooled.buffer(payloadSize).writeZero(payloadSize);
            ChannelPromise promise = this.context.newPromise();
            this.codec.write(this.context,
                    new RakMessage(payload, RakReliability.RELIABLE_ORDERED, priority), promise);
            Assertions.assertTrue(promise.isSuccess());
            return payload;
        }

        private void close() throws Exception {
            if (this.closed) {
                return;
            }
            Method release = RakSessionCodec.class.getDeclaredMethod("releaseSessionResources");
            release.setAccessible(true);
            release.invoke(this.codec);
            this.channel.finishAndReleaseAll();
            this.closed = true;
        }
    }
}
