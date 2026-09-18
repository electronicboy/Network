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
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.cloudburstmc.netty.channel.raknet.RakChannel;
import org.cloudburstmc.netty.channel.raknet.RakDisconnectReason;
import org.cloudburstmc.netty.channel.raknet.RakPriority;
import org.cloudburstmc.netty.channel.raknet.RakReliability;
import org.cloudburstmc.netty.channel.raknet.RakSlidingWindow;
import org.cloudburstmc.netty.channel.raknet.RakState;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelConfig;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelMetrics;
import org.cloudburstmc.netty.channel.raknet.config.RakDatagramSendType;
import org.cloudburstmc.netty.channel.raknet.packet.EncapsulatedPacket;
import org.cloudburstmc.netty.channel.raknet.packet.RakDatagramPacket;
import org.cloudburstmc.netty.channel.raknet.packet.RakMessage;
import org.cloudburstmc.netty.util.FastWeightedFairQueue;
import org.cloudburstmc.netty.util.IntRange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.cloudburstmc.netty.channel.raknet.RakConstants.ID_DISCONNECTION_NOTIFICATION;

public class RakSessionCodecBoundedRecoveryTests {
    private static final int MTU = 1_200;

    @Test
    public void queueTelemetryIncludesParentHandoffAndSaturatesSafely() {
        Assertions.assertEquals(6_144, RakSessionCodec.totalQueuedBytes(4_096, 2_048));
        Assertions.assertEquals(Integer.MAX_VALUE,
                RakSessionCodec.totalQueuedBytes(Integer.MAX_VALUE, Integer.MAX_VALUE));
        Assertions.assertEquals(4_096, RakSessionCodec.totalQueuedBytes(4_096, -1));
    }

    @Test
    public void throwingTerminalMetricsCannotPreventSessionResourceRelease() throws Exception {
        AtomicLong clock = new AtomicLong();
        RakChannelMetrics throwingMetrics = new RakChannelMetrics() {
            @Override
            public void rakRecoveryState(long observedAtMillis, int bytesInFlight, double congestionWindow,
                                         double slowStartThreshold, double smoothedRtt, double rttVariance,
                                         long retransmissionTimeout, int retransmittedDatagramsInFlight,
                                         long lastAckProgressAtMillis, long recoveryStartedAtMillis) {
                throw new IllegalStateException("terminal metrics failure");
            }
        };
        Harness harness = harness(clock, throwingMetrics);
        TestDatagram datagram = datagram(100, 0);
        AtomicBoolean tickCancelled = new AtomicBoolean();
        RakSessionTicker.Registration tick = () -> tickCancelled.set(true);
        try {
            harness.add(datagram.packet);
            set(harness.codec, "tickRegistration", tick);

            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    harness.codec::closeAfterEventLoopTermination);
            Assertions.assertEquals("terminal metrics failure", failure.getMessage());
            harness.codecClosed = true;
            Assertions.assertEquals(0, datagram.payload.refCnt());
            Assertions.assertEquals(0, harness.window.getBytesInFlight());
            Assertions.assertEquals(0, harness.window.getUnackedBytes());
            Assertions.assertNull(get(harness.codec, "sentDatagrams"));
            Assertions.assertTrue(tickCancelled.get());
            Assertions.assertNull(get(harness.codec, "tickRegistration"));
        } finally {
            harness.close();
            releaseIfNeeded(datagram.payload);
        }
    }

    @Test
    public void throwingTerminalStateCallbackCannotPreventSessionResourceRelease() throws Exception {
        AtomicLong clock = new AtomicLong();
        RakChannelMetrics throwingMetrics = new RakChannelMetrics() {
            @Override
            public void stateChange(RakState state) {
                if (state == RakState.DISCONNECTED) {
                    throw new IllegalStateException("terminal state failure");
                }
            }
        };
        Harness harness = harness(clock, throwingMetrics);
        TestDatagram datagram = datagram(100, 0);
        AtomicBoolean tickCancelled = new AtomicBoolean();
        RakSessionTicker.Registration tick = () -> tickCancelled.set(true);
        try {
            harness.add(datagram.packet);
            set(harness.codec, "tickRegistration", tick);

            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    harness.codec::closeAfterEventLoopTermination);
            Assertions.assertEquals("terminal state failure", failure.getMessage());
            harness.codecClosed = true;
            Assertions.assertEquals(0, datagram.payload.refCnt());
            Assertions.assertEquals(0, harness.window.getBytesInFlight());
            Assertions.assertEquals(0, harness.window.getUnackedBytes());
            Assertions.assertNull(get(harness.codec, "sentDatagrams"));
            Assertions.assertTrue(tickCancelled.get());
            Assertions.assertNull(get(harness.codec, "tickRegistration"));
        } finally {
            harness.close();
            releaseIfNeeded(datagram.payload);
        }
    }

    @Test
    public void boundsNacksPromotesOnePtoAndReclaimsAcknowledgedDatagram() throws Exception {
        AtomicLong clock = new AtomicLong();
        Harness harness = harness(clock, null);
        TestDatagram first = datagram(100, 0);
        TestDatagram second = datagram(100, 1);
        TestDatagram third = datagram(100, 2);
        try {
            harness.add(first.packet);
            harness.add(second.packet);
            harness.add(third.packet);
            harness.arm();

            invokeNack(harness.codec, first.packet, 0L);
            invokeNack(harness.codec, second.packet, 0L);
            invokeNack(harness.codec, third.packet, 0L);
            Assertions.assertEquals(3, harness.pending.size());

            Assertions.assertEquals(2, invokeRecovery(harness.codec, harness.context, 0L));
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 2);
            Assertions.assertEquals(1, harness.pending.size(), "one NACK remains after the two-datagram flush cap");

            clock.set(1_000L);
            Assertions.assertEquals(1, invokeRecovery(harness.codec, harness.context, 1_000L));
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 1);
            Assertions.assertTrue(harness.pending.isEmpty(), "PTO promotes and sends one pending recovery");
            Assertions.assertFalse(third.packet.isRetransmissionPending(),
                    "the timeout-selected pending NACK is removed exactly once after handoff");

            int acknowledgedSequence = third.packet.getSequenceIndex();
            Assertions.assertSame(third.packet, harness.sent.remove(acknowledgedSequence));
            invokeAck(harness.codec, third.packet, 1_010L);
            Assertions.assertEquals(0, third.payload.refCnt(), "ACK releases the map-owned logical datagram");
            Assertions.assertFalse(harness.sent.containsKey(acknowledgedSequence));
            Assertions.assertEquals(first.packet.getSize() + second.packet.getSize(),
                    harness.window.getUnackedBytes());

            harness.closeCodec();
            Assertions.assertEquals(0, first.payload.refCnt());
            Assertions.assertEquals(0, second.payload.refCnt());
            Assertions.assertEquals(0, harness.window.getBytesInFlight());
            Assertions.assertEquals(0, harness.window.getUnackedBytes());
        } finally {
            harness.close();
            releaseIfNeeded(first.payload);
            releaseIfNeeded(second.payload);
            releaseIfNeeded(third.payload);
        }
    }

    @Test
    public void laterAcknowledgementsCannotPostponeLostRetransmissionPto() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingMetrics metrics = new RecordingMetrics();
        Harness harness = harness(clock, metrics);
        TestDatagram first = datagram(100, 0);
        TestDatagram second = datagram(100, 1);
        TestDatagram third = datagram(100, 2);
        TestDatagram later = datagram(100, 10);
        try {
            harness.add(first.packet);
            harness.add(second.packet);
            harness.add(third.packet);
            harness.arm();

            invokeNack(harness.codec, first.packet, 0L);
            Assertions.assertEquals(1, invokeRecovery(harness.codec, harness.context, 0L));
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 1);
            Assertions.assertEquals(1_000L, first.packet.getNextSend());

            clock.set(400L);
            acknowledge(harness, second.packet, 400L);
            clock.set(800L);
            acknowledge(harness, third.packet, 800L);

            harness.add(later.packet);
            harness.schedule(later.packet);
            clock.set(999L);
            acknowledge(harness, later.packet, 999L);
            Assertions.assertEquals(1_000L, harness.recovery.getNextProbeAtMillis(),
                    "ACKs for later datagrams retain the lost retransmission's attempt deadline");
            Assertions.assertEquals(0, invokeRecovery(harness.codec, harness.context, 999L));

            clock.set(1_000L);
            Assertions.assertEquals(1, invokeRecovery(harness.codec, harness.context, 1_000L));
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 1);
            Assertions.assertEquals(Arrays.asList(RakDatagramSendType.NACK_RETRANSMISSION,
                    RakDatagramSendType.TIMEOUT_RETRANSMISSION), metrics.sendTypes);
            Assertions.assertEquals(1, harness.recovery.getPtoBackoff());

            acknowledge(harness, first.packet, 1_010L);
            Assertions.assertEquals(0, first.payload.refCnt());
            Assertions.assertEquals(0, harness.window.getUnackedBytes());
            Assertions.assertEquals(-1L, harness.recovery.getNextProbeAtMillis());
        } finally {
            harness.close();
            releaseIfNeeded(first.payload);
            releaseIfNeeded(second.payload);
            releaseIfNeeded(third.payload);
            releaseIfNeeded(later.payload);
        }
    }

    @Test
    public void sendCallbackFailureRestoresMapQueueAndFlightAccounting() throws Exception {
        AtomicLong clock = new AtomicLong();
        RakChannelMetrics throwingMetrics = new RakChannelMetrics() {
            @Override
            public void rakDatagramSent(RakDatagramSendType sendType, int bytes, int retransmissionAttempt,
                                        int bytesInFlight) {
                throw new IllegalStateException("metrics failure");
            }
        };
        Harness harness = harness(clock, throwingMetrics);
        TestDatagram datagram = datagram(100, 0);
        try {
            harness.add(datagram.packet);
            harness.arm();
            invokeNack(harness.codec, datagram.packet, 0L);
            long oldSendTime = datagram.packet.getSendTime();
            long oldDeadline = datagram.packet.getNextSend();
            long oldOrdinal = datagram.packet.getSendOrdinal();

            InvocationTargetException failure = Assertions.assertThrows(InvocationTargetException.class,
                    () -> recoveryMethod().invoke(harness.codec, harness.context, 0L, MTU));
            Assertions.assertEquals("metrics failure", failure.getCause().getMessage());
            Assertions.assertSame(datagram.packet, harness.sent.get(0));
            Assertions.assertEquals(1, harness.pending.size());
            Assertions.assertTrue(datagram.packet.isRetransmissionPending());
            Assertions.assertEquals(0, harness.window.getBytesInFlight());
            Assertions.assertEquals(datagram.packet.getSize(), harness.window.getUnackedBytes());
            Assertions.assertEquals(oldSendTime, datagram.packet.getSendTime());
            Assertions.assertEquals(oldDeadline, datagram.packet.getNextSend());
            Assertions.assertEquals(oldOrdinal, datagram.packet.getSendOrdinal());
            Assertions.assertEquals(0, datagram.packet.getRetransmissionCount());
            Object recoveryMetrics = get(harness.codec, "recoveryMetrics");
            Assertions.assertEquals(0, get(recoveryMetrics, "retransmittedDatagramsInFlight"));
            Assertions.assertEquals(-1L, get(recoveryMetrics, "recoveryStartedAtMillis"));
            Assertions.assertEquals(-1L, get(recoveryMetrics, "lastStateReportAtMillis"));
            Assertions.assertEquals(1, datagram.packet.refCnt(), "failed send retains exactly the map-owned reference");
        } finally {
            harness.close();
            releaseIfNeeded(datagram.payload);
        }
    }

    @Test
    public void deferredPtoDoesNotOccupyNackFifoOrBlockNewTraffic() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingMetrics metrics = new RecordingMetrics();
        Harness harness = harness(clock, metrics);
        TestDatagram occupyingProbe = datagram(800, 0);
        TestDatagram timeoutCandidate = datagram(1_100, 0);
        TestDatagram pendingNack = datagram(100, 2);
        TestDatagram otherExpired = datagram(100, 20);
        List<TestDatagram> windowBlockers = new ArrayList<>();
        ByteBuf applicationPayload = Unpooled.buffer(50).writeByte(0x42).writeZero(49);
        try {
            harness.add(occupyingProbe.packet);
            timeoutCandidate.packet.setSequenceIndex(1);
            timeoutCandidate.packet.setSendOrdinal(1L);
            harness.add(timeoutCandidate.packet);
            harness.add(pendingNack.packet);
            harness.add(otherExpired.packet);
            for (int i = 21; i < 30; i++) {
                TestDatagram blocker = datagram(1_100, i);
                windowBlockers.add(blocker);
                harness.add(blocker.packet);
            }
            harness.arm();

            harness.window.onBoundedLoss(occupyingProbe.packet, 1L);
            harness.window.onBoundedRetransmit(occupyingProbe.packet, true);
            occupyingProbe.packet.setNextSend(2_000L);
            harness.recovery.refreshProbeDeadline(harness.window, harness.sent.values());
            invokeNack(harness.codec, pendingNack.packet, 0L);

            ChannelPromise applicationPromise = harness.context.newPromise();
            harness.codec.write(harness.context,
                    new RakMessage(applicationPayload, RakReliability.RELIABLE, RakPriority.HIGH),
                    applicationPromise);
            Assertions.assertTrue(applicationPromise.isSuccess());

            clock.set(1_000L);
            invokeInternalFlush(harness.codec, harness.context);
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 2);

            Assertions.assertEquals(Arrays.asList(RakDatagramSendType.NACK_RETRANSMISSION,
                    RakDatagramSendType.ORIGINAL), metrics.sendTypes,
                    "a deferred timeout leaves both the NACK scheduler and new-send admission live");
            Assertions.assertTrue(harness.pending.isEmpty());
            Assertions.assertFalse(timeoutCandidate.packet.isRetransmissionPending(),
                    "timeout selection is ephemeral and never enters the NACK FIFO");
            Assertions.assertFalse(timeoutCandidate.packet.isInFlight());
            Assertions.assertTrue(otherExpired.packet.isInFlight(),
                    "one due PTO declares only its selected oldest attempt lost");
            Assertions.assertEquals(0, harness.recovery.getPtoBackoff());
            Assertions.assertTrue(harness.recovery.getNextProbeAtMillis() > 1_000L);
        } finally {
            harness.close();
            releaseIfNeeded(occupyingProbe.payload);
            releaseIfNeeded(timeoutCandidate.payload);
            releaseIfNeeded(pendingNack.payload);
            releaseIfNeeded(otherExpired.payload);
            for (TestDatagram blocker : windowBlockers) {
                releaseIfNeeded(blocker.payload);
            }
            releaseIfNeeded(applicationPayload);
        }
    }

    @Test
    public void nackIsImmediatelyEligibleWithoutReducingWindowForOneLoss() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingMetrics metrics = new RecordingMetrics();
        Harness harness = harness(clock, metrics);
        TestDatagram lost = datagram(100, 0);
        try {
            harness.add(lost.packet);
            harness.arm();
            double initialCwnd = harness.window.getCongestionWindow();

            invokeNack(harness.codec, lost.packet, 200L);
            Assertions.assertEquals(1, harness.pending.size());
            Assertions.assertFalse(lost.packet.isInFlight(), "the NACK retires the missing physical attempt");
            Assertions.assertTrue(lost.packet.isModelLossClassified());
            Assertions.assertEquals(1, invokeRecovery(harness.codec, harness.context, 200L),
                    "reliable recovery must not wait behind a reordering timer");
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 1);

            Assertions.assertEquals(initialCwnd, harness.window.getCongestionWindow(),
                    "one loss is evidence, not a congestion-window reduction");
            Assertions.assertEquals(Arrays.asList(RakDatagramSendType.NACK_RETRANSMISSION), metrics.sendTypes);
            Assertions.assertTrue(harness.pending.isEmpty());
        } finally {
            harness.close();
            releaseIfNeeded(lost.payload);
        }
    }

    @Test
    public void minimumRttEstimateNeverDelaysExplicitNackRecovery() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingMetrics metrics = new RecordingMetrics();
        Harness harness = harness(clock, metrics);
        TestDatagram cleanHandshake = datagram(100, 0);
        TestDatagram lost = datagram(100, 1);
        try {
            harness.add(cleanHandshake.packet);
            harness.schedule(cleanHandshake.packet);
            acknowledge(harness, cleanHandshake.packet, 5L);
            Assertions.assertEquals(5L, harness.window.getModelMinimumRttMillis());

            lost.packet.setSendTime(1_000L);
            harness.add(lost.packet);
            harness.schedule(lost.packet);
            double initialCwnd = harness.window.getCongestionWindow();

            invokeNack(harness.codec, lost.packet, 1_190L);
            Assertions.assertEquals(1, invokeRecovery(harness.codec, harness.context, 1_190L),
                    "a prior low minRTT sample must not install a recovery delay");
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 1);

            Assertions.assertTrue(harness.pending.isEmpty());
            Assertions.assertEquals(initialCwnd, harness.window.getCongestionWindow());
            Assertions.assertEquals(Arrays.asList(RakDatagramSendType.NACK_RETRANSMISSION), metrics.sendTypes);
        } finally {
            harness.close();
            releaseIfNeeded(cleanHandshake.payload);
            releaseIfNeeded(lost.payload);
        }
    }

    @Test
    public void lateOriginalAckBeforeRetransmitReclaimsPendingTransportState() throws Exception {
        AtomicLong clock = new AtomicLong();
        Harness harness = harness(clock, null);
        TestDatagram reordered = datagram(100, 0);
        try {
            harness.add(reordered.packet);
            harness.arm();
            invokeNack(harness.codec, reordered.packet, 200L);
            Assertions.assertSame(reordered.packet, harness.sent.remove(0));

            invokeAck(harness.codec, reordered.packet, 225L);
            Assertions.assertTrue(harness.sent.isEmpty());
            Assertions.assertTrue(harness.pending.isEmpty());
            Assertions.assertEquals(0, harness.window.getBytesInFlight());
            Assertions.assertEquals(0, harness.window.getUnackedBytes());
            Assertions.assertEquals(0, reordered.payload.refCnt());
        } finally {
            harness.close();
            releaseIfNeeded(reordered.payload);
        }
    }

    @Test
    public void duplicateNackIsRecoveredOnceWithoutWindowCollapse() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingMetrics metrics = new RecordingMetrics();
        Harness harness = harness(clock, metrics);
        TestDatagram lost = datagram(100, 0);
        try {
            harness.add(lost.packet);
            harness.arm();
            double initialCwnd = harness.window.getCongestionWindow();

            invokeNack(harness.codec, lost.packet, 200L);
            invokeNack(harness.codec, lost.packet, 200L);
            Assertions.assertEquals(1, harness.pending.size());
            Assertions.assertEquals(1, invokeRecovery(harness.codec, harness.context, 200L));
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 1);

            Assertions.assertEquals(initialCwnd, harness.window.getCongestionWindow(),
                    "isolated loss is recovered but not treated as a multiplicative decrease");
            Assertions.assertEquals(Arrays.asList(RakDatagramSendType.NACK_RETRANSMISSION), metrics.sendTypes);
            Assertions.assertTrue(harness.pending.isEmpty());
        } finally {
            harness.close();
            releaseIfNeeded(lost.payload);
        }
    }

    @Test
    public void nackIsClassifiedBeforeAnyLaterPtoPromotion() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingMetrics metrics = new RecordingMetrics();
        Harness harness = harness(clock, metrics);
        TestDatagram lost = datagram(100, 0);
        try {
            harness.add(lost.packet);
            harness.arm();
            invokeNack(harness.codec, lost.packet, 200L);
            Assertions.assertEquals(1, harness.pending.size());
            Assertions.assertTrue(lost.packet.isModelLossClassified(),
                    "explicit loss is classified synchronously with the NACK");

            Assertions.assertEquals(1, invokeRecovery(harness.codec, harness.context, 200L));
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 1);

            Assertions.assertEquals(Arrays.asList(RakDatagramSendType.NACK_RETRANSMISSION), metrics.sendTypes);
            Assertions.assertTrue(harness.pending.isEmpty());
            Assertions.assertFalse(lost.packet.isModelLossClassified(),
                    "the replacement physical attempt starts with fresh loss-classification state");
        } finally {
            harness.close();
            releaseIfNeeded(lost.payload);
        }
    }

    @Test
    public void immediateNackSchedulerPreservesFifoWithinBoundedFlush() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingMetrics metrics = new RecordingMetrics();
        Harness harness = harness(clock, metrics);
        TestDatagram older = datagram(100, 0);
        TestDatagram newer = datagram(100, 1);
        try {
            newer.packet.setSendTime(100L);
            harness.add(older.packet);
            harness.add(newer.packet);
            harness.arm();

            invokeNack(harness.codec, newer.packet, 200L);
            invokeNack(harness.codec, older.packet, 200L);
            Assertions.assertEquals(2, harness.pending.size());
            Assertions.assertEquals(2, invokeRecovery(harness.codec, harness.context, 200L));
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 2);

            Assertions.assertEquals(1, older.packet.getRetransmissionCount());
            Assertions.assertEquals(1, newer.packet.getRetransmissionCount());
            Assertions.assertTrue(harness.pending.isEmpty());
            Assertions.assertEquals(Arrays.asList(RakDatagramSendType.NACK_RETRANSMISSION,
                    RakDatagramSendType.NACK_RETRANSMISSION), metrics.sendTypes);
        } finally {
            harness.close();
            releaseIfNeeded(older.payload);
            releaseIfNeeded(newer.payload);
        }
    }

    @Test
    public void modelPersistentBlackholeSendsOnlyBackedOffProbesAndSuppressesOriginals() throws Exception {
        AtomicLong clock = new AtomicLong();
        RecordingMetrics metrics = new RecordingMetrics();
        Harness harness = harness(clock, metrics);
        TestDatagram blackholed = datagram(100, 0);
        ByteBuf queuedPayload = Unpooled.buffer(100).writeByte(0x42).writeZero(99);
        try {
            harness.add(blackholed.packet);
            harness.arm();
            double preProbeCwnd = harness.window.getCongestionWindow();

            for (int probe = 0; probe < 2; probe++) {
                long due = harness.recovery.getNextProbeAtMillis();
                clock.set(due);
                Assertions.assertEquals(1, invokeRecovery(harness.codec, harness.context, due));
                harness.channel.flushOutbound();
                releaseOutbound(harness.channel, 1);
                Assertions.assertFalse(harness.window.isModelPersistentCongestion());
                Assertions.assertEquals(preProbeCwnd, harness.window.getCongestionWindow(),
                        "PTO expiry elicits progress but does not itself prove congestion loss");
                Assertions.assertEquals(0L, harness.window.getModelHardLossResponseCount());
                Assertions.assertEquals(0L, harness.window.getModelDelayLossResponseCount());
                Assertions.assertEquals(-1D, harness.window.getModelRecentLossRate());
                Assertions.assertFalse(harness.window.isInRecovery(),
                        "a model PTO does not open a NACK/loss recovery epoch");
                Assertions.assertEquals(blackholed.packet.getSize(), harness.window.getBytesInFlight(),
                        "each replacement PTO attempt remains charged exactly once");
                Assertions.assertEquals(blackholed.packet.getSize(), harness.window.getUnackedBytes());
            }

            ChannelPromise applicationPromise = harness.context.newPromise();
            harness.codec.write(harness.context,
                    new RakMessage(queuedPayload, RakReliability.RELIABLE, RakPriority.HIGH), applicationPromise);
            Assertions.assertTrue(applicationPromise.isSuccess());

            long persistentDue = harness.recovery.getNextProbeAtMillis();
            clock.set(persistentDue);
            invokeInternalFlush(harness.codec, harness.context);
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 1);

            Assertions.assertTrue(harness.window.isModelPersistentCongestion());
            Assertions.assertEquals(2D * MTU, harness.window.getCongestionWindow());
            Assertions.assertEquals(Arrays.asList(RakDatagramSendType.TIMEOUT_RETRANSMISSION,
                    RakDatagramSendType.TIMEOUT_RETRANSMISSION,
                    RakDatagramSendType.TIMEOUT_RETRANSMISSION), metrics.sendTypes);
            Assertions.assertTrue((Integer) get(harness.codec, "queuedBytes") > 0,
                    "ordinary traffic remains queued until ACK progress exits persistent congestion");
        } finally {
            harness.close();
            releaseIfNeeded(blackholed.payload);
            releaseIfNeeded(queuedPayload);
        }
    }

    @Test
    public void modelRetransmissionCallbackFailureRestoresDeliverySampleAndPacerState() throws Exception {
        AtomicLong clock = new AtomicLong();
        RakChannelMetrics throwingMetrics = new RakChannelMetrics() {
            @Override
            public void rakDatagramSent(RakDatagramSendType sendType, int bytes, int retransmissionAttempt,
                                        int bytesInFlight) {
                throw new IllegalStateException("model metrics failure");
            }
        };
        Harness harness = harness(clock, throwingMetrics);
        TestDatagram datagram = datagram(100, 0);
        try {
            harness.add(datagram.packet);
            harness.arm();
            long oldDelivered = datagram.packet.getDeliveredBytesAtSend();
            long oldDeliveredTime = datagram.packet.getDeliveredTimeAtSend();
            long oldFirstSend = datagram.packet.getFirstSendTime();
            long oldModelSend = datagram.packet.getModelSendTime();
            int oldModelInFlight = datagram.packet.getModelTxInFlight();
            Object modelController = get(harness.window, "modelController");
            long oldControllerDeliveredTime = (Long) get(modelController, "deliveredTimeMillis");
            int oldWriteIndex = (Integer) get(harness.codec, "datagramWriteIndex");
            long oldSendOrdinalCounter = (Long) get(harness.codec, "datagramSendOrdinal");

            invokeNack(harness.codec, datagram.packet, 200L);
            InvocationTargetException failure = Assertions.assertThrows(InvocationTargetException.class,
                    () -> recoveryMethod().invoke(harness.codec, harness.context, 250L, MTU));
            Assertions.assertEquals("model metrics failure", failure.getCause().getMessage());
            Assertions.assertSame(datagram.packet, harness.sent.get(0));
            Assertions.assertEquals(1, harness.pending.size());
            Assertions.assertEquals(oldDelivered, datagram.packet.getDeliveredBytesAtSend());
            Assertions.assertEquals(oldDeliveredTime, datagram.packet.getDeliveredTimeAtSend());
            Assertions.assertEquals(oldFirstSend, datagram.packet.getFirstSendTime());
            Assertions.assertEquals(oldModelSend, datagram.packet.getModelSendTime());
            Assertions.assertEquals(oldModelInFlight, datagram.packet.getModelTxInFlight());
            Assertions.assertEquals(oldControllerDeliveredTime, get(modelController, "deliveredTimeMillis"),
                    "a failed sole-flight retry restores the delivery-rate clock");
            Assertions.assertEquals(oldWriteIndex, get(harness.codec, "datagramWriteIndex"));
            Assertions.assertEquals(oldSendOrdinalCounter, get(harness.codec, "datagramSendOrdinal"));
            Assertions.assertEquals(0, datagram.packet.getRetransmissionCount());
            Assertions.assertFalse(datagram.packet.isInFlight());
            Assertions.assertTrue(datagram.packet.isRetransmissionPending());
            Assertions.assertEquals(1, datagram.packet.refCnt());
        } finally {
            harness.close();
            releaseIfNeeded(datagram.payload);
        }
    }

    @Test
    public void modelUnreliableOriginalCallbackFailureRollsBackScalarFlightPacerAndPayload() throws Exception {
        AtomicLong clock = new AtomicLong();
        RakChannelMetrics throwingMetrics = new RakChannelMetrics() {
            @Override
            public void rakDatagramSent(RakDatagramSendType sendType, int bytes, int retransmissionAttempt,
                                        int bytesInFlight) {
                if (sendType == RakDatagramSendType.ORIGINAL) {
                    throw new IllegalStateException("unreliable metrics failure");
                }
            }
        };
        Harness harness = harness(clock, throwingMetrics);
        ByteBuf payload = Unpooled.buffer(100).writeByte(0x44).writeZero(99);
        try {
            int initialAllowance = harness.window.getTransmissionBandwidth(0L);
            ChannelPromise promise = harness.context.newPromise();
            harness.codec.write(harness.context,
                    new RakMessage(payload, RakReliability.UNRELIABLE, RakPriority.HIGH), promise);
            Assertions.assertTrue(promise.isSuccess());

            Method sendDatagrams = RakSessionCodec.class.getDeclaredMethod("sendDatagrams",
                    ChannelHandlerContext.class, long.class, int.class);
            sendDatagrams.setAccessible(true);
            InvocationTargetException failure = Assertions.assertThrows(InvocationTargetException.class,
                    () -> sendDatagrams.invoke(harness.codec, harness.context, 0L, MTU));
            Assertions.assertEquals("unreliable metrics failure", failure.getCause().getMessage());

            Assertions.assertTrue(((IntObjectMap<?>) get(harness.codec, "modelDatagramSamples")).isEmpty());
            Assertions.assertTrue(((Queue<?>) get(harness.codec, "modelSampleExpiries")).isEmpty());
            Assertions.assertTrue(harness.sent.isEmpty());
            Assertions.assertEquals(0, harness.window.getBytesInFlight());
            Assertions.assertEquals(initialAllowance, harness.window.getTransmissionBandwidth(0L),
                    "a failed handoff restores the exact pacing credit available before the attempt");
            Assertions.assertEquals(-1D, harness.window.getModelBandwidthBytesPerMillis(),
                    "a never-handed-off datagram is not credited or recorded as loss");
            Assertions.assertEquals(0, payload.refCnt(),
                    "the failed original has no map or channel owner and must release its payload");
            Assertions.assertNull(harness.channel.readOutbound());
        } finally {
            harness.close();
            releaseIfNeeded(payload);
        }
    }

    @Test
    public void modelReliableOriginalCallbackFailureRollsBackMapFlightPacerOrdinalsAndPayload() throws Exception {
        AtomicLong clock = new AtomicLong();
        RakChannelMetrics throwingMetrics = new RakChannelMetrics() {
            @Override
            public void rakDatagramSent(RakDatagramSendType sendType, int bytes, int retransmissionAttempt,
                                        int bytesInFlight) {
                if (sendType == RakDatagramSendType.ORIGINAL) {
                    throw new IllegalStateException("reliable metrics failure");
                }
            }
        };
        Harness harness = harness(clock, throwingMetrics);
        ByteBuf payload = Unpooled.buffer(100).writeByte(0x45).writeZero(99);
        try {
            Object modelController = get(harness.window, "modelController");
            set(modelController, "continuouslyBacklogged", true);
            int initialAllowance = harness.window.getTransmissionBandwidth(0L);
            int initialWriteIndex = (Integer) get(harness.codec, "datagramWriteIndex");
            long initialSendOrdinal = (Long) get(harness.codec, "datagramSendOrdinal");
            ChannelPromise promise = harness.context.newPromise();
            harness.codec.write(harness.context,
                    new RakMessage(payload, RakReliability.RELIABLE, RakPriority.HIGH), promise);
            Assertions.assertTrue(promise.isSuccess());

            Method sendDatagrams = RakSessionCodec.class.getDeclaredMethod("sendDatagrams",
                    ChannelHandlerContext.class, long.class, int.class);
            sendDatagrams.setAccessible(true);
            InvocationTargetException failure = Assertions.assertThrows(InvocationTargetException.class,
                    () -> sendDatagrams.invoke(harness.codec, harness.context, 0L, MTU));
            Assertions.assertEquals("reliable metrics failure", failure.getCause().getMessage());

            Assertions.assertTrue(harness.sent.isEmpty());
            Assertions.assertEquals(-1L, harness.recovery.getNextProbeAtMillis());
            Assertions.assertEquals(0, harness.window.getBytesInFlight());
            Assertions.assertEquals(0, harness.window.getUnackedBytes());
            Assertions.assertEquals(initialWriteIndex, get(harness.codec, "datagramWriteIndex"));
            Assertions.assertEquals(initialSendOrdinal, get(harness.codec, "datagramSendOrdinal"));
            Assertions.assertEquals(initialAllowance, harness.window.getTransmissionBandwidth(0L),
                    "a failed reliable original restores exact pacing/backlog state");
            Assertions.assertTrue((Boolean) get(modelController, "continuouslyBacklogged"));
            Assertions.assertEquals(-1D, harness.window.getModelBandwidthBytesPerMillis());
            Assertions.assertEquals(0, payload.refCnt());
            Assertions.assertNull(harness.channel.readOutbound());
        } finally {
            harness.close();
            releaseIfNeeded(payload);
        }
    }

    @Test
    public void deferredPtoStillClassifiesLaterNackImmediately() throws Exception {
        AtomicLong clock = new AtomicLong();
        Harness harness = harness(clock, null);
        TestDatagram timeoutCandidate = datagram(1_050, 0);
        TestDatagram occupyingProbe = datagram(750, 1);
        TestDatagram filler = datagram(1_050, 2);
        try {
            harness.add(timeoutCandidate.packet);
            harness.add(occupyingProbe.packet);
            harness.add(filler.packet);
            harness.arm();
            harness.window.onBoundedLoss(occupyingProbe.packet, 2L);
            harness.window.onBoundedRetransmit(occupyingProbe.packet, true);
            occupyingProbe.packet.setNextSend(2_000L);
            filler.packet.setNextSend(2_000L);
            harness.window.onPersistentCongestion();
            harness.recovery.refreshProbeDeadline(harness.window, harness.sent.values());

            long due = harness.recovery.getNextProbeAtMillis();
            clock.set(due);
            Assertions.assertEquals(0, invokeRecovery(harness.codec, harness.context, due),
                    "the occupied one-probe exception must defer this PTO");
            Assertions.assertFalse(timeoutCandidate.packet.isInFlight());
            Assertions.assertFalse(timeoutCandidate.packet.isModelLossClassified());

            invokeNack(harness.codec, timeoutCandidate.packet, due);
            Assertions.assertTrue(timeoutCandidate.packet.isModelLossClassified(),
                    "explicit NACK loss is classified without a validation timer");
            Assertions.assertEquals(0, invokeRecovery(harness.codec, harness.context, due),
                    "classification occurs even while cwnd still defers the retransmission");
        } finally {
            harness.close();
            releaseIfNeeded(timeoutCandidate.payload);
            releaseIfNeeded(occupyingProbe.payload);
            releaseIfNeeded(filler.payload);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void modelUnreliableAckResetsPathWidePtoBackoffBeforeNextProbe() throws Exception {
        AtomicLong clock = new AtomicLong();
        Harness harness = harness(clock, null);
        TestDatagram blackholed = datagram(100, 0);
        ByteBuf unreliablePayload = Unpooled.buffer(100).writeByte(0x46).writeZero(99);
        try {
            harness.add(blackholed.packet);
            harness.arm();
            for (int probe = 0; probe < 2; probe++) {
                long due = harness.recovery.getNextProbeAtMillis();
                clock.set(due);
                Assertions.assertEquals(1, invokeRecovery(harness.codec, harness.context, due));
                harness.channel.flushOutbound();
                releaseOutbound(harness.channel, 1);
            }
            Assertions.assertEquals(2, harness.recovery.getPtoBackoff());

            ChannelPromise promise = harness.context.newPromise();
            harness.codec.write(harness.context,
                    new RakMessage(unreliablePayload, RakReliability.UNRELIABLE, RakPriority.HIGH), promise);
            Assertions.assertTrue(promise.isSuccess());
            Method sendDatagrams = RakSessionCodec.class.getDeclaredMethod("sendDatagrams",
                    ChannelHandlerContext.class, long.class, int.class);
            sendDatagrams.setAccessible(true);
            sendDatagrams.invoke(harness.codec, harness.context, clock.get(), MTU);
            harness.channel.flushOutbound();
            RakDatagramPacket outbound = harness.channel.readOutbound();
            Assertions.assertNotNull(outbound);
            int unreliableSequence = outbound.getSequenceIndex();
            outbound.release();

            Queue<IntRange> incomingAcks = (Queue<IntRange>) get(harness.codec, "incomingAcks");
            incomingAcks.offer(new IntRange(unreliableSequence));
            clock.incrementAndGet();
            invokeInternalFlush(harness.codec, harness.context);
            Assertions.assertEquals(0, harness.recovery.getPtoBackoff(),
                    "any acknowledged model datagram proves path progress");
            Assertions.assertFalse(harness.window.isModelPersistentCongestion());

            long nextDue = harness.recovery.getNextProbeAtMillis();
            clock.set(nextDue);
            Assertions.assertEquals(1, invokeRecovery(harness.codec, harness.context, nextDue));
            harness.channel.flushOutbound();
            releaseOutbound(harness.channel, 1);
            Assertions.assertFalse(harness.window.isModelPersistentCongestion(),
                    "the next reliable PTO starts a fresh backoff epoch");
        } finally {
            harness.close();
            releaseIfNeeded(blackholed.payload);
            releaseIfNeeded(unreliablePayload);
        }
    }

    @Test
    public void modelInternalFlushMarksFullyDrainedSenderIdle() throws Exception {
        AtomicLong clock = new AtomicLong();
        Harness harness = harness(clock, null);
        TestDatagram datagram = datagram(100, 0);
        try {
            harness.add(datagram.packet);
            harness.schedule(datagram.packet);
            Object modelController = get(harness.window, "modelController");
            Assertions.assertTrue((Boolean) get(modelController, "continuouslyBacklogged"));
            acknowledge(harness, datagram.packet, 1L);

            clock.set(2L);
            invokeInternalFlush(harness.codec, harness.context);
            Assertions.assertFalse((Boolean) get(modelController, "continuouslyBacklogged"),
                    "the real session flush clears catch-up credit after both queues drain");
        } finally {
            harness.close();
            releaseIfNeeded(datagram.payload);
        }
    }

    @Test
    public void terminalDisconnectIsHandedOffBeforeCloseWhileOrdinaryImmediateWaitsForCwnd() throws Exception {
        AtomicLong clock = new AtomicLong();
        Harness harness = harness(clock, null);
        List<TestDatagram> occupyingWindow = new ArrayList<>();
        try {
            for (int i = 0; i < 11; i++) {
                TestDatagram datagram = datagram(1_100, i);
                occupyingWindow.add(datagram);
                harness.add(datagram.packet);
            }
            harness.arm();

            clock.set(1L);
            ByteBuf applicationPayload = Unpooled.buffer(100).writeByte(0x42).writeZero(99);
            ChannelPromise applicationPromise = harness.context.newPromise();
            harness.codec.write(harness.context,
                    new RakMessage(applicationPayload, RakReliability.RELIABLE, RakPriority.IMMEDIATE),
                    applicationPromise);
            Assertions.assertTrue(applicationPromise.isSuccess());
            Assertions.assertNull(harness.channel.readOutbound(),
                    "ordinary reliable-immediate traffic remains behind occupied cwnd");
            Assertions.assertTrue((Integer) get(harness.codec, "queuedBytes") > 0);

            clock.set(2L);
            Method disconnect = RakSessionCodec.class.getDeclaredMethod("disconnect0", RakDisconnectReason.class);
            disconnect.setAccessible(true);
            ChannelPromise disconnectPromise = (ChannelPromise) disconnect.invoke(
                    harness.codec, RakDisconnectReason.DISCONNECTED);

            Assertions.assertTrue(disconnectPromise.isSuccess());
            RakDatagramPacket outbound = harness.channel.readOutbound();
            Assertions.assertNotNull(outbound,
                    "terminal notification is handed to the outbound channel before the close listener runs");
            Assertions.assertEquals(ID_DISCONNECTION_NOTIFICATION,
                    outbound.getPackets().get(0).getBuffer().getUnsignedByte(
                            outbound.getPackets().get(0).getBuffer().readerIndex()));
            Assertions.assertFalse(harness.channel.isOpen(), "disconnect promise listener closes after handoff");
            outbound.release();
        } finally {
            harness.close();
            for (TestDatagram datagram : occupyingWindow) {
                releaseIfNeeded(datagram.payload);
            }
        }
    }

    @Test
    public void modelImmediateUnreliableTrafficCannotBypassSharedPacerAndWindow() throws Exception {
        AtomicLong clock = new AtomicLong();
        Harness harness = harness(clock, null);
        List<TestDatagram> occupyingWindow = new ArrayList<>();
        ByteBuf applicationPayload = Unpooled.buffer(100).writeByte(0x43).writeZero(99);
        try {
            for (int i = 0; i < 11; i++) {
                TestDatagram datagram = datagram(1_100, i);
                occupyingWindow.add(datagram);
                harness.add(datagram.packet);
                harness.schedule(datagram.packet);
            }

            clock.set(1L);
            ChannelPromise promise = harness.context.newPromise();
            harness.codec.write(harness.context,
                    new RakMessage(applicationPayload, RakReliability.UNRELIABLE, RakPriority.IMMEDIATE), promise);

            Assertions.assertTrue(promise.isSuccess());
            Assertions.assertNull(harness.channel.readOutbound(),
                    "unreliable application data must not use sendImmediate to bypass model admission");
            Assertions.assertTrue((Integer) get(harness.codec, "queuedBytes") > 0);
        } finally {
            harness.close();
            for (TestDatagram datagram : occupyingWindow) {
                releaseIfNeeded(datagram.payload);
            }
            releaseIfNeeded(applicationPayload);
        }
    }

    private static Harness harness(AtomicLong clock, RakChannelMetrics metrics) throws Exception {
        ChannelDuplexHandler outboundHandler = new ChannelDuplexHandler();
        EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        embeddedChannel.pipeline().addLast(RakSessionCodec.NAME, outboundHandler);
        ChannelHandlerContext embeddedContext = embeddedChannel.pipeline().context(outboundHandler);

        RakChannelConfig config = (RakChannelConfig) Proxy.newProxyInstance(
                RakChannelConfig.class.getClassLoader(), new Class<?>[]{RakChannelConfig.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getMetrics")) {
                        return metrics;
                    }
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
                    if (method.getName().equals("pipeline") || method.getName().equals("rakPipeline")) {
                        return embeddedChannel.pipeline();
                    }
                    if (method.getName().equals("remoteAddress") || method.getName().equals("localAddress")) {
                        return new InetSocketAddress("127.0.0.1", 19132);
                    }
                    return defaultValue(method.getReturnType());
                });

        RakSessionCodec codec = new RakSessionCodec(channel, clock::get);
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        RakBoundedRecovery recovery = new RakBoundedRecovery(clock::get, () -> 0L);
        IntObjectMap<RakDatagramPacket> sent = new IntObjectHashMap<>();
        Queue<Object> pending = new ArrayDeque<>();

        set(codec, "slidingWindow", window);
        set(codec, "boundedRecovery", recovery);
        set(codec, "sentDatagrams", sent);
        set(codec, "modelDatagramSamples", new IntObjectHashMap<>());
        set(codec, "modelSampleExpiries", new PriorityQueue<>());
        set(codec, "pendingRetransmissions", pending);
        set(codec, "incomingAcks", new ArrayDeque<>());
        set(codec, "incomingNaks", new ArrayDeque<>());
        set(codec, "outgoingAcks", new ArrayDeque<>());
        set(codec, "outgoingNaks", new ArrayDeque<>());
        set(codec, "datagramWriteIndex", 3);
        set(codec, "datagramSendOrdinal", 3L);
        set(codec, "outgoingPackets", new FastWeightedFairQueue<EncapsulatedPacket>(RakPriority.values().length));
        set(codec, "outgoingPacketNextWeights", new long[4]);
        set(codec, "orderWriteIndex", new int[16]);
        set(codec, "state", RakState.CONNECTED);
        Method initWeights = RakSessionCodec.class.getDeclaredMethod("initOutgoingPacketWeights");
        initWeights.setAccessible(true);
        initWeights.invoke(codec);

        return new Harness(codec, window, recovery, sent, pending, embeddedChannel, embeddedContext);
    }

    private static void invokeNack(RakSessionCodec codec, RakDatagramPacket packet, long time) throws Exception {
        Method method = RakSessionCodec.class.getDeclaredMethod("onIncomingNack",
                ChannelHandlerContext.class, RakDatagramPacket.class, long.class);
        method.setAccessible(true);
        method.invoke(codec, null, packet, time);
    }

    private static int invokeRecovery(RakSessionCodec codec, ChannelHandlerContext context, long time)
            throws Exception {
        return (Integer) recoveryMethod().invoke(codec, context, time, MTU);
    }

    private static Method recoveryMethod() throws NoSuchMethodException {
        Method method = RakSessionCodec.class.getDeclaredMethod("sendBoundedRecovery",
                ChannelHandlerContext.class, long.class, int.class);
        method.setAccessible(true);
        return method;
    }

    private static void invokeAck(RakSessionCodec codec, RakDatagramPacket packet, long time) throws Exception {
        Method method = RakSessionCodec.class.getDeclaredMethod("onIncomingAck", RakDatagramPacket.class, long.class);
        method.setAccessible(true);
        method.invoke(codec, packet, time);
    }

    private static void acknowledge(Harness harness, RakDatagramPacket packet, long time) throws Exception {
        Assertions.assertSame(packet, harness.sent.remove(packet.getSequenceIndex()));
        invokeAck(harness.codec, packet, time);
    }

    private static void invokeInternalFlush(RakSessionCodec codec, ChannelHandlerContext context) throws Exception {
        Method method = RakSessionCodec.class.getDeclaredMethod("internalFlush", ChannelHandlerContext.class);
        method.setAccessible(true);
        method.invoke(codec, context);
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object get(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
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

    private static TestDatagram datagram(int payloadBytes, int sequenceIndex) {
        ByteBuf payload = Unpooled.buffer(payloadBytes).writeZero(payloadBytes);
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setReliability(RakReliability.RELIABLE);
        packet.setBuffer(payload);

        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        Assertions.assertTrue(datagram.tryAddPacket(packet, MTU));
        datagram.setSequenceIndex(sequenceIndex);
        datagram.setSendOrdinal(sequenceIndex);
        datagram.setSendTime(0L);
        return new TestDatagram(datagram, payload);
    }

    private static void releaseOutbound(EmbeddedChannel channel, int expected) {
        for (int i = 0; i < expected; i++) {
            RakDatagramPacket outbound = channel.readOutbound();
            Assertions.assertNotNull(outbound);
            outbound.release();
        }
        Assertions.assertNull(channel.readOutbound());
    }

    private static void releaseIfNeeded(ByteBuf payload) {
        if (payload.refCnt() > 0) {
            payload.release(payload.refCnt());
        }
    }

    private static final class RecordingMetrics implements RakChannelMetrics {
        private final List<RakDatagramSendType> sendTypes = new ArrayList<>();

        @Override
        public void rakDatagramSent(RakDatagramSendType sendType, int bytes, int retransmissionAttempt,
                                    int bytesInFlight) {
            this.sendTypes.add(sendType);
        }
    }

    private static final class TestDatagram {
        private final RakDatagramPacket packet;
        private final ByteBuf payload;

        private TestDatagram(RakDatagramPacket packet, ByteBuf payload) {
            this.packet = packet;
            this.payload = payload;
        }
    }

    private static final class Harness {
        private final RakSessionCodec codec;
        private final RakSlidingWindow window;
        private final RakBoundedRecovery recovery;
        private final IntObjectMap<RakDatagramPacket> sent;
        private final Queue<Object> pending;
        private final EmbeddedChannel channel;
        private final ChannelHandlerContext context;
        private boolean codecClosed;

        private Harness(RakSessionCodec codec, RakSlidingWindow window, RakBoundedRecovery recovery,
                        IntObjectMap<RakDatagramPacket> sent, Queue<Object> pending,
                        EmbeddedChannel channel, ChannelHandlerContext context) {
            this.codec = codec;
            this.window = window;
            this.recovery = recovery;
            this.sent = sent;
            this.pending = pending;
            this.channel = channel;
            this.context = context;
        }

        private void add(RakDatagramPacket packet) {
            this.sent.put(packet.getSequenceIndex(), packet);
            this.window.onReliableSend(packet);
        }

        private void arm() {
            for (RakDatagramPacket packet : this.sent.values()) {
                this.schedule(packet);
            }
        }

        private void schedule(RakDatagramPacket packet) {
            this.recovery.onReliableSend(this.window, packet);
        }

        private void closeCodec() throws Exception {
            if (this.codecClosed) {
                return;
            }
            this.codec.closeAfterEventLoopTermination();
            this.codecClosed = true;
        }

        private void close() throws Exception {
            this.closeCodec();
            this.channel.finishAndReleaseAll();
        }
    }
}
