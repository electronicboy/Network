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

package org.cloudburstmc.netty.channel.raknet;

import io.netty.buffer.Unpooled;
import org.cloudburstmc.netty.channel.raknet.packet.EncapsulatedPacket;
import org.cloudburstmc.netty.channel.raknet.packet.RakDatagramPacket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class RakSlidingWindowModelTests {
    private static final int MTU = 1_200;

    @Test
    public void oneMillisecondPathWithTenMillisecondSendQuantumSustainsSerializedFiveMegabits() {
        QuantizedSendResult result = simulateQuantizedShortRtt();
        Assertions.assertTrue(result.measuredMbps >= 4.5D,
                () -> "send-quantized model collapsed below the serialized offer: "
                        + result.measuredMbps + " Mbps");
        Assertions.assertEquals(1L, result.minimumObservedRttMillis,
                "send-quantum accounting must not rewrite the raw propagation RTT");
        Assertions.assertEquals(1L, result.finalMinimumRttMillis,
                "serialized data must not replace the raw minimum with the effective send quantum");
        Assertions.assertTrue(result.finalBandwidthBytesPerMillis >= 500D
                        && result.finalBandwidthBytesPerMillis <= 700D,
                () -> "delivery estimator diverged from the 625 B/ms path: "
                        + result.finalBandwidthBytesPerMillis + " B/ms");
        Assertions.assertTrue(result.finalCwnd >= 8D * MTU && result.finalCwnd <= 24D * MTU,
                () -> "effective-RTT BDP window is unreasonable: " + result.finalCwnd + " bytes");
        Assertions.assertTrue(result.maxBytesSentInTick <= 8 * MTU,
                "accounting for the send quantum must retain the bounded catch-up burst");
        Assertions.assertTrue(result.finalQueuedBytes < 100_000L,
                () -> "a healthy saturated sender retained a growing queue: " + result.finalQueuedBytes);
    }

    @Test
    public void backloggedPacerRetainsBoundedCreditAcrossDelayedSendActivations() {
        QuantizedSendResult result = simulateQuantizedShortRtt(true, true);
        Assertions.assertTrue(result.measuredMbps >= 4.5D,
                () -> "alternating 10/15 ms activations discarded continuously-backlogged pacing credit: "
                        + result.measuredMbps + " Mbps");
        Assertions.assertTrue(result.maxBytesSentInTick <= 8 * MTU,
                "delayed activation credit must retain the absolute eight-MTU burst ceiling");
        Assertions.assertTrue(result.finalQueuedBytes < 200_000L,
                () -> "scheduler slippage created a growing application queue: " + result.finalQueuedBytes);
    }

    private static QuantizedSendResult simulateQuantizedShortRtt() {
        return simulateQuantizedShortRtt(false, false);
    }

    private static QuantizedSendResult simulateQuantizedShortRtt(boolean delayedActivations,
                                                                 boolean continuouslyBacklogged) {
        RakSlidingWindow window = new RakSlidingWindow(MTU, 10L);
        PriorityQueue<Delivery> deliveries = new PriorityQueue<>(Comparator
                .comparingLong((Delivery value) -> value.at)
                .thenComparingLong(value -> value.datagram.getSendOrdinal()));
        long offeredCredit = 0L;
        long deliveredMeasurementBytes = 0L;
        long sendOrdinal = 0L;
        long minimumObservedRttMillis = Long.MAX_VALUE;
        double nextLinkAvailableMillis = 10D;
        int maxBytesSentInTick = 0;
        final int payloadBytes = 1_000;
        final long offeredBytesPerSecond = 625_000L;
        final double linkBytesPerMillis = 625D;
        final long measurementStartMillis = 2_000L;
        final long measurementEndMillis = 12_000L;
        long nextSendActivationMillis = 10L;
        int activationIndex = 0;

        try {
            // A small pre-data flight records the true 1 ms propagation minimum. Its serialization is below this
            // controller's millisecond clock resolution; saturated data below still traverses the serialized link.
            RakDatagramPacket handshake = datagram(1);
            handshake.setSequenceIndex(0);
            handshake.setSendOrdinal(sendOrdinal++);
            handshake.setSendTime(0L);
            window.onReliableSend(handshake, true);
            window.onAck(1L, handshake, sendOrdinal);
            minimumObservedRttMillis = window.getModelMinimumRttMillis();
            handshake.release();

            for (long now = 10L; now <= measurementEndMillis + 100L; now++) {
                while (!deliveries.isEmpty() && deliveries.peek().at <= now) {
                    Delivery delivery = deliveries.poll();
                    window.onAck(now, delivery.datagram, sendOrdinal);
                    minimumObservedRttMillis = Math.min(minimumObservedRttMillis,
                            window.getModelMinimumRttMillis());
                    if (now >= measurementStartMillis && now < measurementEndMillis) {
                        deliveredMeasurementBytes += payloadBytes;
                    }
                    releaseIfNeeded(delivery.datagram);
                }
                if (now >= measurementEndMillis) {
                    continue;
                }

                offeredCredit += offeredBytesPerSecond;
                if (now != nextSendActivationMillis) {
                    continue;
                }
                long nextDelay = delayedActivations && (activationIndex++ & 1) == 0 ? 15L : 10L;
                nextSendActivationMillis += nextDelay;
                int bytesSentInTick = 0;
                while (offeredCredit >= payloadBytes * 1_000L) {
                    RakDatagramPacket datagram = datagram(payloadBytes);
                    if (window.getTransmissionBandwidth(now) < datagram.getSize()) {
                        datagram.release();
                        break;
                    }
                    datagram.setSequenceIndex((int) sendOrdinal);
                    datagram.setSendOrdinal(sendOrdinal++);
                    datagram.setSendTime(now);
                    offeredCredit -= payloadBytes * 1_000L;
                    window.onReliableSend(datagram, !continuouslyBacklogged
                            && offeredCredit < payloadBytes * 1_000L);
                    bytesSentInTick += datagram.getSize();

                    double serviceStartedAt = Math.max(now, nextLinkAvailableMillis);
                    nextLinkAvailableMillis = serviceStartedAt + datagram.getSize() / linkBytesPerMillis;
                    long acknowledgeAt = (long) Math.ceil(nextLinkAvailableMillis + 1D);
                    deliveries.offer(new Delivery(acknowledgeAt, datagram, false));
                }
                maxBytesSentInTick = Math.max(maxBytesSentInTick, bytesSentInTick);
            }

            long applicationQueuedBytes = offeredCredit / 1_000L;
            long linkQueuedBytes = (long) Math.ceil(
                    Math.max(0D, nextLinkAvailableMillis - measurementEndMillis) * linkBytesPerMillis);
            return new QuantizedSendResult(
                    deliveredMeasurementBytes * 8D
                            / ((measurementEndMillis - measurementStartMillis) * 1_000D),
                    window.getCongestionWindow(), minimumObservedRttMillis, maxBytesSentInTick,
                    applicationQueuedBytes + linkQueuedBytes, window.getModelBandwidthBytesPerMillis(),
                    window.getModelMinimumRttMillis());
        } finally {
            while (!deliveries.isEmpty()) {
                releaseIfNeeded(deliveries.poll().datagram);
            }
            window.close();
        }
    }

    @Test
    public void ackCompressedFlightCannotSampleFasterThanItWasSent() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> flight = new ArrayList<>();
        int flightBytes = 0;
        try {
            for (int i = 0; i < 10; i++) {
                RakDatagramPacket datagram = datagram(1_000);
                datagram.setSequenceIndex(i);
                datagram.setSendOrdinal(i);
                datagram.setSendTime(100L + i);
                window.onReliableSend(datagram);
                flight.add(datagram);
                flightBytes += datagram.getSize();
            }

            for (int i = 0; i < flight.size(); i++) {
                window.onAck(300L, flight.get(i), i + 1L);
            }

            double maximumPossibleRate = (double) flightBytes / (300L - 109L);
            Assertions.assertTrue(window.getModelBandwidthBytesPerMillis() > 0D);
            Assertions.assertTrue(window.getModelBandwidthBytesPerMillis() <= maximumPossibleRate,
                    "one-timestamp ACK compression must be capped by the flight's send/delivery interval");
            Assertions.assertEquals(191L, window.getModelMinimumRttMillis());
        } finally {
            for (RakDatagramPacket datagram : flight) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void twoHundredRoundFivePercentLossWithRetriesDoesNotDecayOrDiverge() {
        double sum = 0D;
        double sumSquares = 0D;
        for (int lossPhase = 0; lossPhase < 5; lossPhase++) {
            SimulationResult result = simulatePeriodicLoss(lossPhase);
            Assertions.assertTrue(result.measuredMbps >= 3.5D,
                    () -> "model prototype did not fill the target BDP: " + result.measuredMbps + " Mbps");
            Assertions.assertTrue(result.finalCwnd >= 93_000D,
                    "the model must learn at least the target path BDP instead of a static floor");
            Assertions.assertTrue(result.maxBytesSentInTick <= 8 * MTU,
                    "event-loop delay must not release an unbounded catch-up burst");
            Assertions.assertTrue(result.rounds >= 200L,
                    () -> "the retrying simulation must outlive the ten-round bandwidth filter many times: "
                            + result.rounds + " rounds, rate=" + result.measuredMbps
                            + ", responses=" + result.lossResponses);
            Assertions.assertTrue(result.retransmissions > 0,
                    "the shared pacer must carry actual physical retry traffic");
            Assertions.assertTrue(result.maxPendingRetries < 100,
                    "bounded recovery must keep up with deterministic five-percent loss");
            Assertions.assertTrue(result.lossResponses <= 1L,
                    "continuing five-percent loss must not repeatedly decay the same peer");
            sum += result.measuredMbps;
            sumSquares += result.measuredMbps * result.measuredMbps;
        }
        double fairness = sum * sum / (5D * sumSquares);
        Assertions.assertTrue(fairness >= 0.99D,
                () -> "loss phase must not lock otherwise identical peers into divergent windows: " + fairness);
    }

    @Test
    public void longMixedReliableUnreliableTrafficDoesNotUnderestimateSharedPacer() {
        double sum = 0D;
        double sumSquares = 0D;
        int retransmissions = 0;
        for (int lossPhase = 0; lossPhase < 5; lossPhase++) {
            SimulationResult result = simulateMixedTraffic(lossPhase);
            Assertions.assertTrue(result.measuredMbps >= 3.5D,
                    () -> "mixed traffic decayed below target: " + result.measuredMbps + " Mbps");
            Assertions.assertTrue(result.finalCwnd >= 93_000D);
            Assertions.assertTrue(result.rounds >= 200L);
            Assertions.assertTrue(result.maxPendingRetries < 100);
            Assertions.assertTrue(result.maxBytesSentInTick <= 8 * MTU);
            sum += result.measuredMbps;
            sumSquares += result.measuredMbps * result.measuredMbps;
            retransmissions += result.retransmissions;
        }
        Assertions.assertTrue(retransmissions > 0,
                "at least one loss phase must exercise reliable retries beside unreliable samples");
        double fairness = sum * sum / (5D * sumSquares);
        Assertions.assertTrue(fairness >= 0.99D,
                () -> "mixed traffic loss phase must not create controller divergence: " + fairness);
    }

    private static SimulationResult simulateMixedTraffic(int lossPhase) {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        PriorityQueue<MixedDelivery> deliveries = new PriorityQueue<>(Comparator.comparingLong(value -> value.at));
        Queue<RakDatagramPacket> pendingRetries = new ArrayDeque<>();
        long offeredCredit = 0L;
        long deliveredMeasurementBytes = 0L;
        long sendOrdinal = 0L;
        int physicalDatagrams = 0;
        int logicalDatagrams = 0;
        int retransmissions = 0;
        int maxPendingRetries = 0;
        int maxBytesSentInTick = 0;
        final int payloadBytes = 1_000;
        final int unreliableDatagramBytes = 1_011;
        final int pathRttMillis = 213;
        final long offeredBytesPerSecond = 625_000L;
        final long measurementStartMillis = 5_000L;
        final long measurementEndMillis = 50_000L;

        try {
            for (long now = 0L; now <= measurementEndMillis + pathRttMillis; now++) {
                while (!deliveries.isEmpty() && deliveries.peek().at <= now) {
                    MixedDelivery delivery = deliveries.poll();
                    if (delivery.lost) {
                        if (delivery.datagram != null) {
                            window.onBoundedLoss(delivery.datagram, sendOrdinal - 1L);
                            pendingRetries.offer(delivery.datagram);
                        } else {
                            window.onUnreliableLoss(delivery.sample);
                        }
                    } else {
                        if (delivery.datagram != null) {
                            window.onAck(now, delivery.datagram, sendOrdinal);
                            releaseIfNeeded(delivery.datagram);
                        } else {
                            window.onUnreliableAck(delivery.sample, now);
                        }
                        if (now >= measurementStartMillis && now < measurementEndMillis) {
                            deliveredMeasurementBytes += payloadBytes;
                        }
                    }
                }
                if (now >= measurementEndMillis) {
                    continue;
                }

                offeredCredit += offeredBytesPerSecond;
                int bytesSentInTick = 0;
                if (now % 10L != 0L) {
                    continue;
                }
                int retriesThisFlush = 0;
                while (retriesThisFlush < 2 && !pendingRetries.isEmpty()) {
                    RakDatagramPacket datagram = pendingRetries.peek();
                    if (!window.canSendBoundedRecovery(datagram.getSize(), now)) {
                        break;
                    }
                    pendingRetries.poll();
                    window.onBoundedRetransmit(datagram, false, now, pendingRetries.isEmpty());
                    datagram.setSendTime(now);
                    datagram.setSendOrdinal(sendOrdinal++);
                    datagram.markRetransmitted();
                    bytesSentInTick += datagram.getSize();
                    retransmissions++;
                    physicalDatagrams++;
                    deliveries.offer(MixedDelivery.reliable(now + pathRttMillis, datagram,
                            (physicalDatagrams + lossPhase) % 20 == 0));
                    retriesThisFlush++;
                }
                maxPendingRetries = Math.max(maxPendingRetries, pendingRetries.size());
                if (!pendingRetries.isEmpty()) {
                    maxBytesSentInTick = Math.max(maxBytesSentInTick, bytesSentInTick);
                    continue;
                }

                while (offeredCredit >= payloadBytes * 1_000L) {
                    boolean unreliable = (logicalDatagrams++ & 1) != 0;
                    if (unreliable) {
                        if (window.getTransmissionBandwidth(now) < unreliableDatagramBytes) {
                            logicalDatagrams--;
                            break;
                        }
                        RakSlidingWindow.ModelDatagramSample sample = window.onUnreliableSendTracked(
                                unreliableDatagramBytes, now, false);
                        physicalDatagrams++;
                        deliveries.offer(MixedDelivery.unreliable(now + pathRttMillis, sample,
                                (physicalDatagrams + lossPhase) % 20 == 0));
                        bytesSentInTick += unreliableDatagramBytes;
                    } else {
                        RakDatagramPacket datagram = datagram(payloadBytes);
                        int size = datagram.getSize();
                        if (window.getTransmissionBandwidth(now) < size) {
                            datagram.release();
                            logicalDatagrams--;
                            break;
                        }
                        datagram.setSequenceIndex((int) sendOrdinal);
                        datagram.setSendOrdinal(sendOrdinal++);
                        datagram.setSendTime(now);
                        window.onReliableSend(datagram);
                        physicalDatagrams++;
                        deliveries.offer(MixedDelivery.reliable(now + pathRttMillis, datagram,
                                (physicalDatagrams + lossPhase) % 20 == 0));
                        bytesSentInTick += size;
                    }
                    offeredCredit -= payloadBytes * 1_000L;
                }
                maxBytesSentInTick = Math.max(maxBytesSentInTick, bytesSentInTick);
            }

            double measuredMbps = deliveredMeasurementBytes * 8D
                    / ((measurementEndMillis - measurementStartMillis) * 1_000D);
            return new SimulationResult(measuredMbps, window.getCongestionWindow(), maxBytesSentInTick,
                    window.getModelRoundCount(), retransmissions, maxPendingRetries);
        } finally {
            while (!deliveries.isEmpty()) {
                MixedDelivery delivery = deliveries.poll();
                if (delivery.datagram != null) {
                    releaseIfNeeded(delivery.datagram);
                } else {
                    window.onUnreliableLoss(delivery.sample);
                }
            }
            while (!pendingRetries.isEmpty()) {
                releaseIfNeeded(pendingRetries.poll());
            }
            window.close();
        }
    }

    @Test
    public void cleanHandshakeToImpairedPathProbeAvoidsRepeatedFalseLossCollapse() {
        SimulationResult result = simulatePeriodicLoss(0, true);
        Assertions.assertTrue(result.measuredMbps >= 3.5D,
                () -> "clean-handshake RTT poisoned impaired-path control: " + result.measuredMbps + " Mbps");
        Assertions.assertTrue(result.finalCwnd >= 93_000D);
        Assertions.assertTrue(result.retransmissions > 0);
        Assertions.assertTrue(result.rounds >= 200L,
                () -> "impaired replay ended after " + result.rounds + " rounds, responses="
                        + result.lossResponses + ", rate=" + result.measuredMbps);
    }

    @Test
    public void tinyHandshakeAndFirstFlightLossesReachImpairedRateWithinThreeSeconds() {
        SimulationResult terminalBaseline = simulatePeriodicLoss(
                0, true, -1, false, 2_750L, 3_000L);
        SimulationResult sustainedBaseline = simulatePeriodicLoss(
                0, true, -1, false, 3_000L, 4_500L);
        double sum = 0D;
        double sumSquares = 0D;
        for (int peer = 0; peer < 4; peer++) {
            int lossPosition = 1 + (peer & 1);
            boolean ackLoss = peer >= 2;
            SimulationResult terminal = simulatePeriodicLoss(0, true, lossPosition, ackLoss, 2_750L, 3_000L);
            Assertions.assertTrue(terminal.measuredMbps >= terminalBaseline.measuredMbps * 0.90D,
                    () -> "peer missed three-second warmup after early " + (ackLoss ? "ACK" : "data")
                            + " loss at packet " + lossPosition + ": " + terminal.measuredMbps
                            + " Mbps versus " + terminalBaseline.measuredMbps
                            + " Mbps baseline in the terminal 250 ms, minRTT="
                            + terminal.finalMinimumRttMillis
                            + " ms, cwnd=" + terminal.finalCwnd
                            + ", lossResponses=" + terminal.lossResponses
                            + " (hard=" + terminal.hardLossResponses
                            + ", delay=" + terminal.delayLossResponses + ")");

            SimulationResult sustained = simulatePeriodicLoss(0, true, lossPosition, ackLoss, 3_000L, 4_500L);
            Assertions.assertTrue(sustained.measuredMbps >= sustainedBaseline.measuredMbps * 0.90D,
                    () -> "peer did not sustain its recovered rate for 1.5 seconds after warmup: "
                            + sustained.measuredMbps + " Mbps versus "
                            + sustainedBaseline.measuredMbps + " Mbps baseline, cwnd=" + sustained.finalCwnd
                            + ", bandwidth=" + sustained.finalBandwidthBytesPerMillis
                            + ", pacing=" + sustained.finalPacingBytesPerMillis
                            + ", lossResponses=" + sustained.lossResponses
                            + " (hard=" + sustained.hardLossResponses
                            + ", delay=" + sustained.delayLossResponses + ")");
            Assertions.assertTrue(sustained.finalMinimumRttMillis >= 190L
                            && sustained.finalMinimumRttMillis <= 230L,
                    () -> "peer retained the tiny-handshake RTT: "
                            + sustained.finalMinimumRttMillis + " ms");
            Assertions.assertTrue(Math.max(terminal.maxBytesSentInTick, sustained.maxBytesSentInTick) <= 8 * MTU);
            Assertions.assertTrue(Math.max(terminal.maxPendingRetries, sustained.maxPendingRetries) < 100);
            Assertions.assertTrue(sustained.retransmissions > 0,
                    "periodic loss and an omitted early ACK must exercise bounded reliable retry");
            Assertions.assertTrue(sustained.lossResponses <= 1L,
                    () -> "early loss cannot manufacture repeated responses during one continuing epoch: "
                            + sustained.lossResponses + " (hard=" + sustained.hardLossResponses
                            + ", delay=" + sustained.delayLossResponses + ")");
            sum += sustained.measuredMbps;
            sumSquares += sustained.measuredMbps * sustained.measuredMbps;
        }
        double fairness = sum * sum / (4D * sumSquares);
        Assertions.assertTrue(fairness >= 0.99D,
                () -> "first-flight data/ACK loss created warmup divergence: " + fairness);
    }

    @Test
    public void probeCycleRediscoversCapacityAndIdleRestartKeepsBoundedBurst() {
        CapacityStepResult result = simulateCapacityStepAndIdleRestart();
        Assertions.assertTrue(result.stepUpMbps >= 3.5D,
                () -> "steady probing failed to rediscover added capacity: " + result.stepUpMbps + " Mbps");
        Assertions.assertTrue(result.restartMbps >= 3.5D,
                () -> "an app-limited idle period left a stale low-rate restart: " + result.restartMbps + " Mbps");
        Assertions.assertTrue(result.maxBytesSentInTick <= 8 * MTU,
                "capacity growth and idle credit remain within one bounded pacing quantum");
        Assertions.assertTrue(result.finalBandwidthBytesPerMillis <= 1_250D,
                "idle/app-limited sampling must not inflate the five-megabit path model");
    }

    private static CapacityStepResult simulateCapacityStepAndIdleRestart() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        PriorityQueue<Delivery> deliveries = new PriorityQueue<>(Comparator.comparingLong(value -> value.at));
        long offeredCredit = 0L;
        long nextLinkAvailable = 0L;
        long sendOrdinal = 0L;
        long stepDelivered = 0L;
        long restartDelivered = 0L;
        int maxBytesSentInTick = 0;
        final int payloadBytes = 1_000;
        final long lowRateBytesPerSecond = 125_000L;
        final long highRateBytesPerSecond = 625_000L;
        final long capacityStepAt = 12_000L;
        final long idleAt = 30_000L;
        final long restartAt = 35_000L;
        final long endAt = 42_000L;
        final long stepMeasureAt = 25_000L;
        final long restartMeasureAt = 37_000L;

        try {
            for (long now = 0L; now <= endAt + 500L; now++) {
                while (!deliveries.isEmpty() && deliveries.peek().at <= now) {
                    Delivery delivery = deliveries.poll();
                    window.onAck(now, delivery.datagram, sendOrdinal);
                    if (now >= stepMeasureAt && now < idleAt) {
                        stepDelivered += payloadBytes;
                    }
                    if (now >= restartMeasureAt && now < endAt) {
                        restartDelivered += payloadBytes;
                    }
                    releaseIfNeeded(delivery.datagram);
                }
                if (now >= endAt) {
                    continue;
                }

                long offeredRate = now < capacityStepAt ? lowRateBytesPerSecond
                        : now < idleAt ? highRateBytesPerSecond
                        : now < restartAt ? 0L : highRateBytesPerSecond;
                double capacityBytesPerMillis = now < capacityStepAt ? 125D : 625D;
                offeredCredit += offeredRate;
                int bytesSentInTick = 0;
                if (now % 10L != 0L) {
                    continue;
                }
                while (offeredCredit >= payloadBytes * 1_000L) {
                    RakDatagramPacket datagram = datagram(payloadBytes);
                    int datagramSize = datagram.getSize();
                    if (window.getTransmissionBandwidth(now) < datagramSize) {
                        datagram.release();
                        break;
                    }
                    offeredCredit -= payloadBytes * 1_000L;
                    datagram.setSequenceIndex((int) sendOrdinal);
                    datagram.setSendOrdinal(sendOrdinal++);
                    datagram.setSendTime(now);
                    boolean appLimited = offeredCredit < payloadBytes * 1_000L;
                    window.onReliableSend(datagram, appLimited);
                    bytesSentInTick += datagramSize;

                    long serviceStart = Math.max(now, nextLinkAvailable);
                    long serializationMillis = Math.max(1L,
                            (long) Math.ceil(datagramSize / capacityBytesPerMillis));
                    nextLinkAvailable = serviceStart + serializationMillis;
                    deliveries.offer(new Delivery(nextLinkAvailable + 100L, datagram, false));
                }
                maxBytesSentInTick = Math.max(maxBytesSentInTick, bytesSentInTick);
            }

            return new CapacityStepResult(
                    stepDelivered * 8D / ((idleAt - stepMeasureAt) * 1_000D),
                    restartDelivered * 8D / ((endAt - restartMeasureAt) * 1_000D),
                    maxBytesSentInTick, window.getModelBandwidthBytesPerMillis());
        } finally {
            while (!deliveries.isEmpty()) {
                releaseIfNeeded(deliveries.poll().datagram);
            }
            window.close();
        }
    }

    private static SimulationResult simulatePeriodicLoss(int lossPhase) {
        return simulatePeriodicLoss(lossPhase, false, -1, false, 5_000L, 50_000L);
    }

    private static SimulationResult simulatePeriodicLoss(int lossPhase, boolean cleanHandshake) {
        return simulatePeriodicLoss(lossPhase, cleanHandshake, -1, false, 5_000L, 50_000L);
    }

    private static SimulationResult simulatePeriodicLoss(int lossPhase, boolean cleanHandshake,
                                                         int firstLossPacket, boolean firstLossIsAck,
                                                         long measurementStartMillis,
                                                         long measurementEndMillis) {
        return simulateLoss(cleanHandshake, firstLossPacket, firstLossIsAck,
                measurementStartMillis, measurementEndMillis,
                (sentDatagrams, sendAt) -> isPeriodicLoss(sentDatagrams, lossPhase, firstLossPacket));
    }

    private static SimulationResult simulateLoss(boolean cleanHandshake,
                                                 int firstLossPacket, boolean firstLossIsAck,
                                                 long measurementStartMillis,
                                                 long measurementEndMillis,
                                                 LossPattern lossPattern) {
        return simulateLoss(cleanHandshake, firstLossPacket, firstLossIsAck, measurementStartMillis,
                measurementEndMillis, 1_000, lossPattern);
    }

    private static SimulationResult simulateLoss(boolean cleanHandshake,
                                                 int firstLossPacket, boolean firstLossIsAck,
                                                 long measurementStartMillis,
                                                 long measurementEndMillis,
                                                 int payloadBytes,
                                                 LossPattern lossPattern) {
        return simulateLoss(cleanHandshake, firstLossPacket, firstLossIsAck, measurementStartMillis,
                measurementEndMillis, payloadBytes, 5L, 625D,
                (sentDatagrams, sendAt) -> 200L, lossPattern);
    }

    private static SimulationResult simulateLoss(boolean cleanHandshake,
                                                 int firstLossPacket, boolean firstLossIsAck,
                                                 long measurementStartMillis,
                                                 long measurementEndMillis,
                                                 int payloadBytes,
                                                 long handshakeRttMillis,
                                                 double capacityBytesPerMillis,
                                                 PathRttPattern pathRttPattern,
                                                 LossPattern lossPattern) {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        PriorityQueue<Delivery> deliveries = new PriorityQueue<>(deliveryOrder());
        Queue<RakDatagramPacket> pendingRetries = new ArrayDeque<>();
        Set<Integer> receiverDelivered = new HashSet<>();
        long offeredCredit = 0L;
        long deliveredMeasurementBytes = 0L;
        long sendOrdinal = 0L;
        int sentDatagrams = 0;
        int retransmissions = 0;
        int maxPendingRetries = 0;
        int maxBytesSentInTick = 0;
        long pathRebasedAtMillis = -1L;
        long minimumCwndSinceMillis = -1L;
        long maximumMinimumCwndDurationMillis = 0L;
        final long offeredBytesPerSecond = 625_000L;
        long nextLinkAvailable = 0L;

        try {
            if (cleanHandshake) {
                RakDatagramPacket handshake = orderedDatagram(1);
                handshake.setSequenceIndex(0);
                handshake.setSendOrdinal(sendOrdinal++);
                handshake.setSendTime(0L);
                window.onReliableSend(handshake, true);
                window.onAck(handshakeRttMillis, handshake, sendOrdinal);
                handshake.release();
                Assertions.assertEquals(handshakeRttMillis, window.getModelMinimumRttMillis());
            }
            long simulationStart = cleanHandshake ? 10L : 0L;
            for (long now = simulationStart; now <= measurementEndMillis + 1_000L; now++) {
                while (!deliveries.isEmpty() && deliveries.peek().at <= now) {
                    Delivery delivery = deliveries.poll();
                    if (delivery.deliversPayload && receiverDelivered.add(delivery.datagram.getSequenceIndex())
                            && now >= measurementStartMillis && now < measurementEndMillis) {
                        deliveredMeasurementBytes += payloadBytes;
                    }
                    if (!delivery.completesSender) {
                        continue;
                    }
                    if (delivery.lost) {
                        window.onBoundedLoss(delivery.datagram, sendOrdinal - 1L);
                        pendingRetries.offer(delivery.datagram);
                    } else {
                        window.onAck(now, delivery.datagram, sendOrdinal);
                        releaseIfNeeded(delivery.datagram);
                    }
                }

                if (cleanHandshake && pathRebasedAtMillis < 0L
                        && window.getModelMinimumRttMillis() >= 20L) {
                    pathRebasedAtMillis = now;
                }
                if (!window.isModelPersistentCongestion()
                        && window.getCongestionWindow() <= 2D * MTU) {
                    if (minimumCwndSinceMillis < 0L) {
                        minimumCwndSinceMillis = now;
                    }
                    maximumMinimumCwndDurationMillis = Math.max(maximumMinimumCwndDurationMillis,
                            now - minimumCwndSinceMillis + 1L);
                } else {
                    minimumCwndSinceMillis = -1L;
                }

                if (now >= measurementEndMillis) {
                    continue;
                }
                offeredCredit += offeredBytesPerSecond;
                int bytesSentInTick = 0;
                if (now % 10L == 0L) {
                    int retriesThisFlush = 0;
                    while (retriesThisFlush < 2 && !pendingRetries.isEmpty()) {
                        RakDatagramPacket datagram = pendingRetries.peek();
                        if (!window.canSendBoundedRecovery(datagram.getSize(), now)) {
                            break;
                        }
                        pendingRetries.poll();
                        window.onBoundedRetransmit(datagram, false, now, pendingRetries.isEmpty());
                        datagram.setSendTime(now);
                        datagram.setSendOrdinal(sendOrdinal++);
                        datagram.markRetransmitted();
                        bytesSentInTick += datagram.getSize();
                        retransmissions++;
                        sentDatagrams++;
                        boolean lost = lossPattern.isLost(sentDatagrams, now);
                        boolean ackLost = sentDatagrams == firstLossPacket && firstLossIsAck;
                        long serviceStart = Math.max(now, nextLinkAvailable);
                        long serializationMillis = Math.max(1L,
                                (long) Math.ceil(datagram.getSize() / capacityBytesPerMillis));
                        nextLinkAvailable = serviceStart + serializationMillis;
                        scheduleDelivery(deliveries, window, now,
                                nextLinkAvailable + pathRttPattern.rttMillis(sentDatagrams, now),
                                datagram, lost, ackLost);
                        retriesThisFlush++;
                    }
                }
                maxPendingRetries = Math.max(maxPendingRetries, pendingRetries.size());
                if (!pendingRetries.isEmpty()) {
                    maxBytesSentInTick = Math.max(maxBytesSentInTick, bytesSentInTick);
                    continue;
                }
                if (now % 10L != 0L) {
                    continue;
                }
                while (offeredCredit >= payloadBytes * 1_000L) {
                    int allowance = window.getTransmissionBandwidth(now);
                    RakDatagramPacket datagram = orderedDatagram(payloadBytes);
                    int datagramSize = datagram.getSize();
                    if (allowance < datagramSize) {
                        datagram.release();
                        break;
                    }

                    datagram.setSequenceIndex((int) sendOrdinal);
                    datagram.setSendOrdinal(sendOrdinal++);
                    datagram.setSendTime(now);
                    offeredCredit -= payloadBytes * 1_000L;
                    window.onReliableSend(datagram, offeredCredit < payloadBytes * 1_000L);
                    bytesSentInTick += datagramSize;
                    sentDatagrams++;
                    boolean lost = lossPattern.isLost(sentDatagrams, now);
                    boolean ackLost = sentDatagrams == firstLossPacket && firstLossIsAck;
                    long serviceStart = Math.max(now, nextLinkAvailable);
                    long serializationMillis = Math.max(1L,
                            (long) Math.ceil(datagram.getSize() / capacityBytesPerMillis));
                    nextLinkAvailable = serviceStart + serializationMillis;
                    scheduleDelivery(deliveries, window, now,
                            nextLinkAvailable + pathRttPattern.rttMillis(sentDatagrams, now),
                            datagram, lost, ackLost);
                }
                maxBytesSentInTick = Math.max(maxBytesSentInTick, bytesSentInTick);
            }

            double measuredMbps = deliveredMeasurementBytes * 8D
                    / ((measurementEndMillis - measurementStartMillis) * 1_000D);
            return new SimulationResult(measuredMbps, window.getCongestionWindow(), maxBytesSentInTick,
                    window.getModelRoundCount(), retransmissions, maxPendingRetries,
                    window.getModelMinimumRttMillis(), window.getModelLossResponseCount(),
                    window.getModelHardLossResponseCount(), window.getModelDelayLossResponseCount(),
                    window.getModelBandwidthBytesPerMillis(), window.getModelPacingRateBytesPerMillis(),
                    window.getModelInflightLimit(), window.isModelLossResponseHeld(),
                    pathRebasedAtMillis, maximumMinimumCwndDurationMillis);
        } finally {
            while (!deliveries.isEmpty()) {
                releaseIfNeeded(deliveries.poll().datagram);
            }
            while (!pendingRetries.isEmpty()) {
                releaseIfNeeded(pendingRetries.poll());
            }
            window.close();
        }
    }

    @Test
    public void shallowPostHandshakePathStepSustainsSeededLossAcrossPhases() {
        for (long handshakeRttMillis : new long[]{5L, 11L}) {
            double sum = 0D;
            double sumSquares = 0D;
            for (int phase = 0; phase < 16; phase++) {
                int lossPhase = phase;
                SimulationResult result = simulateLoss(true, -1, false, 9_000L, 12_000L,
                        1_000, handshakeRttMillis, 2_000D,
                        (sentDatagrams, sendAt) -> handshakeRttMillis + 16L
                                + Math.floorMod(sentDatagrams * 17 + lossPhase, 9),
                        (sentDatagrams, sendAt) -> Math.floorMod(sentDatagrams + lossPhase, 49) == 0);
                long minimumExpectedRtt = handshakeRttMillis + 16L;
                long maximumExpectedRtt = handshakeRttMillis + 24L;
                Assertions.assertTrue(result.finalMinimumRttMillis >= minimumExpectedRtt
                                && result.finalMinimumRttMillis <= maximumExpectedRtt,
                        () -> "phase " + lossPhase + " retained the " + handshakeRttMillis
                                + " ms handshake minimum: " + result.finalMinimumRttMillis + " ms");
                Assertions.assertTrue(result.pathRebasedAtMillis >= 0L && result.pathRebasedAtMillis <= 3_000L,
                        () -> "phase " + lossPhase + " did not rebase the " + handshakeRttMillis
                                + " ms shallow path within three seconds: " + result.pathRebasedAtMillis);
                Assertions.assertTrue(result.measuredMbps >= 3.5D,
                        () -> "phase " + lossPhase + " collapsed after the " + handshakeRttMillis
                                + " ms shallow path step: " + result.measuredMbps
                                + " Mbps, cwnd=" + result.finalCwnd);
                Assertions.assertEquals(0L, result.hardLossResponses,
                        "stationary near-threshold loss must not become a HARD response");
                Assertions.assertTrue(result.delayLossResponses <= 1L,
                        "stationary loss must never split into repeated DELAY episodes");
                Assertions.assertTrue(result.finalCwnd >= 16D * MTU,
                        () -> "phase " + lossPhase + " finished below sixteen MTUs: " + result.finalCwnd);
                Assertions.assertTrue(result.maximumMinimumCwndDurationMillis <= 1_000L,
                        () -> "phase " + lossPhase + " stayed at the two-MTU floor for "
                                + result.maximumMinimumCwndDurationMillis + " ms");
                Assertions.assertTrue(result.lossResponses <= 1L);
                Assertions.assertTrue(result.maxPendingRetries < 100);
                Assertions.assertTrue(result.maxBytesSentInTick <= 8 * MTU);
                sum += result.measuredMbps;
                sumSquares += result.measuredMbps * result.measuredMbps;
            }
            double fairness = sum * sum / (16D * sumSquares);
            Assertions.assertTrue(fairness >= 0.99D,
                    () -> "loss phase created " + handshakeRttMillis
                            + " ms shallow-path divergence: " + fairness);
        }
    }

    private static boolean isPeriodicLoss(int sentDatagrams, int lossPhase, int firstLossPacket) {
        return sentDatagrams == firstLossPacket || (sentDatagrams + lossPhase) % 20 == 0;
    }

    @Test
    public void transientThirtyPercentLossRecoversCleanGoodputWithinThreeSecondsForEveryPhase() {
        for (int payloadBytes : new int[]{128, 512, MTU - 100}) {
            SimulationResult baseline = simulateLoss(false, -1, false, 9_000L, 10_000L,
                    payloadBytes, (sentDatagrams, sendAt) -> false);
            for (int phase = 0; phase < 10; phase++) {
                int lossPhase = phase;
                SimulationResult recovered = simulateLoss(false, -1, false, 9_000L, 10_000L,
                        payloadBytes, (sentDatagrams, sendAt) -> sendAt >= 5_000L && sendAt < 7_000L
                                && Math.floorMod(sentDatagrams + lossPhase, 10) < 3);
                Assertions.assertTrue(recovered.measuredMbps >= baseline.measuredMbps * 0.90D,
                        () -> "clean goodput stayed below 90% three seconds after transient loss: payload="
                                + payloadBytes + ", phase=" + lossPhase + ", baseline="
                                + baseline.measuredMbps + " Mbps, recovered=" + recovered.measuredMbps
                                + " Mbps, cwnd=" + recovered.finalCwnd + ", bandwidth="
                                + recovered.finalBandwidthBytesPerMillis + ", pacing="
                                + recovered.finalPacingBytesPerMillis + ", responses="
                                + recovered.lossResponses + ", limit=" + recovered.finalInflightLimit
                                + ", held=" + recovered.lossResponseHeld);
                Assertions.assertTrue(recovered.retransmissions > 0,
                        "the transient-loss fixture must exercise bounded reliable recovery");
                Assertions.assertEquals(1L, recovered.lossResponses,
                        "one two-second loss episode must cause exactly one window cut");
                Assertions.assertFalse(recovered.lossResponseHeld,
                        "two clean evidence buckets must release HOLD within three seconds");
                Assertions.assertEquals(Double.POSITIVE_INFINITY, recovered.finalInflightLimit);
                Assertions.assertTrue(recovered.maxBytesSentInTick <= 8 * MTU);
                Assertions.assertTrue(recovered.maxPendingRetries < 100);
            }
        }
    }

    private static void scheduleDelivery(PriorityQueue<Delivery> deliveries, RakSlidingWindow window,
                                         long sendAt, long arrivesAt, RakDatagramPacket datagram,
                                         boolean lost, boolean ackLost) {
        if (!ackLost) {
            deliveries.offer(new Delivery(arrivesAt, datagram, lost, !lost));
            return;
        }

        // The receiver gets the payload, but the sender gets no ACK. Retain the in-flight attempt until the
        // bounded retransmission clock expires, then surface sender-side loss and retry it.
        deliveries.offer(new Delivery(arrivesAt, datagram, false, true, false));
        long timeoutAt = Math.max(arrivesAt + 1L, sendAt + window.getRtoForRetransmission());
        deliveries.offer(new Delivery(timeoutAt, datagram, true, false, true));
    }

    private static Comparator<Delivery> deliveryOrder() {
        return Comparator.comparingLong((Delivery value) -> value.at)
                .thenComparingLong(value -> value.datagram.getSendOrdinal());
    }

    @Test
    public void persistentNoProgressCollapsesOnlyTheModelWindow() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        Assertions.assertEquals(10D * MTU, window.getCongestionWindow());
        window.onPersistentCongestion();
        Assertions.assertEquals(2D * MTU, window.getCongestionWindow());
    }

    @Test
    public void transientCongestionBoundReleasesDuringNonCongestiveFivePercentLoss() {
        RakModelCongestionController controller = new RakModelCongestionController(MTU);
        long now = 0L;
        for (int round = 0; round < 8; round++) {
            now = completeModelRound(controller, now, 125, 0, 213L, 213D);
        }
        double learnedCwnd = controller.getCongestionWindow();
        Assertions.assertTrue(learnedCwnd >= 93_000D);

        now = completeModelRound(controller, now, 125, 38, 320L, 320D);
        now = completeModelRound(controller, now, 125, 0, 213L, 213D);
        double boundedCwnd = controller.getCongestionWindow();
        Assertions.assertTrue(boundedCwnd < learnedCwnd,
                "hard transient loss must install a durable in-flight bound");

        for (int round = 0; round < 24; round++) {
            now = completeModelRound(controller, now, 125, 6, 213L, 213D);
        }
        completeModelRound(controller, now, 125, 6, 213L, 213D);
        Assertions.assertEquals(1L, controller.getHardLossResponseCount());
        Assertions.assertEquals(0L, controller.getDelayLossResponseCount());
        Assertions.assertFalse(controller.isLossResponseHeld(),
                "a HARD-origin hold may clear under mature stable non-inflated random loss");
        Assertions.assertEquals(Double.POSITIVE_INFINITY, controller.getInflightLimit());
        Assertions.assertTrue(controller.getCongestionWindow() > boundedCwnd,
                "ordinary random loss without delay inflation must release, not deadlock, the loss bound");
        Assertions.assertTrue(controller.getCongestionWindow() >= 93_000D);
    }

    @Test
    public void unstableHighQueueDelayCannotSuppressModerateLossIndefinitely() {
        RakModelCongestionController controller = new RakModelCongestionController(MTU);
        long now = 0L;
        for (int round = 0; round < 8; round++) {
            now = completeModelRound(controller, now, 125, 0, 5L, 5D);
        }
        double learnedCwnd = controller.getCongestionWindow();

        now = completeModelRound(controller, now, 125, 6, 100L, 100D);
        now = completeModelRound(controller, now, 125, 6, 300L, 300D);
        now = completeModelRound(controller, now, 125, 6, 120L, 120D);

        Assertions.assertEquals(5L, controller.getMinimumRttMillis(),
                "unstable busy-flight queue delay is never promoted to propagation RTT");
        controller.transmissionAllowance(now + 1_001L, 0);
        now = completeModelRound(controller, now + 1_002L, 125, 6, 5L, 100D);
        completeModelRound(controller, now, 125, 6, 5L, 100D);
        Assertions.assertTrue(controller.getCongestionWindow() < learnedCwnd,
                "once the unstable suspicion cools down, steady-path delay-qualified loss must respond");
    }

    @Test
    public void lossResponseRequiresMatureLostPacketEvidence() {
        RakModelCongestionController oneOfTwo = new RakModelCongestionController(MTU);
        double initialCwnd = oneOfTwo.getCongestionWindow();
        completeModelRound(oneOfTwo, 0L, 2, 1, 200L, 200D);
        Assertions.assertEquals(0L, oneOfTwo.getLossResponseCount());
        Assertions.assertEquals(initialCwnd, oneOfTwo.getCongestionWindow());

        RakModelCongestionController eighthLoss = new RakModelCongestionController(MTU);
        long now = completeModelRound(eighthLoss, 0L, 8, 7, 200L, 200D);
        Assertions.assertEquals(0L, eighthLoss.getLossResponseCount(),
                "seven losses are still undersampled hard-loss evidence");
        completeModelRound(eighthLoss, now, 2, 1, 200L, 200D);
        Assertions.assertEquals(1L, eighthLoss.getHardLossResponseCount(),
                "the eighth accumulated loss must make a high-rate hard-loss episode actionable");
        Assertions.assertTrue(eighthLoss.getCongestionWindow() < initialCwnd);
    }

    @Test
    public void startupLossEvidenceIsBoundedAgainstCleanHistory() {
        RakModelCongestionController isolatedLoss = new RakModelCongestionController(MTU);
        double initialCwnd = isolatedLoss.getCongestionWindow();
        long now = 0L;
        for (int round = 0; round < 100; round++) {
            now = completeModelRound(isolatedLoss, now, 1, 0, 200L, 200D);
        }
        now = completeModelRound(isolatedLoss, now, 2, 1, 200L, 200D);
        Assertions.assertEquals(0L, isolatedLoss.getLossResponseCount(),
                "arbitrarily long clean history plus one loss is not mature evidence");
        Assertions.assertEquals(initialCwnd, isolatedLoss.getCongestionWindow());

        for (int round = 0; round < 20; round++) {
            now = completeModelRound(isolatedLoss, now, 2, 1, 200L, 200D);
        }
        Assertions.assertEquals(1L, isolatedLoss.getHardLossResponseCount(),
                "clean history cannot dilute a later sustained tiny-packet loss episode forever");
    }

    @Test
    public void hardLossUsesTheLargerPacketOrByteLossRate() {
        RakModelCongestionController byteDominated = new RakModelCongestionController(MTU);
        long now = recordMixedModelRound(byteDominated, 0L, 120, 128, 8, MTU - 100, 200L, 200D);
        completeModelRound(byteDominated, now, 1, 0, 200L, 200D);
        Assertions.assertEquals(1L, byteDominated.getHardLossResponseCount(),
                "large lost packets must trigger the byte-loss-rate branch when packet loss is below 20%");

        RakModelCongestionController packetDominated = new RakModelCongestionController(MTU);
        now = recordMixedModelRound(packetDominated, 0L, 24, MTU - 100, 8, 128, 200L, 200D);
        completeModelRound(packetDominated, now, 1, 0, 200L, 200D);
        Assertions.assertEquals(1L, packetDominated.getHardLossResponseCount(),
                "many small lost packets must trigger the packet-loss-rate branch when byte loss is below 20%");
    }

    @Test
    public void hardLossThresholdIncludesExactlyTwentyPercent() {
        RakModelCongestionController below = new RakModelCongestionController(MTU);
        long now = recordMixedModelRound(below, 0L, 33, 512, 8, 512, 200L, 200D);
        completeModelRound(below, now, 1, 0, 200L, 200D);
        Assertions.assertEquals(0L, below.getHardLossResponseCount(),
                "eight of forty-one lost packets is below the hard threshold");

        RakModelCongestionController exact = new RakModelCongestionController(MTU);
        now = recordMixedModelRound(exact, 0L, 32, 512, 8, 512, 200L, 200D);
        completeModelRound(exact, now, 1, 0, 200L, 200D);
        Assertions.assertEquals(1L, exact.getHardLossResponseCount(),
                "the hard threshold includes exactly twenty percent loss");
    }

    @Test
    public void delayLossThresholdsRequireStrictlyAboveTwoPercentFourLossesAnd128Packets() {
        RakModelCongestionController exactTwoPercent = learnedController(5L);
        long now = recordMixedModelRound(exactTwoPercent, 1_001L,
                196, 512, 4, 512, 5L, 10D);
        completeModelRound(exactTwoPercent, now, 1, 0, 5L, 10D);
        Assertions.assertEquals(0L, exactTwoPercent.getDelayLossResponseCount(),
                "delay loss is strict above, not inclusive of, two percent");

        RakModelCongestionController aboveTwoPercent = learnedController(5L);
        now = recordMixedModelRound(aboveTwoPercent, 1_001L,
                195, 512, 4, 512, 5L, 10D);
        completeModelRound(aboveTwoPercent, now, 1, 0, 5L, 10D);
        Assertions.assertEquals(1L, aboveTwoPercent.getDelayLossResponseCount());

        RakModelCongestionController threeLosses = learnedController(5L);
        now = recordMixedModelRound(threeLosses, 1_001L,
                125, 512, 3, 512, 5L, 10D);
        completeModelRound(threeLosses, now, 1, 0, 5L, 10D);
        Assertions.assertEquals(0L, threeLosses.getDelayLossResponseCount(),
                "three losses remain below delay-loss maturity");

        RakModelCongestionController fourLosses = learnedController(5L);
        now = recordMixedModelRound(fourLosses, 1_001L,
                124, 512, 4, 512, 5L, 10D);
        completeModelRound(fourLosses, now, 1, 0, 5L, 10D);
        Assertions.assertEquals(1L, fourLosses.getDelayLossResponseCount(),
                "four losses in a mature bucket are actionable");

        RakModelCongestionController only127Packets = learnedController(5L);
        now = recordMixedModelRound(only127Packets, 1_001L,
                123, 512, 4, 512, 5L, 10D);
        completeModelRound(only127Packets, now, 1, 0, 5L, 10D);
        Assertions.assertEquals(0L, only127Packets.getDelayLossResponseCount(),
                "127 packets remain below delay-loss maturity");
    }

    @Test
    public void boundaryAckRttCannotRewriteDelayLossProvenance() {
        RakModelCongestionController highBoundary = learnedController(5L);
        long now = recordMixedModelRound(highBoundary, 1_001L,
                123, 512, 4, 512, 5L, 5D);
        now = completeModelRound(highBoundary, now, 1, 0, 5L, 10D);
        completeModelRound(highBoundary, now, 1, 0, 5L, 10D);
        Assertions.assertEquals(0L, highBoundary.getDelayLossResponseCount(),
                "one inflated boundary ACK cannot qualify 127 non-inflated loss-evidence packets");

        RakModelCongestionController lowerMinimum = learnedController(100L);
        now = recordMixedModelRound(lowerMinimum, 1_001L,
                123, 512, 4, 512, 100L, 100D);
        now = completeModelRound(lowerMinimum, now, 1, 0, 5L, 5D);
        completeModelRound(lowerMinimum, now, 1, 0, 5L, 5D);
        Assertions.assertEquals(0L, lowerMinimum.getDelayLossResponseCount(),
                "a lower boundary minimum cannot retroactively inflate prior loss observations");

        RakModelCongestionController genuinelyInflated = learnedController(100L);
        now = recordMixedModelRound(genuinelyInflated, 1_001L,
                123, 512, 4, 512, 100L, 150D);
        now = completeModelRound(genuinelyInflated, now, 1, 0, 5L, 5D);
        completeModelRound(genuinelyInflated, now, 1, 0, 5L, 5D);
        Assertions.assertEquals(0L, genuinelyInflated.getDelayLossResponseCount(),
                "a material lower-path boundary invalidates even genuinely inflated old-path loss evidence");
    }

    @Test
    public void suppressedPathCleanAcksCannotReleaseHeldLossCap() {
        RakModelCongestionController controller = learnedController(5L);
        long now = 1_001L;
        for (int round = 0; round < 3; round++) {
            now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        }
        Assertions.assertTrue(controller.isLossResponseHeld());
        double heldLimit = controller.getInflightLimit();
        Assertions.assertTrue(Double.isFinite(heldLimit));

        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertEquals(2D * MTU, controller.getCongestionWindow());

        now = recordMixedModelRound(controller, now, 80, 512, 0, 512, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        now = recordMixedModelRound(controller, now, 80, 512, 0, 512, 100L, 100D);
        completeModelRound(controller, now, 1, 0, 100L, 100D);

        Assertions.assertTrue(controller.isLossResponseHeld(),
                "two clean-sized buckets collected during DRAIN cannot release HOLD");
        Assertions.assertEquals(heldLimit, controller.getInflightLimit(),
                "path-suppressed clean ACKs must preserve the finite loss cap");
    }

    @Test
    public void hardHoldConvertsToDelayHoldBeforeRandomLossCanClearIt() {
        RakModelCongestionController controller = learnedController(5L);
        long now = completeModelRound(controller, 1_001L, 125, 38, 5L, 10D);
        now = completeModelRound(controller, now, 125, 38, 5L, 10D);
        Assertions.assertEquals(1L, controller.getHardLossResponseCount());
        Assertions.assertEquals(0L, controller.getDelayLossResponseCount());

        for (int round = 0; round < 4; round++) {
            now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        }
        Assertions.assertEquals(1L, controller.getLossResponseCount(),
                "a delay signal during HOLD changes hysteresis without making a second cut");

        for (int round = 0; round < 24; round++) {
            now = completeModelRound(controller, now, 125, 6, 5L, 5D);
        }
        Assertions.assertTrue(controller.isLossResponseHeld(),
                "after HARD converts to DELAY, continuing five-percent loss cannot count as clear");
        Assertions.assertTrue(Double.isFinite(controller.getInflightLimit()));

        now = completeModelRound(controller, now, 256, 0, 5L, 5D);
        Assertions.assertTrue(controller.isLossResponseHeld());
        now = completeModelRound(controller, now, 256, 0, 5L, 5D);
        completeModelRound(controller, now, 1, 0, 5L, 5D);
        Assertions.assertFalse(controller.isLossResponseHeld(),
                "two 256-packet loss-free windows release the converted DELAY hold");
    }

    @Test
    public void pathAbortCannotRetroactivelyClassifySuppressedLossEvidence() {
        RakModelCongestionController controller = learnedController(5L);
        long now = completeModelRound(controller, 1_001L, 1, 0, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertEquals(2D * MTU, controller.getCongestionWindow());

        now = recordMixedModelRound(controller, now, 121, 1_000, 6, 1_000, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 5L, 10D);
        Assertions.assertEquals(0L, controller.getLossResponseCount(),
                "the lower-RTT boundary ACK cannot turn 127 suppressed packets into a delay signal");

        now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        completeModelRound(controller, now, 125, 6, 5L, 10D);
        Assertions.assertEquals(1L, controller.getDelayLossResponseCount(),
                "a wholly post-boundary mature bucket remains actionable");
    }

    @Test
    public void pathTimeoutCannotRetroactivelyClassifySuppressedLossEvidence() {
        RakModelCongestionController controller = learnedController(5L);
        long now = completeModelRound(controller, 1_001L, 1, 0, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        now = recordMixedModelRound(controller, now, 121, 1_000, 6, 1_000, 100L, 100D);

        now += 3_001L;
        controller.transmissionAllowance(now, 0);
        Assertions.assertEquals(0L, controller.getLossResponseCount(),
                "timing out a drain discards its suppressed open-round evidence");
        now += 1_001L;
        controller.transmissionAllowance(now, 0);

        now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        completeModelRound(controller, now, 125, 6, 5L, 10D);
        Assertions.assertEquals(1L, controller.getDelayLossResponseCount(),
                "fresh evidence after suppressed cooldown expiry remains actionable");
    }

    @Test
    public void hardLossEvidenceSurvivesPathStateBoundaries() {
        RakModelCongestionController controller = new RakModelCongestionController(MTU);
        long now = completeModelRound(controller, 0L, 8, 7, 5L, 5D);
        Assertions.assertEquals(0L, controller.getLossResponseCount());

        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        completeModelRound(controller, now, 2, 1, 100L, 100D);
        Assertions.assertEquals(1L, controller.getHardLossResponseCount(),
                "the eighth high-rate loss remains actionable across SUSPECT/DRAIN boundaries");
        Assertions.assertEquals(0L, controller.getDelayLossResponseCount(),
                "suppressed path evidence cannot be retroactively classified as delay loss");
    }

    @Test
    public void hardLossDuringPathDrainCannotMakeTheProbeFloorPermanent() {
        RakModelCongestionController controller = learnedController(5L);
        long now = completeModelRound(controller, 1_001L, 1, 0, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertEquals(2D * MTU, controller.getCongestionWindow(),
                "the fixture must enter the intentional two-MTU path drain");

        now = completeModelRound(controller, now, 10, 8, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertEquals(1L, controller.getHardLossResponseCount(),
                "hard loss remains actionable while path-delay evidence is suppressed");
        Assertions.assertTrue(controller.getInflightLimit() >= 6D * MTU,
                () -> "an artificial probe flight cannot install a bootstrap-deadlocking cap: "
                        + controller.getInflightLimit());

        now += 100L;
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertEquals(100L, controller.getMinimumRttMillis(),
                "the fixture must accept the stable replacement path");
        Assertions.assertTrue(controller.getCongestionWindow() >= 6D * MTU,
                () -> "accepted-path discovery must restart above the probe floor: "
                        + controller.getCongestionWindow());
    }

    @Test
    public void acceptedPathRestartsInitialWindowAfterSelfClockedProbeFloor() {
        RakModelCongestionController controller = learnedController(5L);
        controller.onPersistentCongestion();
        Assertions.assertEquals(2D * MTU, controller.getCongestionWindow(),
                "persistent congestion itself must retain the two-MTU safety collapse");

        acceptStablePathStep(controller, 1_001L, 100L);

        Assertions.assertEquals(0L, controller.getLossResponseCount());
        Assertions.assertFalse(controller.isPersistentCongestion(),
                "delivered path-validation samples prove forward progress");
        Assertions.assertTrue(controller.getCongestionWindow() >= 9D * MTU,
                () -> "a clean accepted path must restart with a useful initial window: "
                        + controller.getCongestionWindow());
        Assertions.assertTrue(controller.getPacingRateBytesPerMillis() >= 9D * MTU / 10D,
                () -> "accepted-path startup must pace a useful initial flight per send quantum: "
                        + controller.getPacingRateBytesPerMillis());
    }

    @Test
    public void rejectedQueueDelayProbeEntersCooldownBeforeAnotherDrain() {
        RakModelCongestionController controller = learnedController(5L);
        long now = completeModelRound(controller, 1_001L, 1, 0, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertEquals(2D * MTU, controller.getCongestionWindow(),
                "the fixture must enter the intentional two-MTU path drain");

        now = completeModelRound(controller, now, 1, 0, 5L, 5D);
        long rejectedAt = now;
        double restoredWindow = controller.getCongestionWindow();
        Assertions.assertTrue(restoredWindow > 2D * MTU,
                "a baseline RTT observation must reject the queued-delay candidate and restore the window");

        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertTrue(controller.getCongestionWindow() > 2D * MTU,
                () -> "a rejected queued-delay candidate must not immediately trigger another probe drain: "
                        + controller.getCongestionWindow() + " after restoring " + restoredWindow);

        now = completeModelRound(controller, rejectedAt + 1_000L, 1, 0, 100L, 100D);
        completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertTrue(controller.getCongestionWindow() > 2D * MTU,
                "a disproved candidate must remain rate-limited for one complete validation horizon");

        controller.transmissionAllowance(rejectedAt + 2_001L, 0);
        now = completeModelRound(controller, rejectedAt + 2_002L, 1, 0, 100L, 100D);
        completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertEquals(2D * MTU, controller.getCongestionWindow(),
                "cooldown expiry must still permit a later stable replacement-path probe");
    }

    @Test
    public void continuousDelayQualifiedLossCutsExactlyOnceUntilTwoLossFreeClearWindows() {
        RakModelCongestionController controller = learnedController(5L);
        long now = 1_001L;
        double signalPeakFlight = 125D * datagramWireSize(1_000);

        for (int round = 0; round < 12; round++) {
            now = completeModelRound(controller, now, 125, 6, 11L, 11D);
        }
        // Close the last flight's evidence without introducing a clear bucket.
        completeModelRound(controller, now, 125, 6, 11L, 11D);

        Assertions.assertEquals(1L, controller.getLossResponseCount(),
                "one continuing delay-qualified epoch must cause exactly one response");
        Assertions.assertEquals(0L, controller.getHardLossResponseCount());
        Assertions.assertEquals(1L, controller.getDelayLossResponseCount());
        Assertions.assertTrue(controller.isLossResponseHeld());
        Assertions.assertTrue(controller.getCongestionWindow() >= signalPeakFlight * 0.88D
                        && controller.getCongestionWindow() <= signalPeakFlight * 0.91D,
                () -> "DELAY must make one material ten-percent response without borrowing HARD severity: "
                        + controller.getCongestionWindow() + " from peak flight " + signalPeakFlight);
        Assertions.assertTrue(Double.isFinite(controller.getInflightLimit()),
                "the continuing delay-loss episode must retain its finite cap");
        Assertions.assertEquals(5L, controller.getMinimumRttMillis(),
                "moderate raw queue delay below the path-step envelope remains congestion evidence");
    }

    @Test
    public void hardLossKeepsTheThirtyPercentResponse() {
        RakModelCongestionController controller = learnedController(5L);
        long now = 1_001L;
        double signalPeakFlight = 125D * datagramWireSize(1_000);

        now = completeModelRound(controller, now, 125, 38, 5L, 10D);
        completeModelRound(controller, now, 125, 38, 5L, 10D);

        Assertions.assertEquals(1L, controller.getHardLossResponseCount());
        Assertions.assertEquals(0L, controller.getDelayLossResponseCount());
        Assertions.assertTrue(controller.getCongestionWindow() >= signalPeakFlight * 0.68D
                        && controller.getCongestionWindow() <= signalPeakFlight * 0.71D,
                () -> "HARD must preserve the existing thirty-percent response: "
                        + controller.getCongestionWindow() + " from peak flight " + signalPeakFlight);
    }

    @Test
    public void twoDisjointLossFreeWindowsRearmOneNewLossResponse() {
        RakModelCongestionController controller = learnedController(5L);
        long now = 1_001L;

        now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        Assertions.assertEquals(1L, controller.getLossResponseCount());
        Assertions.assertTrue(controller.isLossResponseHeld());
        Assertions.assertTrue(Double.isFinite(controller.getInflightLimit()));

        now = completeModelRound(controller, now, 256, 0, 5L, 5D);
        Assertions.assertTrue(controller.isLossResponseHeld(),
                "one 256-packet loss-free window cannot rearm the response");
        Assertions.assertTrue(Double.isFinite(controller.getInflightLimit()));
        now = completeModelRound(controller, now, 256, 0, 5L, 5D);
        now = completeModelRound(controller, now, 1, 0, 5L, 5D);
        Assertions.assertFalse(controller.isLossResponseHeld(),
                "two disjoint 256-packet loss-free windows release the cap and rearm loss response");
        Assertions.assertEquals(Double.POSITIVE_INFINITY, controller.getInflightLimit());

        for (int round = 0; round < 8; round++) {
            now = completeModelRound(controller, now, 125, 0, 5L, 5D);
        }
        Assertions.assertFalse(controller.isStartup(),
                "clean post-release discovery must reach steady state before the next delay epoch");
        now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        completeModelRound(controller, now, 125, 6, 5L, 10D);
        Assertions.assertEquals(2L, controller.getLossResponseCount(),
                "one later congestion epoch gets exactly one new response");
        Assertions.assertTrue(controller.isLossResponseHeld());
    }

    @Test
    public void stationaryTwoPercentLossCannotSplitOneDelayLossEpoch() {
        for (int phase = 0; phase < 16; phase++) {
            RakModelCongestionController controller = learnedController(5L);
            java.util.Random random = new java.util.Random(0x5EED_0200L + phase);
            long now = 1_001L;
            int observedPackets = 0;
            int round = 0;
            while (observedPackets < 20_000) {
                int packets = 8 + (round++ + phase) % 9;
                int lost = 0;
                for (int packet = 0; packet < packets; packet++) {
                    if (random.nextDouble() < 0.0204D) {
                        lost++;
                    }
                }
                now = completeModelRound(controller, now, packets, lost, 5L, 10D);
                observedPackets += packets;
            }
            now = completeModelRound(controller, now, 1, 0, 5L, 10D);

            Assertions.assertEquals(1L, controller.getDelayLossResponseCount(),
                    "stationary near-threshold loss must remain one held episode for phase " + phase);
            Assertions.assertTrue(controller.isLossResponseHeld(),
                    "ordinary stochastic dips below two percent cannot rearm phase " + phase);

            now = completeModelRound(controller, now, 256, 0, 5L, 5D);
            now = completeModelRound(controller, now, 256, 0, 5L, 5D);
            now = completeModelRound(controller, now, 1, 0, 5L, 5D);
            Assertions.assertFalse(controller.isLossResponseHeld(),
                    "512 clean packets must release the held episode for phase " + phase);

            for (int cleanRound = 0; cleanRound < 8; cleanRound++) {
                now = completeModelRound(controller, now, 125, 0, 5L, 5D);
            }
            for (int lossRound = 0; lossRound < 3; lossRound++) {
                now = completeModelRound(controller, now, 125, 6, 5L, 10D);
            }
            completeModelRound(controller, now, 1, 0, 5L, 10D);
            Assertions.assertEquals(2L, controller.getDelayLossResponseCount(),
                    "a later distinct episode gets exactly one response for phase " + phase);
        }
    }

    @Test
    public void heldDelayResponseCannotApplyASecondModelOnlyWindowCollapse() {
        RakModelCongestionController controller = learnedController(5L);
        long now = 1_001L;
        for (int round = 0; round < 3; round++) {
            now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        }

        Assertions.assertEquals(1L, controller.getDelayLossResponseCount());
        Assertions.assertTrue(controller.isLossResponseHeld());
        double heldFlight = controller.getInflightLimit();
        Assertions.assertTrue(Double.isFinite(heldFlight));

        // Age all pre-response delivery samples out of the ten-round filter using a self-clocked trickle. This
        // used to pull cwnd to the two-MTU model target even though the DELAY epoch allowed only one 0.90 cut.
        for (int round = 0; round < 12; round++) {
            now = completeModelRound(controller, now, 1, 0, 5L, 5D);
        }

        Assertions.assertTrue(controller.isLossResponseHeld(),
                "twelve clean packets are not enough to release a DELAY hold");
        Assertions.assertEquals(heldFlight, controller.getCongestionWindow(), 0.001D,
                "bandwidth-filter decay cannot impose a second reduction during the held DELAY epoch");
    }

    @Test
    public void delayResponseCannotFreezeAnAlreadySelfClockedSubInitialFlight() {
        RakModelCongestionController controller = learnedController(5L);
        long now = 1_001L;
        for (int round = 0; round < 2_000; round++) {
            now = completeModelRound(controller, now, 1, 0, 5L, 5D);
        }
        Assertions.assertTrue(controller.getCongestionWindow() < 5D * MTU,
                () -> "the fixture must age the learned bandwidth into a small self-clocked flight: "
                        + controller.getCongestionWindow());

        for (int round = 0; round < 3; round++) {
            now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        }

        Assertions.assertEquals(1L, controller.getDelayLossResponseCount());
        Assertions.assertTrue(controller.isLossResponseHeld());
        Assertions.assertTrue(controller.getInflightLimit() >= 9D * MTU,
                () -> "a DELAY hold cannot permanently cap the already-small flight at "
                        + controller.getInflightLimit());
        Assertions.assertEquals(controller.getInflightLimit(), controller.getCongestionWindow(), 0.001D);
    }

    @Test
    public void delayHoldNeeds512ConsecutiveCleanPacketsAcrossLossAndPathBoundaries() {
        RakModelCongestionController controller = learnedController(5L);
        long now = 1_001L;
        for (int round = 0; round < 3; round++) {
            now = completeModelRound(controller, now, 125, 6, 5L, 10D);
        }
        Assertions.assertTrue(controller.isLossResponseHeld());

        now = completeModelRound(controller, now, 511, 0, 5L, 5D);
        now = completeModelRound(controller, now, 2, 1, 5L, 5D);
        now = completeModelRound(controller, now, 510, 0, 5L, 5D);
        now = completeModelRound(controller, now, 1, 0, 5L, 5D);
        Assertions.assertTrue(controller.isLossResponseHeld(),
                "one loss between two 511-packet runs resets all clear progress");
        now = completeModelRound(controller, now, 1, 0, 5L, 5D);
        Assertions.assertFalse(controller.isLossResponseHeld(),
                "the 512th consecutive actionable clean packet releases DELAY hold");

        RakModelCongestionController pathChange = learnedController(100L);
        now = 1_001L;
        for (int round = 0; round < 3; round++) {
            now = completeModelRound(pathChange, now, 125, 6, 100L, 150D);
        }
        Assertions.assertTrue(pathChange.isLossResponseHeld());
        now = completeModelRound(pathChange, now, 300, 0, 100L, 100D);
        now = completeModelRound(pathChange, now, 1, 0, 5L, 5D);
        Assertions.assertEquals(5L, pathChange.getMinimumRttMillis());
        now = completeModelRound(pathChange, now, 510, 0, 5L, 5D);
        now = completeModelRound(pathChange, now, 1, 0, 5L, 5D);
        Assertions.assertTrue(pathChange.isLossResponseHeld(),
                "clean evidence from a materially different old path cannot combine with 511 new-path packets");
        completeModelRound(pathChange, now, 1, 0, 5L, 5D);
        Assertions.assertFalse(pathChange.isLossResponseHeld());
    }

    @Test
    public void hardLossMaturityIsPacketShapeIndependent() {
        for (int payloadBytes : new int[]{128, 512, MTU - 100}) {
            RakModelCongestionController controller = new RakModelCongestionController(MTU);
            long now = completeModelRound(controller, 0L, 8, 7, 200L, 200D, payloadBytes);
            Assertions.assertEquals(0L, controller.getLossResponseCount(),
                    "seven losses remain immature for payload " + payloadBytes);
            completeModelRound(controller, now, 2, 1, 200L, 200D, payloadBytes);
            Assertions.assertEquals(1L, controller.getHardLossResponseCount(),
                    "the eighth loss must be actionable for payload " + payloadBytes);
        }
    }

    @Test
    public void persistentCongestionResetsHeldLossEpoch() {
        RakModelCongestionController controller = new RakModelCongestionController(MTU);
        long now = completeModelRound(controller, 0L, 8, 7, 200L, 200D);
        now = completeModelRound(controller, now, 2, 1, 200L, 200D);
        Assertions.assertTrue(controller.isLossResponseHeld());
        Assertions.assertEquals(1L, controller.getLossResponseCount());

        controller.onPersistentCongestion();
        Assertions.assertFalse(controller.isLossResponseHeld(),
                "persistent no-progress collapse starts a fresh loss epoch");
        Assertions.assertEquals(2D * MTU, controller.getCongestionWindow());

        // The first ACK exits persistent state; the next mature hard-loss bucket must be actionable again.
        now = completeModelRound(controller, now, 1, 0, 200L, 200D);
        now = completeModelRound(controller, now, 8, 7, 200L, 200D);
        completeModelRound(controller, now, 2, 1, 200L, 200D);
        Assertions.assertEquals(2L, controller.getLossResponseCount());
        Assertions.assertEquals(2L, controller.getHardLossResponseCount());
        Assertions.assertTrue(controller.isLossResponseHeld());
    }

    @Test
    public void persistentCongestionDiscardsPartialOpenLossEvidence() {
        RakModelCongestionController controller = new RakModelCongestionController(MTU);
        long now = recordMixedModelRound(controller, 0L, 1, 1_000, 7, 1_000, 200L, 200D);

        controller.onPersistentCongestion();
        now = completeModelRound(controller, now, 2, 1, 200L, 200D);
        Assertions.assertEquals(0L, controller.getLossResponseCount(),
                "seven pre-reset losses plus one fresh loss cannot cross the persistent epoch boundary");

        now = completeModelRound(controller, now, 8, 7, 200L, 200D);
        completeModelRound(controller, now, 1, 0, 200L, 200D);
        Assertions.assertEquals(1L, controller.getHardLossResponseCount(),
                "eight entirely fresh losses after persistent reset remain actionable");
    }

    @Test
    public void ackBeforeDelayedNackCannotCreateLossEvidence() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            for (int index = 0; index < 16; index++) {
                RakDatagramPacket datagram = orderedDatagram(512);
                packets.add(datagram);
                datagram.setSequenceIndex(index);
                datagram.setSendOrdinal(index);
                datagram.setSendTime(index * 10L);
                window.onReliableSend(datagram);
                window.onAck(200L + index * 10L, datagram, index + 1L);
                Assertions.assertFalse(window.onBoundedLoss(datagram, index),
                        "a NACK arriving after logical ACK completion is stale");
            }

            Assertions.assertEquals(0L, window.getModelLossResponseCount(),
                    "stale delayed NACK callbacks cannot mature a hard-loss bucket");
            Assertions.assertEquals(0L, window.getModelHardLossResponseCount());
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    private static RakModelCongestionController learnedController(long rttMillis) {
        RakModelCongestionController controller = new RakModelCongestionController(MTU);
        long now = 0L;
        for (int round = 0; round < 8; round++) {
            now = completeModelRound(controller, now, 125, 0, rttMillis, rttMillis);
        }
        Assertions.assertFalse(controller.isStartup(), "the fixture must leave startup before loss classification");
        return controller;
    }

    private static long recordMixedModelRound(RakModelCongestionController controller, long sendAt,
                                              int deliveredPackets, int deliveredPayloadBytes,
                                              int lostPackets, int lostPayloadBytes,
                                              long rttMillis, double smoothedRttMillis) {
        List<RakDatagramPacket> delivered = new ArrayList<>(deliveredPackets);
        List<RakDatagramPacket> lost = new ArrayList<>(lostPackets);
        int inFlight = 0;
        for (int i = 0; i < deliveredPackets; i++) {
            RakDatagramPacket datagram = datagram(deliveredPayloadBytes);
            datagram.setSendTime(sendAt);
            inFlight += datagram.getSize();
            controller.onPacketSent(datagram, sendAt, inFlight, false);
            delivered.add(datagram);
        }
        for (int i = 0; i < lostPackets; i++) {
            RakDatagramPacket datagram = datagram(lostPayloadBytes);
            datagram.setSendTime(sendAt);
            inFlight += datagram.getSize();
            controller.onPacketSent(datagram, sendAt, inFlight, false);
            lost.add(datagram);
        }

        int currentInFlight = inFlight;
        long ackAt = sendAt + rttMillis;
        for (RakDatagramPacket datagram : delivered) {
            currentInFlight -= datagram.getSize();
            controller.onAcknowledged(datagram, ackAt, rttMillis, smoothedRttMillis, currentInFlight);
            datagram.release();
        }
        for (RakDatagramPacket datagram : lost) {
            currentInFlight -= datagram.getSize();
            controller.onLost(datagram, smoothedRttMillis);
            datagram.release();
        }
        return ackAt + 1L;
    }

    @Test
    public void threeFailedPathDrainsExhaustDelayLossSuppression() {
        RakModelCongestionController controller = new RakModelCongestionController(MTU);
        long now = 0L;
        for (int round = 0; round < 8; round++) {
            now = completeModelRound(controller, now, 125, 0, 5L, 5D);
        }

        for (int attempt = 0; attempt < 3; attempt++) {
            now = failPathDrainByTimeout(controller, now, 100L);
            Assertions.assertTrue(controller.getCongestionWindow() > 2D * MTU,
                    "every timed-out drain must restore useful sending progress");
            if (attempt < 2) {
                // A 100 ms suspect RTT uses the 250 ms minimum cooldown.
                now += 251L;
            }
        }

        now = completeModelRound(controller, now + 1L, 125, 0, 100L, 100D);
        double restoredCwnd = controller.getCongestionWindow();
        completeModelRound(controller, now, 125, 6, 100L, 100D);
        Assertions.assertTrue(controller.getRecentLossRate() > 0.02D
                        && controller.getRecentLossRate() < 0.20D,
                "the post-budget signal must be delay-qualified moderate loss, not hard loss");
        Assertions.assertTrue(controller.getCongestionWindow() < restoredCwnd,
                "the third failed drain must expose delay-qualified loss even during its cooldown");
    }

    @Test
    public void exhaustedPathValidationKeepsRetryingWithoutSuppressingDelayLoss() {
        RakModelCongestionController controller = learnedController(5L);
        long now = 1_001L;
        for (int attempt = 0; attempt < 3; attempt++) {
            now = failPathDrainByTimeout(controller, now, 100L);
            if (attempt < 2) {
                now += 251L;
            }
        }

        now += 251L;
        now = completeModelRound(controller, now, 1, 0, 5L, 5D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertEquals(3, controller.getPathAttempts(),
                "a baseline-jitter sample cannot regrant suppression to later validation retries");

        now = completeModelRound(controller, now, 125, 0, 100L, 100D);
        now = completeModelRound(controller, now, 125, 6, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertEquals(1L, controller.getDelayLossResponseCount(),
                "a retry after exhaustion must not suppress fresh delay-qualified loss");
        Assertions.assertEquals(5L, controller.getMinimumRttMillis(),
                "high-flight loss evidence must not itself validate the candidate path");
        double heldLimit = controller.getInflightLimit();

        controller.transmissionAllowance(now + 2_000L, 0);
        acceptStablePathStep(controller, now + 2_251L, 100L);
        Assertions.assertEquals(100L, controller.getMinimumRttMillis(),
                "loss-suppression exhaustion must not permanently disable later path validation");
        Assertions.assertEquals(1L, controller.getDelayLossResponseCount(),
                "a validation retry cannot manufacture another loss response");
        Assertions.assertEquals(heldLimit, controller.getInflightLimit(),
                "accepting the path must preserve the installed delay-loss cap");
    }

    @Test
    public void hardLossCapSurvivesPathTimeoutAndAcceptedRetry() {
        RakModelCongestionController controller = new RakModelCongestionController(MTU);
        long now = 0L;
        for (int round = 0; round < 8; round++) {
            now = completeModelRound(controller, now, 125, 0, 5L, 5D);
        }
        now = completeModelRound(controller, now, 125, 38, 100L, 100D);
        double hardLossCap = controller.getCongestionWindow();

        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        now += 2_000L;
        controller.transmissionAllowance(now, 0);
        Assertions.assertTrue(controller.getCongestionWindow() <= hardLossCap,
                "a timed-out path drain cannot restore above the active hard-loss cap");

        now += 251L;
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        now += 100L;
        now = completeModelRound(controller, now, 1, 0, 100L, 100D);
        completeModelRound(controller, now, 1, 0, 100L, 100D);
        Assertions.assertEquals(100L, controller.getMinimumRttMillis());
        Assertions.assertTrue(controller.getCongestionWindow() <= hardLossCap + 2D * MTU,
                "accepting a new path may restart discovery but cannot discard the active hard-loss cap");
    }

    private static long failPathDrainByTimeout(RakModelCongestionController controller, long now,
                                               long suspectRttMillis) {
        now = completeModelRound(controller, now, 1, 0, suspectRttMillis, suspectRttMillis);
        now = completeModelRound(controller, now, 1, 0, suspectRttMillis, suspectRttMillis);
        long timeoutAt = now + 2_000L;
        controller.transmissionAllowance(timeoutAt, 0);
        return timeoutAt;
    }

    private static long acceptStablePathStep(RakModelCongestionController controller, long now,
                                             long pathRttMillis) {
        now = completeModelRound(controller, now, 1, 0, pathRttMillis, pathRttMillis);
        now = completeModelRound(controller, now, 1, 0, pathRttMillis, pathRttMillis);
        now = completeModelRound(controller, now, 1, 0, pathRttMillis, pathRttMillis);
        now += Math.max(50L, pathRttMillis);
        now = completeModelRound(controller, now, 1, 0, pathRttMillis, pathRttMillis);
        now = completeModelRound(controller, now, 1, 0, pathRttMillis, pathRttMillis);
        Assertions.assertEquals(pathRttMillis, controller.getMinimumRttMillis(),
                "the fixture must accept the stable drained path step");
        return now;
    }

    private static long acceptStableIdlePathStep(RakModelCongestionController controller, long now,
                                                 long pathRttMillis) {
        now = completeAppLimitedModelRound(controller, now, pathRttMillis);
        now = completeAppLimitedModelRound(controller, now, pathRttMillis);
        now = completeAppLimitedModelRound(controller, now, pathRttMillis);
        now += Math.max(50L, pathRttMillis);
        now = completeAppLimitedModelRound(controller, now, pathRttMillis);
        now = completeAppLimitedModelRound(controller, now, pathRttMillis);
        Assertions.assertEquals(pathRttMillis, controller.getMinimumRttMillis(),
                "the fixture must accept the stable app-limited path step");
        return now;
    }

    @Test
    public void returningAcrossShallowPathBoundaryResetsDelayLossAndClearProvenance() {
        RakModelCongestionController partialLoss = learnedController(5L);
        long now = acceptStablePathStep(partialLoss, 1_001L, 25L);
        for (int round = 0; round < 8; round++) {
            now = completeModelRound(partialLoss, now, 125, 0, 25L, 25D);
        }
        Assertions.assertFalse(partialLoss.isStartup());
        now = recordMixedModelRound(partialLoss, now, 123, 512, 4, 512, 25L, 40D);
        now = completeModelRound(partialLoss, now, 1, 0, 5L, 5D);
        Assertions.assertEquals(5L, partialLoss.getMinimumRttMillis());
        now = recordMixedModelRound(partialLoss, now, 122, 512, 4, 512, 5L, 10D);
        completeModelRound(partialLoss, now, 1, 0, 5L, 10D);
        Assertions.assertEquals(0L, partialLoss.getDelayLossResponseCount(),
                "partial delay-loss evidence cannot combine across a 5-to-25-to-5 ms path boundary");

        RakModelCongestionController partialClear = learnedController(5L);
        now = acceptStablePathStep(partialClear, 1_001L, 25L);
        for (int round = 0; round < 8; round++) {
            now = completeModelRound(partialClear, now, 125, 0, 25L, 25D);
        }
        Assertions.assertFalse(partialClear.isStartup());
        for (int round = 0; round < 3; round++) {
            now = completeModelRound(partialClear, now, 125, 6, 25L, 40D);
        }
        Assertions.assertTrue(partialClear.isLossResponseHeld());
        now = completeModelRound(partialClear, now, 255, 0, 25L, 25D);
        now = completeModelRound(partialClear, now, 1, 0, 5L, 5D);
        Assertions.assertEquals(5L, partialClear.getMinimumRttMillis());
        now = completeModelRound(partialClear, now, 256, 0, 5L, 5D);
        completeModelRound(partialClear, now, 1, 0, 5L, 5D);
        Assertions.assertTrue(partialClear.isLossResponseHeld(),
                "255 old-path clean packets cannot combine with 257 new-path packets to release DELAY hold");
    }

    @Test
    public void returningAcrossIdleShallowPathBoundaryResetsDelayLossProvenance() {
        RakModelCongestionController controller = learnedController(11L);
        long now = acceptStableIdlePathStep(controller, 1_001L, 20L);
        for (int round = 0; round < 8; round++) {
            now = completeModelRound(controller, now, 125, 0, 20L, 20D);
        }
        Assertions.assertFalse(controller.isStartup());

        now = recordMixedModelRound(controller, now, 123, 512, 4, 512, 20L, 35D);
        now = completeModelRound(controller, now, 1, 0, 11L, 11D);
        Assertions.assertEquals(11L, controller.getMinimumRttMillis());
        now = recordMixedModelRound(controller, now, 122, 512, 4, 512, 11L, 20D);
        completeModelRound(controller, now, 1, 0, 11L, 20D);

        Assertions.assertEquals(0L, controller.getDelayLossResponseCount(),
                "loss evidence cannot combine across a low-flight 11-to-20-to-11 ms path boundary");
    }

    private static long completeModelRound(RakModelCongestionController controller, long sendAt, int packets,
                                           int lostPackets, long rttMillis, double smoothedRttMillis) {
        return completeModelRound(controller, sendAt, packets, lostPackets, rttMillis, smoothedRttMillis, 1_000);
    }

    private static long completeModelRound(RakModelCongestionController controller, long sendAt, int packets,
                                           int lostPackets, long rttMillis, double smoothedRttMillis,
                                           int payloadBytes) {
        List<RakDatagramPacket> flight = new ArrayList<>(packets);
        int inFlight = 0;
        for (int i = 0; i < packets; i++) {
            RakDatagramPacket datagram = datagram(payloadBytes);
            datagram.setSendTime(sendAt);
            inFlight += datagram.getSize();
            controller.onPacketSent(datagram, sendAt, inFlight, false);
            flight.add(datagram);
        }
        int currentInFlight = inFlight;
        long ackAt = sendAt + rttMillis;
        for (int i = 0; i < flight.size(); i++) {
            RakDatagramPacket datagram = flight.get(i);
            currentInFlight -= datagram.getSize();
            if (i < lostPackets) {
                controller.onLost(datagram, smoothedRttMillis);
            } else {
                controller.onAcknowledged(datagram, ackAt, rttMillis, smoothedRttMillis, currentInFlight);
            }
            datagram.release();
        }
        return ackAt + 1L;
    }

    private static long completeAppLimitedModelRound(RakModelCongestionController controller, long sendAt,
                                                     long rttMillis) {
        RakDatagramPacket datagram = datagram(1_000);
        try {
            datagram.setSendTime(sendAt);
            controller.onPacketSent(datagram, sendAt, datagram.getSize(), true);
            controller.onAcknowledged(datagram, sendAt + rttMillis, rttMillis, rttMillis, 0);
            return sendAt + rttMillis + 1L;
        } finally {
            datagram.release();
        }
    }

    @Test
    public void minimumRttAdaptsFromCleanHandshakeToStablePathStepAndBack() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 100L, 105L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());

            acknowledgeOne(window, packets, 1, 1_000L, 1_200L);
            acknowledgeOne(window, packets, 2, 1_220L, 1_420L);
            // First drained ACK starts the hold. Only original packets sent after that hold and the drain boundary
            // can contribute the two distinct low-flight confirmations.
            acknowledgeOne(window, packets, 3, 1_440L, 1_640L);
            acknowledgeOne(window, packets, 4, 1_850L, 2_050L);
            acknowledgeOne(window, packets, 5, 2_070L, 2_270L);
            Assertions.assertEquals(200L, window.getModelMinimumRttMillis(),
                    "a sustained propagation-delay step must replace the clean pre-impairment handshake minimum");

            acknowledgeOne(window, packets, 6, 2_500L, 2_505L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis(),
                    "path recovery is accepted immediately when lower-delay evidence returns");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void shallowPostHandshakePathStepRequiresDrainedStableEvidence() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 5L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());

            acknowledgeOne(window, packets, 1, 100L, 125L);
            acknowledgeOne(window, packets, 2, 140L, 165L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis(),
                    "busy-path suspicion alone cannot replace the handshake minimum");

            // The first drained ACK starts the hold. Two later original flights, sent after that hold and from
            // distinct delivery snapshots, are required to accept the new propagation path.
            acknowledgeOne(window, packets, 3, 180L, 205L);
            acknowledgeOne(window, packets, 4, 270L, 295L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());
            acknowledgeOne(window, packets, 5, 310L, 335L);
            Assertions.assertEquals(25L, window.getModelMinimumRttMillis(),
                    "a stable 5-to-25 ms post-connect step must not retain the shallow handshake minimum");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void idleOneWayTenMillisecondPathStepReplacesElevatedHandshakeMinimum() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 11L);
            Assertions.assertEquals(11L, window.getModelMinimumRttMillis());

            // This is the production netns shape: the route changes while only the low-rate probe stream is
            // active, adding about ten milliseconds one way with two milliseconds of jitter. The 19-22 ms
            // samples are a real propagation step but remain below the busy-path 2.5x threshold from 11 ms.
            acknowledgeUnreliable(window, 100L, 119L);
            acknowledgeUnreliable(window, 220L, 241L);
            acknowledgeUnreliable(window, 340L, 360L);
            acknowledgeUnreliable(window, 460L, 482L);
            acknowledgeUnreliable(window, 580L, 600L);

            Assertions.assertTrue(window.getModelMinimumRttMillis() >= 19L
                            && window.getModelMinimumRttMillis() <= 22L,
                    "stable low-flight samples must replace the pre-route-change handshake minimum");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void idleLowMillisecondPathStepToleratesAbsoluteNetemJitter() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 1L);

            // Four milliseconds of peak-to-peak jitter is a large ratio at this baseline, but every sample is
            // still low-flight and remains well above the old route. This exact 9-13 ms envelope occurred in the
            // external 10ms/2ms netns profile and must not exhaust the validator at the stale 1 ms minimum.
            acknowledgeUnreliable(window, 100L, 109L);
            acknowledgeUnreliable(window, 220L, 233L);
            acknowledgeUnreliable(window, 340L, 350L);
            acknowledgeUnreliable(window, 460L, 472L);
            acknowledgeUnreliable(window, 580L, 591L);

            Assertions.assertTrue(window.getModelMinimumRttMillis() >= 9L
                            && window.getModelMinimumRttMillis() <= 13L,
                    "absolute low-millisecond jitter must not strand the pre-impairment minimum");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void busyLowMillisecondPathStepToleratesAbsoluteNetemJitterAfterDrain() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 1L);

            // The external benchmark begins bulk traffic immediately after netem is installed, so the first
            // post-change samples are backlogged rather than app-limited. They must use the conservative busy
            // trigger, but once that trigger drains the flight, the same 9-13 ms low-millisecond jitter envelope
            // is stable propagation evidence rather than three failed probe attempts at the stale 1 ms minimum.
            acknowledgeOne(window, packets, 1, 100L, 109L);
            acknowledgeOne(window, packets, 2, 220L, 233L);
            acknowledgeOne(window, packets, 3, 340L, 350L);
            acknowledgeOne(window, packets, 4, 460L, 472L);
            acknowledgeOne(window, packets, 5, 580L, 591L);

            Assertions.assertTrue(window.getModelMinimumRttMillis() >= 9L
                            && window.getModelMinimumRttMillis() <= 13L,
                    "a drained busy probe must tolerate the external low-millisecond jitter envelope");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void backloggedTwoMtuSenderCannotUseIdlePathAdmission() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 11L);

            // Every send is physically low-flight, but appLimited=false records that more work was queued behind
            // it. Such a cwnd-limited sender must use the loaded-path threshold rather than treating its standing
            // queue as the idle probe stream.
            acknowledgeOne(window, packets, 1, 100L, 119L);
            acknowledgeOne(window, packets, 2, 220L, 241L);
            acknowledgeOne(window, packets, 3, 340L, 360L);
            acknowledgeOne(window, packets, 4, 460L, 482L);
            acknowledgeOne(window, packets, 5, 580L, 600L);

            Assertions.assertEquals(11L, window.getModelMinimumRttMillis(),
                    "backlogged low-flight traffic cannot enter the app-limited path envelope");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void idleHighRttVariationCannotMasqueradeAsAStablePathStep() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 60L);

            long sendAt = 200L;
            for (int attempt = 0; attempt < 3; attempt++) {
                acknowledgeUnreliable(window, sendAt, sendAt + 100L);
                sendAt += 400L;
                acknowledgeUnreliable(window, sendAt, sendAt + 149L);
                sendAt += 500L;
            }

            Assertions.assertEquals(60L, window.getModelMinimumRttMillis(),
                    "a 100-149 ms low-flight spread is not stable propagation evidence");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void busyOneWayTenMillisecondDelayCannotUseIdlePathAdmission() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 11L);

            for (int i = 1; i <= 8; i++) {
                RakDatagramPacket datagram = datagram(1_000);
                packets.add(datagram);
                datagram.setSequenceIndex(i);
                datagram.setSendOrdinal(i);
                datagram.setSendTime(100L);
                window.onReliableSend(datagram);
            }
            for (int i = 1; i <= 8; i++) {
                window.onAck(120L, packets.get(i), i + 1L);
            }

            Assertions.assertEquals(11L, window.getModelMinimumRttMillis(),
                    "a full-flight queue delay must not enter the low-flight path-step path");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void agedMinimumRttStillRequiresStableDrainedPathEvidence() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 5L);
            acknowledgeOne(window, packets, 1, 10_000L, 10_200L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis(),
                    "an aged minimum cannot accept one high low-flight sample outside the path validator");

            acknowledgeOne(window, packets, 2, 10_220L, 10_420L);
            acknowledgeOne(window, packets, 3, 10_440L, 10_640L);
            acknowledgeOne(window, packets, 4, 10_850L, 11_050L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());
            acknowledgeOne(window, packets, 5, 11_070L, 11_270L);
            Assertions.assertEquals(200L, window.getModelMinimumRttMillis());
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void handshakeOnlyRoundsKeepStartupProgressFloorAndCannotDeclarePlateau() {
        RakModelCongestionController controller = new RakModelCongestionController(MTU, 10L);
        long now = 0L;
        for (int round = 0; round < 10; round++) {
            now = completeModelRound(controller, now, 1, 0, 5L, 5D, 1);
        }
        Assertions.assertTrue(controller.isStartup(),
                "sub-four-MTU handshake rounds cannot exhaust full-bandwidth discovery");
        Assertions.assertTrue(controller.getPacingRateBytesPerMillis() >= 9D * MTU / 10D,
                "startup must be able to exercise the standards-sized initial window per send opportunity");
    }

    @Test
    public void oldPreDrainAckCannotCancelProbeAndTimeoutRestoresBeforeCooldownRetry() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 5L);
            double usefulCwnd = window.getCongestionWindow();
            acknowledgeOne(window, packets, 1, 100L, 300L);

            RakDatagramPacket confirmation = datagram(1_000);
            packets.add(confirmation);
            confirmation.setSequenceIndex(2);
            confirmation.setSendOrdinal(2L);
            confirmation.setSendTime(400L);
            window.onReliableSend(confirmation);

            RakDatagramPacket oldLowRtt = datagram(1_000);
            packets.add(oldLowRtt);
            oldLowRtt.setSequenceIndex(3);
            oldLowRtt.setSendOrdinal(3L);
            oldLowRtt.setSendTime(598L);
            window.onReliableSend(oldLowRtt);

            RakDatagramPacket timeoutStaleAck = datagram(1_000);
            packets.add(timeoutStaleAck);
            timeoutStaleAck.setSequenceIndex(4);
            timeoutStaleAck.setSendOrdinal(4L);
            timeoutStaleAck.setSendTime(599L);
            window.onReliableSend(timeoutStaleAck);

            window.onAck(600L, confirmation, 4L);
            Assertions.assertEquals(2D * MTU, window.getCongestionWindow());
            window.onAck(603L, oldLowRtt, 4L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());
            Assertions.assertEquals(2D * MTU, window.getCongestionWindow(),
                    "a pre-boundary ACK cannot strand or cancel the active drain");

            window.onAck(2_601L, timeoutStaleAck, 5L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis(),
                    "a pre-boundary ACK at the deadline cannot erase the completed attempt");
            Assertions.assertTrue(window.getCongestionWindow() > 2D * MTU,
                    "a bounded probe timeout must restore a useful pre-probe window");
            Assertions.assertTrue(window.getCongestionWindow() <= usefulCwnd);

            // The 400 ms cooldown for a 200 ms suspect RTT expires at 3001 ms. A later stable attempt can retry and
            // accept the path using only post-drain original low-flight samples.
            acknowledgeOne(window, packets, 5, 3_002L, 3_202L);
            acknowledgeOne(window, packets, 6, 3_220L, 3_420L);
            acknowledgeOne(window, packets, 7, 3_440L, 3_640L);
            acknowledgeOne(window, packets, 8, 3_860L, 4_060L);
            acknowledgeOne(window, packets, 9, 4_080L, 4_280L);
            Assertions.assertEquals(200L, window.getModelMinimumRttMillis(),
                    "cooldown expiry must permit a successful bounded retry");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void recoveryOnlyAdmissionAdvancesPathTimeoutAndRefreshesCachedWindow() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 5L);
            acknowledgeOne(window, packets, 1, 100L, 300L);
            acknowledgeOne(window, packets, 2, 320L, 520L);
            Assertions.assertEquals(2D * MTU, window.getCongestionWindow());

            for (int i = 0; i < 3; i++) {
                RakDatagramPacket datagram = datagram(1_000);
                packets.add(datagram);
                datagram.setSequenceIndex(3 + i);
                datagram.setSendOrdinal(3L + i);
                datagram.setSendTime(530L);
                window.onReliableSend(datagram);
            }
            RakDatagramPacket retry = packets.get(3);
            Assertions.assertTrue(window.onBoundedLoss(retry, 5L));
            Assertions.assertTrue(window.getBytesInFlight() + retry.getSize() > 2D * MTU);

            Assertions.assertTrue(window.canSendBoundedRecovery(retry.getSize(), 2_521L),
                    "recovery-first admission must advance the probe timeout before checking restored flight");
            Assertions.assertTrue(window.getCongestionWindow() > 2D * MTU,
                    "the public cached window must reflect the controller's timeout restoration");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void pathDeadlineSaturatesAtMaximumClockValue() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            long base = Long.MAX_VALUE - 1_000L;
            acknowledgeOne(window, packets, 0, base, base + 5L);
            acknowledgeOne(window, packets, 1, base + 100L, base + 300L);
            acknowledgeOne(window, packets, 2, base + 350L, base + 550L);
            Assertions.assertEquals(2D * MTU, window.getCongestionWindow());

            window.getTransmissionBandwidth(Long.MAX_VALUE - 1L);
            Assertions.assertEquals(2D * MTU, window.getCongestionWindow(),
                    "an overflowing deadline must saturate rather than expire immediately");
            window.getTransmissionBandwidth(Long.MAX_VALUE);
            Assertions.assertTrue(window.getCongestionWindow() > 2D * MTU);
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void staleAckAtDeadlineCannotEraseCompletedAttemptBudget() {
        RakModelCongestionController controller = new RakModelCongestionController(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            RakDatagramPacket baseline = directSend(controller, packets, 0L, MTU);
            controller.onAcknowledged(baseline, 1_000L, 1_000L, 1_000D, 0);

            RakDatagramPacket firstHigh = directSend(controller, packets, 1_100L, MTU);
            RakDatagramPacket progress = directSend(controller, packets, 1_200L, 2 * MTU);
            controller.onAcknowledged(progress, 2_200L, 1_000L, 1_000D, MTU);

            RakDatagramPacket confirmation = directSend(controller, packets, 2_300L, 2 * MTU);
            controller.onAcknowledged(firstHigh, 5_600L, 4_500L, 4_500D, MTU);
            RakDatagramPacket stale = directSend(controller, packets, 6_799L, 2 * MTU);
            controller.onAcknowledged(confirmation, 6_800L, 4_500L, 4_500D, MTU);
            Assertions.assertEquals(2D * MTU, controller.getCongestionWindow());

            controller.onAcknowledged(stale, 9_800L, 3_001L, 3_001D, 0);
            Assertions.assertEquals(1_000L, controller.getMinimumRttMillis(),
                    "a stale 3001 ms ACK at the 3 s deadline cannot refresh or cancel the completed attempt");
            Assertions.assertEquals(1, controller.getPathAttempts(),
                    "the completed attempt budget and cooldown must survive the deadline-triggering stale ACK");
            Assertions.assertTrue(controller.getCongestionWindow() > 2D * MTU);
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
        }
    }

    @Test
    public void lostFirstCandidateDoesNotValidateOrPermanentlyBlockPathStep() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 5L);
            acknowledgeOne(window, packets, 1, 100L, 300L);
            acknowledgeOne(window, packets, 2, 320L, 520L);
            acknowledgeOne(window, packets, 3, 540L, 740L);

            RakDatagramPacket lostCandidate = datagram(1_000);
            packets.add(lostCandidate);
            lostCandidate.setSequenceIndex(4);
            lostCandidate.setSendOrdinal(4L);
            lostCandidate.setSendTime(950L);
            window.onReliableSend(lostCandidate);
            Assertions.assertTrue(window.onBoundedLoss(lostCandidate, 4L));
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis(),
                    "loss has no Karn-safe RTT sample and cannot validate a candidate");

            acknowledgeOne(window, packets, 5, 960L, 1_160L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());
            acknowledgeOne(window, packets, 6, 1_180L, 1_380L);
            Assertions.assertEquals(200L, window.getModelMinimumRttMillis(),
                    "later distinct original samples must still complete the path step");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void timedOutCandidateCannotValidateALaterRetry() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 5L);
            acknowledgeOne(window, packets, 1, 100L, 300L);
            acknowledgeOne(window, packets, 2, 320L, 520L);
            acknowledgeOne(window, packets, 3, 540L, 740L);
            acknowledgeOne(window, packets, 4, 950L, 1_150L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());

            window.getTransmissionBandwidth(2_521L);
            acknowledgeOne(window, packets, 5, 2_922L, 3_122L);
            acknowledgeOne(window, packets, 6, 3_140L, 3_340L);
            acknowledgeOne(window, packets, 7, 3_360L, 3_560L);
            acknowledgeOne(window, packets, 8, 3_770L, 3_970L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis(),
                    "a candidate from a timed-out attempt cannot count toward a retry");
            acknowledgeOne(window, packets, 9, 3_990L, 4_190L);
            Assertions.assertEquals(200L, window.getModelMinimumRttMillis());
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void risingQueueDelayIsNotAcceptedAsAPropagationPathStep() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 100L, 200L);
            long[] risingRtts = {210L, 240L, 280L, 320L, 360L};
            long sendAt = 500L;
            for (int i = 0; i < risingRtts.length; i++) {
                acknowledgeOne(window, packets, i + 1, sendAt, sendAt + risingRtts[i]);
                sendAt += 400L;
            }
            Assertions.assertEquals(100L, window.getModelMinimumRttMillis(),
                    "a rising delay trend is queue evidence, not a stable propagation-delay step");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void stableDeepQueueDuringAFullFlightCannotReplaceMinimumRtt() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 100L, 105L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());

            for (int i = 1; i <= 10; i++) {
                RakDatagramPacket datagram = datagram(1_000);
                packets.add(datagram);
                datagram.setSequenceIndex(i);
                datagram.setSendOrdinal(i);
                datagram.setSendTime(1_000L);
                window.onReliableSend(datagram);
            }
            for (int i = 1; i <= 10; i++) {
                window.onAck(1_025L, packets.get(i), i + 1L);
            }

            for (int i = 11; i <= 20; i++) {
                RakDatagramPacket datagram = datagram(1_000);
                packets.add(datagram);
                datagram.setSequenceIndex(i);
                datagram.setSendOrdinal(i);
                datagram.setSendTime(1_500L);
                window.onReliableSend(datagram);
            }
            for (int i = 11; i <= 20; i++) {
                window.onAck(1_525L, packets.get(i), i + 1L);
            }

            Assertions.assertEquals(5L, window.getModelMinimumRttMillis(),
                    "stable 25 ms delay from full flights is queue evidence, not a drained path step");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void oneAckBatchDelaySpikeCannotTriggerPathProbeOrCollapseWindow() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 5L);
            double initialCwnd = window.getCongestionWindow();

            for (int i = 1; i <= 10; i++) {
                RakDatagramPacket datagram = datagram(1_000);
                packets.add(datagram);
                datagram.setSequenceIndex(i);
                datagram.setSendOrdinal(i);
                datagram.setSendTime(100L);
                window.onReliableSend(datagram);
            }
            for (int i = 1; i <= 10; i++) {
                window.onAck(200L, packets.get(i), i + 1L);
            }

            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());
            Assertions.assertEquals(initialCwnd, window.getCongestionWindow(),
                    "many delayed ACKs from one flight are one scheduler-stall observation, not a path step");

            acknowledgeOne(window, packets, 11, 300L, 305L);
            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());
            Assertions.assertEquals(initialCwnd, window.getCongestionWindow());
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void oneAckBatchWithDifferentSendSnapshotsCannotTriggerPathProbe() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            acknowledgeOne(window, packets, 0, 0L, 5L);
            double initialCwnd = window.getCongestionWindow();

            RakDatagramPacket heldA = datagram(1_000);
            packets.add(heldA);
            heldA.setSequenceIndex(1);
            heldA.setSendOrdinal(1L);
            heldA.setSendTime(100L);
            window.onReliableSend(heldA);

            acknowledgeOne(window, packets, 2, 101L, 106L);

            RakDatagramPacket heldB = datagram(1_000);
            packets.add(heldB);
            heldB.setSequenceIndex(3);
            heldB.setSendOrdinal(3L);
            heldB.setSendTime(110L);
            window.onReliableSend(heldB);
            Assertions.assertNotEquals(heldA.getDeliveredBytesAtSend(), heldB.getDeliveredBytesAtSend(),
                    "the regression requires packets sent across delivery progress");

            window.onAck(300L, heldA, 4L);
            window.onAck(300L, heldB, 4L);

            Assertions.assertEquals(5L, window.getModelMinimumRttMillis());
            Assertions.assertEquals(initialCwnd, window.getCongestionWindow(),
                    "different send snapshots in one stalled ACK observation are not distinct path evidence");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void retransmittedAcknowledgementCannotInflateDeliveryRateOrReceiveDoubleCredit() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            for (int i = 0; i < 6; i++) {
                acknowledgeOne(window, packets, i, 100L + i, 300L);
            }
            double rateBeforeRetry = window.getModelBandwidthBytesPerMillis();
            int unackedBeforeRetry = window.getUnackedBytes();

            RakDatagramPacket retry = datagram(1_000);
            packets.add(retry);
            retry.setSequenceIndex(10);
            retry.setSendOrdinal(10L);
            retry.setSendTime(400L);
            window.onReliableSend(retry);
            window.onBoundedLoss(retry, 10L);
            window.onBoundedRetransmit(retry, false, 450L);
            retry.setSendTime(450L);
            retry.markRetransmitted();
            window.onAck(451L, retry, 11L);

            Assertions.assertEquals(rateBeforeRetry, window.getModelBandwidthBytesPerMillis(),
                    "an ambiguous one-millisecond retry ACK is not a delivery-rate sample");
            Assertions.assertEquals(unackedBeforeRetry, window.getUnackedBytes());
            window.onAck(452L, retry, 12L);
            Assertions.assertEquals(unackedBeforeRetry, window.getUnackedBytes(),
                    "a duplicate/late ACK cannot credit the same logical datagram twice");
            Assertions.assertEquals(rateBeforeRetry, window.getModelBandwidthBytesPerMillis());
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void idlePacerCannotAccumulateAnUnboundedCatchUpBurst() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                acknowledgeOne(window, packets, i, 100L + i, 300L);
            }
            RakDatagramPacket appLimited = datagram(1_000);
            packets.add(appLimited);
            appLimited.setSequenceIndex(10);
            appLimited.setSendOrdinal(10L);
            appLimited.setSendTime(400L);
            window.onReliableSend(appLimited, true);
            window.onAck(500L, appLimited, 11L);

            int allowanceAfterLongIdle = window.getTransmissionBandwidth(1_000_000L);
            int oneQuantumCap = (int) Math.ceil(Math.max(2D * MTU, Math.min(8D * MTU,
                    window.getModelPacingRateBytesPerMillis() * 10D + MTU)));
            Assertions.assertTrue(allowanceAfterLongIdle <= oneQuantumCap,
                    "a genuinely app-limited idle period retains the one-quantum pacing cap");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    @Test
    public void explicitQueueDrainClearsBackloggedCatchUpCredit() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> packets = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                acknowledgeOne(window, packets, i, 100L + i, 300L);
            }
            RakDatagramPacket backlogged = datagram(1_000);
            packets.add(backlogged);
            backlogged.setSequenceIndex(10);
            backlogged.setSendOrdinal(10L);
            backlogged.setSendTime(400L);
            window.onReliableSend(backlogged, false);
            window.onAck(500L, backlogged, 11L);

            window.onSenderIdle();
            int allowanceAfterLongIdle = window.getTransmissionBandwidth(1_000_000L);
            int oneQuantumCap = (int) Math.ceil(Math.max(2D * MTU, Math.min(8D * MTU,
                    window.getModelPacingRateBytesPerMillis() * 10D + MTU)));
            Assertions.assertTrue(allowanceAfterLongIdle <= oneQuantumCap,
                    "draining both transport queues clears stale backlogged pacing credit");
        } finally {
            for (RakDatagramPacket datagram : packets) {
                releaseIfNeeded(datagram);
            }
            window.close();
        }
    }

    private static RakDatagramPacket directSend(RakModelCongestionController controller,
                                                List<RakDatagramPacket> packets, long sendAt,
                                                int bytesInFlight) {
        RakDatagramPacket datagram = datagram(1_000);
        packets.add(datagram);
        datagram.setSendTime(sendAt);
        controller.onPacketSent(datagram, sendAt, bytesInFlight, false);
        return datagram;
    }

    private static void acknowledgeOne(RakSlidingWindow window, List<RakDatagramPacket> packets, int index,
                                       long sendAt, long ackAt) {
        RakDatagramPacket datagram = datagram(1_000);
        packets.add(datagram);
        datagram.setSequenceIndex(index);
        datagram.setSendOrdinal(index);
        datagram.setSendTime(sendAt);
        window.onReliableSend(datagram);
        window.onAck(ackAt, datagram, index + 1L);
    }

    private static void acknowledgeUnreliable(RakSlidingWindow window, long sendAt, long ackAt) {
        RakSlidingWindow.ModelDatagramSample sample = window.onUnreliableSendTracked(1_000, sendAt, true);
        window.onUnreliableAck(sample, ackAt);
    }

    private static RakDatagramPacket datagram(int payloadBytes) {
        return datagram(payloadBytes, RakReliability.RELIABLE);
    }

    private static RakDatagramPacket orderedDatagram(int payloadBytes) {
        return datagram(payloadBytes, RakReliability.RELIABLE_ORDERED);
    }

    private static int datagramWireSize(int payloadBytes) {
        RakDatagramPacket datagram = datagram(payloadBytes);
        try {
            return datagram.getSize();
        } finally {
            datagram.release();
        }
    }

    private static RakDatagramPacket datagram(int payloadBytes, RakReliability reliability) {
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setReliability(reliability);
        packet.setBuffer(Unpooled.buffer(payloadBytes).writeZero(payloadBytes));

        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        if (!datagram.tryAddPacket(packet, MTU)) {
            packet.release();
            datagram.release();
            throw new AssertionError("test datagram does not fit MTU");
        }
        return datagram;
    }

    private static void releaseIfNeeded(RakDatagramPacket datagram) {
        if (datagram.refCnt() > 0) {
            datagram.release();
        }
    }

    private static final class Delivery {
        private final long at;
        private final RakDatagramPacket datagram;
        private final boolean lost;
        private final boolean deliversPayload;
        private final boolean completesSender;

        private Delivery(long at, RakDatagramPacket datagram, boolean lost) {
            this(at, datagram, lost, !lost, true);
        }

        private Delivery(long at, RakDatagramPacket datagram, boolean lost, boolean deliversPayload) {
            this(at, datagram, lost, deliversPayload, true);
        }

        private Delivery(long at, RakDatagramPacket datagram, boolean lost, boolean deliversPayload,
                         boolean completesSender) {
            this.at = at;
            this.datagram = datagram;
            this.lost = lost;
            this.deliversPayload = deliversPayload;
            this.completesSender = completesSender;
        }
    }

    @FunctionalInterface
    private interface LossPattern {
        boolean isLost(int sentDatagrams, long sendAtMillis);
    }

    @FunctionalInterface
    private interface PathRttPattern {
        long rttMillis(int sentDatagrams, long sendAtMillis);
    }

    private static final class MixedDelivery {
        private final long at;
        private final RakDatagramPacket datagram;
        private final RakSlidingWindow.ModelDatagramSample sample;
        private final boolean lost;

        private MixedDelivery(long at, RakDatagramPacket datagram,
                              RakSlidingWindow.ModelDatagramSample sample, boolean lost) {
            this.at = at;
            this.datagram = datagram;
            this.sample = sample;
            this.lost = lost;
        }

        private static MixedDelivery reliable(long at, RakDatagramPacket datagram, boolean lost) {
            return new MixedDelivery(at, datagram, null, lost);
        }

        private static MixedDelivery unreliable(long at, RakSlidingWindow.ModelDatagramSample sample,
                                                boolean lost) {
            return new MixedDelivery(at, null, sample, lost);
        }
    }

    private static final class SimulationResult {
        private final double measuredMbps;
        private final double finalCwnd;
        private final int maxBytesSentInTick;
        private final long rounds;
        private final int retransmissions;
        private final int maxPendingRetries;
        private final long finalMinimumRttMillis;
        private final long lossResponses;
        private final long hardLossResponses;
        private final long delayLossResponses;
        private final double finalBandwidthBytesPerMillis;
        private final double finalPacingBytesPerMillis;
        private final double finalInflightLimit;
        private final boolean lossResponseHeld;
        private final long pathRebasedAtMillis;
        private final long maximumMinimumCwndDurationMillis;

        private SimulationResult(double measuredMbps, double finalCwnd, int maxBytesSentInTick, long rounds,
                                 int retransmissions, int maxPendingRetries) {
            this(measuredMbps, finalCwnd, maxBytesSentInTick, rounds, retransmissions, maxPendingRetries, -1L);
        }

        private SimulationResult(double measuredMbps, double finalCwnd, int maxBytesSentInTick, long rounds,
                                 int retransmissions, int maxPendingRetries, long finalMinimumRttMillis) {
            this(measuredMbps, finalCwnd, maxBytesSentInTick, rounds, retransmissions, maxPendingRetries,
                    finalMinimumRttMillis, 0L, 0L, 0L);
        }

        private SimulationResult(double measuredMbps, double finalCwnd, int maxBytesSentInTick, long rounds,
                                 int retransmissions, int maxPendingRetries, long finalMinimumRttMillis,
                                 long lossResponses, long hardLossResponses, long delayLossResponses) {
            this(measuredMbps, finalCwnd, maxBytesSentInTick, rounds, retransmissions, maxPendingRetries,
                    finalMinimumRttMillis, lossResponses, hardLossResponses, delayLossResponses, -1D, -1D);
        }

        private SimulationResult(double measuredMbps, double finalCwnd, int maxBytesSentInTick, long rounds,
                                 int retransmissions, int maxPendingRetries, long finalMinimumRttMillis,
                                 long lossResponses, long hardLossResponses, long delayLossResponses,
                                 double finalBandwidthBytesPerMillis, double finalPacingBytesPerMillis) {
            this(measuredMbps, finalCwnd, maxBytesSentInTick, rounds, retransmissions, maxPendingRetries,
                    finalMinimumRttMillis, lossResponses, hardLossResponses, delayLossResponses,
                    finalBandwidthBytesPerMillis, finalPacingBytesPerMillis, Double.POSITIVE_INFINITY, false);
        }

        private SimulationResult(double measuredMbps, double finalCwnd, int maxBytesSentInTick, long rounds,
                                 int retransmissions, int maxPendingRetries, long finalMinimumRttMillis,
                                 long lossResponses, long hardLossResponses, long delayLossResponses,
                                 double finalBandwidthBytesPerMillis, double finalPacingBytesPerMillis,
                                 double finalInflightLimit, boolean lossResponseHeld) {
            this(measuredMbps, finalCwnd, maxBytesSentInTick, rounds, retransmissions, maxPendingRetries,
                    finalMinimumRttMillis, lossResponses, hardLossResponses, delayLossResponses,
                    finalBandwidthBytesPerMillis, finalPacingBytesPerMillis, finalInflightLimit,
                    lossResponseHeld, -1L, 0L);
        }

        private SimulationResult(double measuredMbps, double finalCwnd, int maxBytesSentInTick, long rounds,
                                 int retransmissions, int maxPendingRetries, long finalMinimumRttMillis,
                                 long lossResponses, long hardLossResponses, long delayLossResponses,
                                 double finalBandwidthBytesPerMillis, double finalPacingBytesPerMillis,
                                 double finalInflightLimit, boolean lossResponseHeld,
                                 long pathRebasedAtMillis, long maximumMinimumCwndDurationMillis) {
            this.measuredMbps = measuredMbps;
            this.finalCwnd = finalCwnd;
            this.maxBytesSentInTick = maxBytesSentInTick;
            this.rounds = rounds;
            this.retransmissions = retransmissions;
            this.maxPendingRetries = maxPendingRetries;
            this.finalMinimumRttMillis = finalMinimumRttMillis;
            this.lossResponses = lossResponses;
            this.hardLossResponses = hardLossResponses;
            this.delayLossResponses = delayLossResponses;
            this.finalBandwidthBytesPerMillis = finalBandwidthBytesPerMillis;
            this.finalPacingBytesPerMillis = finalPacingBytesPerMillis;
            this.finalInflightLimit = finalInflightLimit;
            this.lossResponseHeld = lossResponseHeld;
            this.pathRebasedAtMillis = pathRebasedAtMillis;
            this.maximumMinimumCwndDurationMillis = maximumMinimumCwndDurationMillis;
        }
    }

    private static final class CapacityStepResult {
        private final double stepUpMbps;
        private final double restartMbps;
        private final int maxBytesSentInTick;
        private final double finalBandwidthBytesPerMillis;

        private CapacityStepResult(double stepUpMbps, double restartMbps, int maxBytesSentInTick,
                                   double finalBandwidthBytesPerMillis) {
            this.stepUpMbps = stepUpMbps;
            this.restartMbps = restartMbps;
            this.maxBytesSentInTick = maxBytesSentInTick;
            this.finalBandwidthBytesPerMillis = finalBandwidthBytesPerMillis;
        }
    }

    private static final class QuantizedSendResult {
        private final double measuredMbps;
        private final double finalCwnd;
        private final long minimumObservedRttMillis;
        private final int maxBytesSentInTick;
        private final long finalQueuedBytes;
        private final double finalBandwidthBytesPerMillis;
        private final long finalMinimumRttMillis;

        private QuantizedSendResult(double measuredMbps, double finalCwnd, long minimumObservedRttMillis,
                                    int maxBytesSentInTick, long finalQueuedBytes,
                                    double finalBandwidthBytesPerMillis, long finalMinimumRttMillis) {
            this.measuredMbps = measuredMbps;
            this.finalCwnd = finalCwnd;
            this.minimumObservedRttMillis = minimumObservedRttMillis;
            this.maxBytesSentInTick = maxBytesSentInTick;
            this.finalQueuedBytes = finalQueuedBytes;
            this.finalBandwidthBytesPerMillis = finalBandwidthBytesPerMillis;
            this.finalMinimumRttMillis = finalMinimumRttMillis;
        }
    }
}
