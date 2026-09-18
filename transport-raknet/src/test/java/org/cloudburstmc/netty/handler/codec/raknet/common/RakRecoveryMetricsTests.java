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

import org.cloudburstmc.netty.channel.raknet.RakSlidingWindow;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelMetrics;
import org.cloudburstmc.netty.channel.raknet.config.RakDatagramSendType;
import org.cloudburstmc.netty.channel.raknet.packet.RakDatagramPacket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RakRecoveryMetricsTests {

    @Test
    public void reportsSendTypesAttemptsAndRecoveryTiming() {
        RecordingMetrics metrics = new RecordingMetrics();
        RakRecoveryMetrics recovery = new RakRecoveryMetrics();
        RakSlidingWindow window = new RakSlidingWindow(1_200);
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        datagram.setSendTime(100L);
        datagram.setSequenceIndex(1);

        try {
            window.onReliableSend(datagram);
            recovery.initialize(metrics, window, 100L);
            recovery.onDatagramSent(metrics, window, datagram, RakDatagramSendType.ORIGINAL, 110L);
            recovery.onDatagramSent(metrics, window, datagram, RakDatagramSendType.TIMEOUT_RETRANSMISSION, 210L);
            recovery.onDatagramSent(metrics, window, datagram, RakDatagramSendType.NACK_RETRANSMISSION, 250L);

            Assertions.assertEquals(3, metrics.sends.size());
            Assertions.assertEquals(RakDatagramSendType.ORIGINAL, metrics.sends.get(0).sendType);
            Assertions.assertEquals(0, metrics.sends.get(0).attempt);
            Assertions.assertEquals(RakDatagramSendType.TIMEOUT_RETRANSMISSION, metrics.sends.get(1).sendType);
            Assertions.assertEquals(1, metrics.sends.get(1).attempt);
            Assertions.assertEquals(RakDatagramSendType.NACK_RETRANSMISSION, metrics.sends.get(2).sendType);
            Assertions.assertEquals(2, metrics.sends.get(2).attempt);
            Assertions.assertEquals(datagram.getSize(), metrics.sends.get(2).bytes);

            RecoveryState activeRecovery = metrics.states.get(metrics.states.size() - 1);
            Assertions.assertEquals(1, activeRecovery.retransmittedInFlight);
            Assertions.assertEquals(210L, activeRecovery.recoveryStartedAtMillis);

            window.onAck(400L, datagram, 2L);
            recovery.onAcknowledgementProgress(metrics, window, datagram, 400L);

            Assertions.assertEquals(1, metrics.acknowledgements.size());
            Acknowledgement acknowledgement = metrics.acknowledgements.get(0);
            Assertions.assertEquals(2, acknowledgement.attempt);
            Assertions.assertEquals(400L, acknowledgement.observedAtMillis);
            Assertions.assertEquals(100L, acknowledgement.previousAckProgressAtMillis);
            Assertions.assertEquals(210L, acknowledgement.recoveryStartedAtMillis);

            RecoveryState recovered = metrics.states.get(metrics.states.size() - 1);
            Assertions.assertEquals(0, recovered.bytesInFlight);
            Assertions.assertEquals(0, recovered.retransmittedInFlight);
            Assertions.assertEquals(400L, recovered.lastAckProgressAtMillis);
            Assertions.assertEquals(-1L, recovered.recoveryStartedAtMillis);
            Assertions.assertEquals(-1.0D, recovered.smoothedRtt,
                    "Karn's algorithm excludes an ACK after retransmission from RTT sampling");
            Assertions.assertEquals(-1.0D, recovered.rttVariance);
            Assertions.assertEquals(1_000L, recovered.retransmissionTimeout);
        } finally {
            datagram.release();
        }
    }

    @Test
    public void rateLimitsStateButForcesRecoveryTransitionsAndTerminalState() {
        RecordingMetrics metrics = new RecordingMetrics();
        RakRecoveryMetrics recovery = new RakRecoveryMetrics();
        RakSlidingWindow window = new RakSlidingWindow(1_200);
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();

        try {
            window.onReliableSend(datagram);
            recovery.initialize(metrics, window, 1_000L);
            recovery.reportState(metrics, window, 1_050L);
            Assertions.assertEquals(1, metrics.states.size());

            recovery.onDatagramSent(metrics, window, datagram,
                    RakDatagramSendType.TIMEOUT_RETRANSMISSION, 1_060L);
            Assertions.assertEquals(2, metrics.states.size(), "recovery start must be reported immediately");

            recovery.reportState(metrics, window, 1_100L);
            Assertions.assertEquals(2, metrics.states.size());
            recovery.reportState(metrics, window, 1_160L);
            Assertions.assertEquals(3, metrics.states.size());

            recovery.close(metrics, window, 1_170L);
            Assertions.assertEquals(4, metrics.states.size());
            RecoveryState terminal = metrics.states.get(3);
            Assertions.assertEquals(0, terminal.bytesInFlight);
            Assertions.assertEquals(0, terminal.retransmittedInFlight);
            Assertions.assertEquals(-1L, terminal.recoveryStartedAtMillis);
            Assertions.assertEquals(Collections.singletonList(1_170L), metrics.closedAtMillis);
        } finally {
            datagram.release();
        }
    }

    @Test
    public void retainsAttemptsAndRecoveryTimingWhenMetricsAreInstalledAtRuntime() {
        RecordingMetrics metrics = new RecordingMetrics();
        RakRecoveryMetrics recovery = new RakRecoveryMetrics();
        RakSlidingWindow window = new RakSlidingWindow(1_200);
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();

        try {
            window.onReliableSend(datagram);
            recovery.initialize(null, window, 1_000L);
            recovery.onDatagramSent(null, window, datagram, RakDatagramSendType.ORIGINAL, 1_010L);
            recovery.onDatagramSent(null, window, datagram,
                    RakDatagramSendType.TIMEOUT_RETRANSMISSION, 1_100L);
            recovery.onDatagramSent(metrics, window, datagram,
                    RakDatagramSendType.NACK_RETRANSMISSION, 1_120L);

            Assertions.assertEquals(1, metrics.sends.size());
            Assertions.assertEquals(2, metrics.sends.get(0).attempt);

            window.onAck(1_200L, datagram, 2L);
            recovery.onAcknowledgementProgress(metrics, window, datagram, 1_200L);

            Assertions.assertEquals(1_000L, metrics.acknowledgements.get(0).previousAckProgressAtMillis);
            Assertions.assertEquals(1_100L, metrics.acknowledgements.get(0).recoveryStartedAtMillis);
        } finally {
            datagram.release();
        }
    }

    @Test
    public void exposesSlidingWindowStateWithoutChangingIt() {
        RakSlidingWindow window = new RakSlidingWindow(1_200);
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        datagram.setSendTime(100L);
        datagram.setSequenceIndex(1);

        try {
            double initialWindow = window.getCongestionWindow();
            Assertions.assertEquals(12_000.0D, initialWindow);
            Assertions.assertEquals(0.0D, window.getSlowStartThreshold());
            Assertions.assertEquals(-1.0D, window.getRTT());
            Assertions.assertEquals(-1.0D, window.getRttDeviation());
            Assertions.assertEquals(1_000L, window.getRtoForRetransmission());

            window.onReliableSend(datagram);
            Assertions.assertEquals(datagram.getSize(), window.getUnackedBytes());
            window.onAck(200L, datagram, 2L);

            Assertions.assertEquals(0, window.getUnackedBytes());
            Assertions.assertEquals(100.0D, window.getRTT());
            Assertions.assertEquals(100.0D, window.getRttDeviation());
            Assertions.assertEquals(530L, window.getRtoForRetransmission());
            Assertions.assertEquals(initialWindow, window.getCongestionWindow(),
                    "one delivery sample does not replace the model's standards-sized initial window");
        } finally {
            datagram.release();
        }
    }

    @Test
    public void attemptsStateRemovalWhenTerminalMetricsCallbackThrows() {
        RakRecoveryMetrics recovery = new RakRecoveryMetrics();
        RakSlidingWindow window = new RakSlidingWindow(1_200);
        boolean[] closed = {false};
        RakChannelMetrics throwingMetrics = new RakChannelMetrics() {
            @Override
            public void rakRecoveryState(long observedAtMillis, int bytesInFlight, double congestionWindow,
                                         double slowStartThreshold, double smoothedRtt, double rttVariance,
                                         long retransmissionTimeout, int retransmittedDatagramsInFlight,
                                         long lastAckProgressAtMillis, long recoveryStartedAtMillis) {
                throw new IllegalStateException("terminal state failure");
            }

            @Override
            public void rakRecoveryStateClosed(long observedAtMillis) {
                closed[0] = true;
            }
        };

        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> recovery.close(throwingMetrics, window, 1_000L));

        Assertions.assertEquals("terminal state failure", error.getMessage());
        Assertions.assertTrue(closed[0], "state removal must be attempted even when terminal state reporting fails");
    }

    @Test
    public void attemptsStateRemovalWhenTerminalModelCallbackThrows() {
        RakRecoveryMetrics recovery = new RakRecoveryMetrics();
        RakSlidingWindow window = new RakSlidingWindow(1_200);
        boolean[] closed = {false};
        RakChannelMetrics throwingMetrics = new RakChannelMetrics() {
            @Override
            public void rakCongestionModelState(long observedAtMillis,
                                                double estimatedDeliveryRateBytesPerSecond,
                                                double pacingRateBytesPerSecond, long minimumRttMillis,
                                                double recentLossRate, long packetRound, boolean startup,
                                                boolean persistentCongestion) {
                throw new IllegalStateException("terminal model failure");
            }

            @Override
            public void rakRecoveryStateClosed(long observedAtMillis) {
                closed[0] = true;
            }
        };

        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> recovery.close(throwingMetrics, window, 1_000L));

        Assertions.assertEquals("terminal model failure", error.getMessage());
        Assertions.assertTrue(closed[0], "state removal must be attempted when terminal model reporting fails");
    }

    @Test
    public void unsampledModelStateUsesUnavailableSentinels() {
        RakRecoveryMetrics recovery = new RakRecoveryMetrics();
        RakSlidingWindow window = new RakSlidingWindow(1_200);
        double[] deliveryRate = {Double.NaN};
        double[] pacingRate = {Double.NaN};
        long[] minimumRtt = {Long.MIN_VALUE};
        RakChannelMetrics metrics = new RakChannelMetrics() {
            @Override
            public void rakCongestionModelState(long observedAtMillis,
                                                double estimatedDeliveryRateBytesPerSecond,
                                                double pacingRateBytesPerSecond, long minimumRttMillis,
                                                double recentLossRate, long packetRound, boolean startup,
                                                boolean persistentCongestion) {
                deliveryRate[0] = estimatedDeliveryRateBytesPerSecond;
                pacingRate[0] = pacingRateBytesPerSecond;
                minimumRtt[0] = minimumRttMillis;
            }
        };

        recovery.initialize(metrics, window, 0L);
        Assertions.assertEquals(-1D, deliveryRate[0]);
        Assertions.assertTrue(pacingRate[0] > 0D, "initial pacing has a real conservative fallback rate");
        Assertions.assertEquals(-1L, minimumRtt[0]);
    }

    @Test
    public void reportsCumulativeModelLossResponseCounts() {
        RakRecoveryMetrics recovery = new RakRecoveryMetrics();
        RakSlidingWindow window = new RakSlidingWindow(1_200) {
            @Override
            public long getModelHardLossResponseCount() {
                return 3L;
            }

            @Override
            public long getModelDelayLossResponseCount() {
                return 2L;
            }
        };
        long[] hardResponses = {-1L};
        long[] delayResponses = {-1L};
        RakChannelMetrics metrics = new RakChannelMetrics() {
            @Override
            public void rakCongestionModelState(long observedAtMillis,
                                                double estimatedDeliveryRateBytesPerSecond,
                                                double pacingRateBytesPerSecond, long minimumRttMillis,
                                                double recentLossRate, long packetRound, boolean startup,
                                                boolean persistentCongestion, long hardLossResponseCount,
                                                long delayLossResponseCount) {
                hardResponses[0] = hardLossResponseCount;
                delayResponses[0] = delayLossResponseCount;
            }
        };

        recovery.initialize(metrics, window, 1_000L);

        Assertions.assertEquals(3L, hardResponses[0]);
        Assertions.assertEquals(2L, delayResponses[0]);
    }

    @Test
    public void extendedModelStateDelegatesToExistingMetricsImplementation() {
        int[] legacyCallbacks = {0};
        RakChannelMetrics legacyMetrics = new RakChannelMetrics() {
            @Override
            public void rakCongestionModelState(long observedAtMillis,
                                                double estimatedDeliveryRateBytesPerSecond,
                                                double pacingRateBytesPerSecond, long minimumRttMillis,
                                                double recentLossRate, long packetRound, boolean startup,
                                                boolean persistentCongestion) {
                legacyCallbacks[0]++;
            }
        };

        legacyMetrics.rakCongestionModelState(1_000L, 50_000D, 60_000D, 25L,
                0.05D, 7L, false, false, 3L, 2L);

        Assertions.assertEquals(1, legacyCallbacks[0]);
    }

    private static final class RecordingMetrics implements RakChannelMetrics {
        private final List<Send> sends = new ArrayList<>();
        private final List<Acknowledgement> acknowledgements = new ArrayList<>();
        private final List<RecoveryState> states = new ArrayList<>();
        private final List<Long> closedAtMillis = new ArrayList<>();

        @Override
        public void rakDatagramSent(RakDatagramSendType sendType, int bytes, int retransmissionAttempt,
                                    int bytesInFlight) {
            this.sends.add(new Send(sendType, bytes, retransmissionAttempt));
        }

        @Override
        public void rakAcknowledgementProgress(int bytes, int retransmissionAttempt, long observedAtMillis,
                                               long previousAckProgressAtMillis, long recoveryStartedAtMillis) {
            this.acknowledgements.add(new Acknowledgement(retransmissionAttempt, observedAtMillis,
                    previousAckProgressAtMillis, recoveryStartedAtMillis));
        }

        @Override
        public void rakRecoveryState(long observedAtMillis, int bytesInFlight, double congestionWindow,
                                     double slowStartThreshold, double smoothedRtt, double rttVariance,
                                     long retransmissionTimeout, int retransmittedDatagramsInFlight,
                                     long lastAckProgressAtMillis, long recoveryStartedAtMillis) {
            this.states.add(new RecoveryState(bytesInFlight, smoothedRtt, rttVariance, retransmissionTimeout,
                    retransmittedDatagramsInFlight, lastAckProgressAtMillis, recoveryStartedAtMillis));
        }

        @Override
        public void rakRecoveryStateClosed(long observedAtMillis) {
            this.closedAtMillis.add(observedAtMillis);
        }
    }

    private static final class Send {
        private final RakDatagramSendType sendType;
        private final int bytes;
        private final int attempt;

        private Send(RakDatagramSendType sendType, int bytes, int attempt) {
            this.sendType = sendType;
            this.bytes = bytes;
            this.attempt = attempt;
        }
    }

    private static final class Acknowledgement {
        private final int attempt;
        private final long observedAtMillis;
        private final long previousAckProgressAtMillis;
        private final long recoveryStartedAtMillis;

        private Acknowledgement(int attempt, long observedAtMillis, long previousAckProgressAtMillis,
                                long recoveryStartedAtMillis) {
            this.attempt = attempt;
            this.observedAtMillis = observedAtMillis;
            this.previousAckProgressAtMillis = previousAckProgressAtMillis;
            this.recoveryStartedAtMillis = recoveryStartedAtMillis;
        }
    }

    private static final class RecoveryState {
        private final int bytesInFlight;
        private final double smoothedRtt;
        private final double rttVariance;
        private final long retransmissionTimeout;
        private final int retransmittedInFlight;
        private final long lastAckProgressAtMillis;
        private final long recoveryStartedAtMillis;

        private RecoveryState(int bytesInFlight, double smoothedRtt, double rttVariance,
                              long retransmissionTimeout, int retransmittedInFlight,
                              long lastAckProgressAtMillis, long recoveryStartedAtMillis) {
            this.bytesInFlight = bytesInFlight;
            this.smoothedRtt = smoothedRtt;
            this.rttVariance = rttVariance;
            this.retransmissionTimeout = retransmissionTimeout;
            this.retransmittedInFlight = retransmittedInFlight;
            this.lastAckProgressAtMillis = lastAckProgressAtMillis;
            this.recoveryStartedAtMillis = recoveryStartedAtMillis;
        }
    }
}
