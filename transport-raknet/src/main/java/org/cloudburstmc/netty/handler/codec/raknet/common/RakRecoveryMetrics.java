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

/**
 * Tracks observation-only recovery timing. All methods are called from the RakNet session event loop.
 */
final class RakRecoveryMetrics {
    private static final long STATE_REPORT_INTERVAL_MILLIS = 100L;

    private long lastAckProgressAtMillis = -1L;
    private long recoveryStartedAtMillis = -1L;
    private long lastStateReportAtMillis = -1L;
    private int retransmittedDatagramsInFlight;

    void initialize(RakChannelMetrics metrics, RakSlidingWindow slidingWindow, long observedAtMillis) {
        this.lastAckProgressAtMillis = observedAtMillis;
        if (metrics != null) {
            this.reportState(metrics, slidingWindow, observedAtMillis, true);
        }
    }

    void onDatagramSent(RakChannelMetrics metrics, RakSlidingWindow slidingWindow, RakDatagramPacket datagram,
                        RakDatagramSendType sendType, long observedAtMillis) {
        int retransmissionAttempt = datagram.getRetransmissionCount();
        boolean recoveryStarted = false;
        if (sendType != RakDatagramSendType.ORIGINAL) {
            if (retransmissionAttempt == 0) {
                this.retransmittedDatagramsInFlight++;
                if (this.recoveryStartedAtMillis == -1L) {
                    this.recoveryStartedAtMillis = observedAtMillis;
                    recoveryStarted = true;
                }
            }
            retransmissionAttempt = datagram.markRetransmitted();
        }

        if (metrics == null) {
            return;
        }
        metrics.rakDatagramSent(sendType, datagram.getSize(), retransmissionAttempt,
                slidingWindow.getBytesInFlight());
        if (sendType != RakDatagramSendType.ORIGINAL) {
            this.reportState(metrics, slidingWindow, observedAtMillis, recoveryStarted);
        }
    }

    void onAcknowledgementProgress(RakChannelMetrics metrics, RakSlidingWindow slidingWindow,
                                   RakDatagramPacket datagram, long observedAtMillis) {
        int retransmissionAttempt = datagram.getRetransmissionCount();
        long recoveryStartedForEvent = this.recoveryStartedAtMillis;

        if (metrics != null) {
            metrics.rakAcknowledgementProgress(datagram.getSize(), retransmissionAttempt, observedAtMillis,
                    this.lastAckProgressAtMillis, recoveryStartedForEvent);
        }
        this.lastAckProgressAtMillis = observedAtMillis;

        boolean recoveryEnded = false;
        if (retransmissionAttempt > 0 && this.retransmittedDatagramsInFlight > 0) {
            this.retransmittedDatagramsInFlight--;
            if (this.retransmittedDatagramsInFlight == 0) {
                this.recoveryStartedAtMillis = -1L;
                recoveryEnded = true;
            }
        }
        if (metrics != null) {
            this.reportState(metrics, slidingWindow, observedAtMillis, recoveryEnded);
        }
    }

    SendState captureSendState() {
        return new SendState(this.recoveryStartedAtMillis, this.lastStateReportAtMillis,
                this.retransmittedDatagramsInFlight);
    }

    void restoreSendState(SendState state) {
        this.recoveryStartedAtMillis = state.recoveryStartedAtMillis;
        this.lastStateReportAtMillis = state.lastStateReportAtMillis;
        this.retransmittedDatagramsInFlight = state.retransmittedDatagramsInFlight;
    }

    void reportState(RakChannelMetrics metrics, RakSlidingWindow slidingWindow, long observedAtMillis) {
        this.reportState(metrics, slidingWindow, observedAtMillis, false);
    }

    void close(RakChannelMetrics metrics, RakSlidingWindow slidingWindow, long observedAtMillis) {
        this.retransmittedDatagramsInFlight = 0;
        this.recoveryStartedAtMillis = -1L;
        this.lastStateReportAtMillis = observedAtMillis;
        try {
            metrics.rakRecoveryState(observedAtMillis, 0, slidingWindow.getCongestionWindow(),
                    slidingWindow.getSlowStartThreshold(), slidingWindow.getRTT(), slidingWindow.getRttDeviation(),
                    slidingWindow.getRtoForRetransmission(), 0, this.lastAckProgressAtMillis, -1L);
            this.reportModelState(metrics, slidingWindow, observedAtMillis);
        } finally {
            metrics.rakRecoveryStateClosed(observedAtMillis);
        }
    }

    private void reportState(RakChannelMetrics metrics, RakSlidingWindow slidingWindow, long observedAtMillis,
                             boolean force) {
        if (!force && this.lastStateReportAtMillis != -1L) {
            long elapsed = observedAtMillis - this.lastStateReportAtMillis;
            if (elapsed >= 0L && elapsed < STATE_REPORT_INTERVAL_MILLIS) {
                return;
            }
        }
        this.lastStateReportAtMillis = observedAtMillis;
        metrics.rakRecoveryState(observedAtMillis, slidingWindow.getBytesInFlight(),
                slidingWindow.getCongestionWindow(), slidingWindow.getSlowStartThreshold(), slidingWindow.getRTT(),
                slidingWindow.getRttDeviation(), slidingWindow.getRtoForRetransmission(),
                this.retransmittedDatagramsInFlight, this.lastAckProgressAtMillis,
                this.recoveryStartedAtMillis);
        this.reportModelState(metrics, slidingWindow, observedAtMillis);
    }

    private void reportModelState(RakChannelMetrics metrics, RakSlidingWindow slidingWindow,
                                  long observedAtMillis) {
        double bandwidth = slidingWindow.getModelBandwidthBytesPerMillis();
        double pacingRate = slidingWindow.getModelPacingRateBytesPerMillis();
        if (pacingRate < 0D) {
            return;
        }
        metrics.rakCongestionModelState(observedAtMillis,
                bandwidth < 0D ? -1D : bandwidth * 1_000D,
                pacingRate < 0D ? -1D : pacingRate * 1_000D,
                slidingWindow.getModelMinimumRttMillis(), slidingWindow.getModelRecentLossRate(),
                slidingWindow.getModelRoundCount(), slidingWindow.isModelStartup(),
                slidingWindow.isModelPersistentCongestion(), slidingWindow.getModelHardLossResponseCount(),
                slidingWindow.getModelDelayLossResponseCount());
    }

    static final class SendState {
        private final long recoveryStartedAtMillis;
        private final long lastStateReportAtMillis;
        private final int retransmittedDatagramsInFlight;

        private SendState(long recoveryStartedAtMillis, long lastStateReportAtMillis,
                          int retransmittedDatagramsInFlight) {
            this.recoveryStartedAtMillis = recoveryStartedAtMillis;
            this.lastStateReportAtMillis = lastStateReportAtMillis;
            this.retransmittedDatagramsInFlight = retransmittedDatagramsInFlight;
        }
    }
}
