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

import org.cloudburstmc.netty.channel.raknet.packet.RakDatagramPacket;

/**
 * Event-loop-confined prototype of a delivery-model congestion controller.
 *
 * <p>This is deliberately not named BBR: it implements only the minimum RakNet integration needed to evaluate
 * per-attempt delivery-rate sampling, packet-timed bandwidth filtering, BDP-based in-flight control, and bounded
 * pacing. It does not claim the full state machine or validation envelope of the IETF BBR draft.</p>
 */
final class RakModelCongestionController {
    private static final int BANDWIDTH_FILTER_ROUNDS = 10;
    private static final long INITIAL_RTT_MILLIS = 333L;
    private static final double STARTUP_PACING_GAIN = 2.77D;
    private static final double PROBE_UP_PACING_GAIN = 1.25D;
    private static final double PROBE_DOWN_PACING_GAIN = 0.75D;
    private static final double CWND_GAIN = 2.0D;
    private static final double FULL_BANDWIDTH_GROWTH = 1.25D;
    private static final int FULL_BANDWIDTH_ROUNDS = 3;
    private static final double LOSS_THRESHOLD = 0.02D;
    private static final double HARD_LOSS_THRESHOLD = 0.20D;
    private static final double DELAY_INFLATION_THRESHOLD = 1.25D;
    private static final double HARD_LOSS_BETA = 0.70D;
    private static final double DELAY_LOSS_BETA = 0.90D;
    private static final int MAX_BURST_DATAGRAMS = 8;
    private static final long MINIMUM_RTT_WINDOW_MILLIS = 10_000L;
    private static final double PATH_STEP_MULTIPLIER = 2.5D;
    private static final long PATH_STEP_ABSOLUTE_DELTA_MILLIS = 8L;
    private static final double LOW_FLIGHT_PATH_STEP_MULTIPLIER = 1.5D;
    private static final long LOW_FLIGHT_PATH_STEP_ABSOLUTE_DELTA_MILLIS = 4L;
    private static final int PATH_SUSPICION_CONFIRMATION_SAMPLES = 2;
    private static final int PATH_STEP_CONFIRMATION_SAMPLES = 2;
    private static final double PATH_STEP_STABILITY_MULTIPLIER = 1.25D;
    private static final long PATH_STEP_STABILITY_ABSOLUTE_DELTA_MILLIS = 4L;
    private static final long PATH_TIMEOUT_RTT_MULTIPLIER = 10L;
    private static final long PATH_TIMEOUT_MIN_MILLIS = 2_000L;
    private static final long PATH_TIMEOUT_MAX_MILLIS = 3_000L;
    private static final long PATH_DRAIN_HOLD_MIN_MILLIS = 50L;
    private static final long PATH_DRAIN_HOLD_MAX_MILLIS = 500L;
    private static final long PATH_COOLDOWN_RTT_MULTIPLIER = 2L;
    private static final long PATH_COOLDOWN_MIN_MILLIS = 250L;
    private static final long PATH_COOLDOWN_MAX_MILLIS = 1_000L;
    private static final int PATH_MAX_ATTEMPTS = 3;
    private static final int STARTUP_MINIMUM_ROUND_MTUS = 4;
    private static final int LOSS_BUCKET_MINIMUM_PACKETS = 64;
    private static final int DELAY_LOSS_MINIMUM_PACKETS = 128;
    private static final int DELAY_CLEAR_BUCKET_MINIMUM_PACKETS = 256;
    private static final int HARD_LOSS_MINIMUM_LOST_PACKETS = 8;
    private static final int DELAY_LOSS_MINIMUM_LOST_PACKETS = 4;
    private static final int LOSS_CLEAR_BUCKETS_TO_RELEASE = 2;

    private final int mtu;
    private final long sendQuantumMillis;
    private final double minimumCwnd;
    private final double initialCwnd;
    private final double maximumCwnd;
    private final double[] bandwidthFilter = new double[BANDWIDTH_FILTER_ROUNDS];

    private double cwnd;
    private double maxBandwidthBytesPerMillis;
    private double fullBandwidthBytesPerMillis;
    private int fullBandwidthRounds;
    private boolean startup = true;
    private boolean persistentCongestion;
    private double inflightLimit = Double.POSITIVE_INFINITY;
    private LossState lossState = LossState.ARMED;
    private LossHoldKind lossHoldKind = LossHoldKind.NONE;
    private int lossClearBuckets;
    private long lossClearPackets;
    private long lossResponseCount;
    private long hardLossResponseCount;
    private long delayLossResponseCount;
    private long lossBucketDeliveredPackets;
    private long lossBucketLostPackets;
    private long lossBucketDeliveredBytes;
    private long lossBucketLostBytes;
    private int lossBucketPeakInFlight;
    private double lossBucketMaximumCwnd;
    private long delayLossBucketDeliveredPackets;
    private long delayLossBucketLostPackets;
    private long delayLossBucketDeliveredBytes;
    private long delayLossBucketLostBytes;
    private int delayLossBucketPeakInFlight;
    private double delayLossBucketMaximumCwnd;
    private double delayLossBucketMaximumRttRatio = -1D;
    private boolean delayLossBucketEvidenceActionable = true;
    private double lossRecoveryCwnd;

    private long deliveredBytes;
    private long deliveredTimeMillis;
    private long firstSendTimeMillis;
    private long nextRoundDelivered;
    private long roundCount;
    private int bandwidthFilterIndex;
    private long roundDeliveredPackets;
    private long roundLostPackets;
    private long roundDeliveredBytes;
    private long roundLostBytes;
    private int roundMaxInFlight;
    private long delayRoundDeliveredPackets;
    private long delayRoundLostPackets;
    private long delayRoundDeliveredBytes;
    private long delayRoundLostBytes;
    private int delayRoundMaxInFlight;
    private double delayRoundMaximumRttRatio = -1D;
    private boolean delayRoundEvidenceActionable = true;
    private double recentLossRate = -1D;

    private long minimumRttMillis = Long.MAX_VALUE;
    private long minimumRttTimestampMillis = -1L;
    private PathState pathState = PathState.STEADY;
    private long pathSuspicionMinimumRttMillis = Long.MAX_VALUE;
    private long pathSuspicionMaximumRttMillis = -1L;
    private long pathSuspicionDeliveredAtSend = -1L;
    private long pathSuspicionObservedAtMillis = -1L;
    private int pathSuspicionSamples;
    private long pathSuspectRttMillis = -1L;
    private long pathProbeDeadlineMillis = -1L;
    private long pathCooldownUntilMillis = -1L;
    private long pathProbeBoundaryDelivered = -1L;
    private long pathLowFlightSinceMillis = -1L;
    private long pathDrainHoldMillis;
    private double pathPreProbeCwnd;
    private int pathAttempts;
    private boolean pathLossSuppressionExhausted;
    private long pathStepMinimumRttMillis = Long.MAX_VALUE;
    private long pathStepMaximumRttMillis = -1L;
    private long pathStepDeliveredAtSend = -1L;
    private long pathStepObservedAtMillis = -1L;
    private int pathStepSamples;
    private boolean pathProbeUsesLowFlightEnvelope;
    private boolean minimumRttUsesLowFlightEnvelope;

    private double pacingTokens;
    private long pacingUpdatedAtMillis = -1L;
    private boolean continuouslyBacklogged;

    RakModelCongestionController(int mtu) {
        this(mtu, 10L);
    }

    RakModelCongestionController(int mtu, long sendQuantumMillis) {
        this.mtu = mtu;
        this.sendQuantumMillis = Math.max(1L, sendQuantumMillis);
        // RFC 9002's initial window formula: min(10*MDS, max(2*MDS, 14720)).
        this.cwnd = Math.min(10D * mtu, Math.max(2D * mtu, 14_720D));
        this.initialCwnd = this.cwnd;
        this.minimumCwnd = 2D * mtu;
        // A finite implementation safety bound; normal control should remain at the measured BDP well below it.
        this.maximumCwnd = 4D * 1024D * 1024D;
    }

    int transmissionAllowance(long nowMillis, int bytesInFlight) {
        this.advancePathState(nowMillis);
        this.refillPacingTokens(nowMillis);
        int windowAllowance = Math.max(0, (int) (this.cwnd - bytesInFlight));
        return Math.min(windowAllowance, Math.max(0, (int) this.pacingTokens));
    }

    boolean canSend(long nowMillis, int bytesInFlight, int size) {
        return size <= this.transmissionAllowance(nowMillis, bytesInFlight);
    }

    void onPacketSent(RakDatagramPacket datagram, long nowMillis, int bytesInFlight, boolean appLimited) {
        this.refillPacingTokens(nowMillis);
        this.pacingTokens = Math.max(0D, this.pacingTokens - datagram.getSize());
        this.continuouslyBacklogged = !appLimited;

        if (bytesInFlight <= datagram.getSize()) {
            this.firstSendTimeMillis = nowMillis;
            this.deliveredTimeMillis = nowMillis;
        }
        datagram.setModelSendState(this.deliveredBytes, this.deliveredTimeMillis, this.firstSendTimeMillis,
                nowMillis, bytesInFlight, appLimited);
    }

    void onUnreliablePacketSent(int size, long nowMillis) {
        this.onUnreliablePacketSent(size, nowMillis, size, false);
    }

    DatagramSample onUnreliablePacketSent(int size, long nowMillis, int bytesInFlight, boolean appLimited) {
        this.refillPacingTokens(nowMillis);
        this.pacingTokens = Math.max(0D, this.pacingTokens - size);
        this.continuouslyBacklogged = !appLimited;
        if (bytesInFlight <= size) {
            this.firstSendTimeMillis = nowMillis;
            this.deliveredTimeMillis = nowMillis;
        }
        return new DatagramSample(size, this.deliveredBytes, this.deliveredTimeMillis, this.firstSendTimeMillis,
                nowMillis, bytesInFlight, appLimited);
    }

    UnreliableSendState captureUnreliableSendState() {
        return new UnreliableSendState(this.pacingTokens, this.pacingUpdatedAtMillis, this.firstSendTimeMillis,
                this.deliveredTimeMillis, this.continuouslyBacklogged);
    }

    void restoreUnreliableSendState(UnreliableSendState state) {
        this.pacingTokens = state.pacingTokens;
        this.pacingUpdatedAtMillis = state.pacingUpdatedAtMillis;
        this.firstSendTimeMillis = state.firstSendTimeMillis;
        this.deliveredTimeMillis = state.deliveredTimeMillis;
        this.continuouslyBacklogged = state.continuouslyBacklogged;
    }

    void onAcknowledged(RakDatagramPacket datagram, long nowMillis, long rttSampleMillis,
                        double smoothedRttMillis, int currentBytesInFlight) {
        if (!datagram.isModelSampleValid()) {
            return;
        }
        this.onAcknowledged(datagram.getSize(), datagram.getDeliveredBytesAtSend(),
                datagram.getDeliveredTimeAtSend(), datagram.getFirstSendTime(), datagram.getModelSendTime(),
                datagram.getModelTxInFlight(), datagram.isModelAppLimited(), datagram.getRetransmissionCount() > 0,
                nowMillis, rttSampleMillis, smoothedRttMillis, currentBytesInFlight);
        datagram.clearModelSendState();
    }

    void onUnreliableAcknowledged(DatagramSample sample, long nowMillis, long rttSampleMillis,
                                  double smoothedRttMillis, int currentBytesInFlight) {
        this.onAcknowledged(sample.size, sample.deliveredBytesAtSend, sample.deliveredTimeAtSend,
                sample.firstSendTime, sample.sendTime, sample.txInFlight, sample.appLimited, false,
                nowMillis, rttSampleMillis, smoothedRttMillis, currentBytesInFlight);
    }

    private void onAcknowledged(int acknowledgedBytes, long priorDelivered, long deliveredTimeAtSend,
                                long packetFirstSendTime, long modelSendTime, int txInFlight, boolean appLimited,
                                boolean ambiguousRetransmission, long nowMillis, long rttSampleMillis,
                                double smoothedRttMillis, int currentBytesInFlight) {
        if (this.persistentCongestion) {
            // Persistent congestion discards the old path model. The first new acknowledgement starts a fresh
            // startup, rather than leaving the minimum window as a permanent cap on the replacement model.
            this.inflightLimit = Double.POSITIVE_INFINITY;
            this.persistentCongestion = false;
        }

        if (rttSampleMillis >= 0L) {
            this.updateMinimumRtt(nowMillis, Math.max(1L, rttSampleMillis), txInFlight,
                    currentBytesInFlight, priorDelivered, modelSendTime, appLimited);
        }
        long ackElapsed = nowMillis - deliveredTimeAtSend;
        long sendElapsed = modelSendTime - packetFirstSendTime;
        this.deliveredBytes += acknowledgedBytes;
        this.deliveredTimeMillis = nowMillis;

        boolean newRound = priorDelivered >= this.nextRoundDelivered;
        long completedRoundDeliveredBytes = 0L;
        if (newRound) {
            completedRoundDeliveredBytes = this.roundDeliveredBytes;
            this.finishRound();
            this.roundCount++;
            this.bandwidthFilterIndex = (this.bandwidthFilterIndex + 1) % BANDWIDTH_FILTER_ROUNDS;
            this.bandwidthFilter[this.bandwidthFilterIndex] = 0D;
            this.nextRoundDelivered = this.deliveredBytes;
        }

        this.roundDeliveredPackets = saturatingAdd(this.roundDeliveredPackets, 1L);
        this.roundDeliveredBytes = saturatingAdd(this.roundDeliveredBytes, acknowledgedBytes);
        this.roundMaxInFlight = Math.max(this.roundMaxInFlight, txInFlight);
        this.delayRoundDeliveredPackets = saturatingAdd(this.delayRoundDeliveredPackets, 1L);
        this.delayRoundDeliveredBytes = saturatingAdd(this.delayRoundDeliveredBytes, acknowledgedBytes);
        this.delayRoundMaxInFlight = Math.max(this.delayRoundMaxInFlight, txInFlight);
        this.delayRoundEvidenceActionable &= !this.startup && this.isPathDelayEvidenceActionable();

        long interval = Math.max(ackElapsed, sendElapsed);
        if (!ambiguousRetransmission && interval > 0L && this.minimumRttMillis != Long.MAX_VALUE
                && interval >= this.minimumRttMillis) {
            long delivered = this.deliveredBytes - priorDelivered;
            double sample = (double) delivered / interval;
            if (Double.isFinite(sample) && sample > 0D) {
                if (!appLimited || sample >= this.maxBandwidthBytesPerMillis) {
                    this.bandwidthFilter[this.bandwidthFilterIndex] = Math.max(
                            this.bandwidthFilter[this.bandwidthFilterIndex], sample);
                    this.updateMaximumBandwidth();
                }
            }
        }

        this.updateFullBandwidth(newRound, completedRoundDeliveredBytes);
        this.updateCongestionWindow(acknowledgedBytes);
        this.firstSendTimeMillis = modelSendTime;
    }

    void onLost(RakDatagramPacket datagram, double smoothedRttMillis) {
        this.onLost(datagram.getSize(), datagram.getModelTxInFlight(), smoothedRttMillis);
    }

    void onUnreliableLost(DatagramSample sample, double smoothedRttMillis) {
        this.onLost(sample.size, sample.txInFlight, smoothedRttMillis);
    }

    private void onLost(int size, int txInFlight, double smoothedRttMillis) {
        if (this.roundLostPackets == 0L && this.lossBucketLostPackets == 0L) {
            // Preserve the last window before ACK/loss processing can lower the estimator during this episode.
            this.lossBucketMaximumCwnd = Math.max(this.lossBucketMaximumCwnd, this.cwnd);
        }
        if (this.delayRoundLostPackets == 0L && this.delayLossBucketLostPackets == 0L) {
            this.delayLossBucketMaximumCwnd = Math.max(this.delayLossBucketMaximumCwnd, this.cwnd);
        }
        this.roundLostPackets = saturatingAdd(this.roundLostPackets, 1L);
        this.roundLostBytes = saturatingAdd(this.roundLostBytes, size);
        this.roundMaxInFlight = Math.max(this.roundMaxInFlight, txInFlight);
        this.delayRoundLostPackets = saturatingAdd(this.delayRoundLostPackets, 1L);
        this.delayRoundLostBytes = saturatingAdd(this.delayRoundLostBytes, size);
        this.delayRoundMaxInFlight = Math.max(this.delayRoundMaxInFlight, txInFlight);
        if (smoothedRttMillis >= 0D && this.minimumRttMillis != Long.MAX_VALUE) {
            this.delayRoundMaximumRttRatio = Math.max(
                    this.delayRoundMaximumRttRatio, smoothedRttMillis / this.minimumRttMillis);
        }
        this.delayRoundEvidenceActionable &= !this.startup && this.isPathDelayEvidenceActionable();
    }

    void onPersistentCongestion() {
        this.cwnd = this.minimumCwnd;
        this.startup = true;
        this.fullBandwidthBytesPerMillis = 0D;
        this.fullBandwidthRounds = 0;
        this.maxBandwidthBytesPerMillis = 0D;
        for (int i = 0; i < this.bandwidthFilter.length; i++) {
            this.bandwidthFilter[i] = 0D;
        }
        this.bandwidthFilterIndex = 0;
        this.inflightLimit = this.minimumCwnd;
        this.lossState = LossState.ARMED;
        this.lossHoldKind = LossHoldKind.NONE;
        this.lossClearBuckets = 0;
        this.lossClearPackets = 0L;
        this.lossRecoveryCwnd = 0D;
        this.resetLossBuckets();
        this.resetRoundLossEvidence();
        this.recentLossRate = -1D;
        this.pacingTokens = 0D;
        this.resetPathTransition();
        this.persistentCongestion = true;
    }

    private void finishRound() {
        long roundTotalBytes = saturatingAdd(this.roundDeliveredBytes, this.roundLostBytes);
        if (roundTotalBytes > 0L) {
            this.recentLossRate = (double) this.roundLostBytes / roundTotalBytes;
            this.lossBucketDeliveredPackets = saturatingAdd(
                    this.lossBucketDeliveredPackets, this.roundDeliveredPackets);
            this.lossBucketLostPackets = saturatingAdd(
                    this.lossBucketLostPackets, this.roundLostPackets);
            this.lossBucketDeliveredBytes = saturatingAdd(
                    this.lossBucketDeliveredBytes, this.roundDeliveredBytes);
            this.lossBucketLostBytes = saturatingAdd(
                    this.lossBucketLostBytes, this.roundLostBytes);
            this.lossBucketPeakInFlight = Math.max(this.lossBucketPeakInFlight, this.roundMaxInFlight);
            this.lossBucketMaximumCwnd = Math.max(this.lossBucketMaximumCwnd, this.cwnd);
            this.delayLossBucketDeliveredPackets = saturatingAdd(
                    this.delayLossBucketDeliveredPackets, this.delayRoundDeliveredPackets);
            this.delayLossBucketLostPackets = saturatingAdd(
                    this.delayLossBucketLostPackets, this.delayRoundLostPackets);
            this.delayLossBucketDeliveredBytes = saturatingAdd(
                    this.delayLossBucketDeliveredBytes, this.delayRoundDeliveredBytes);
            this.delayLossBucketLostBytes = saturatingAdd(
                    this.delayLossBucketLostBytes, this.delayRoundLostBytes);
            this.delayLossBucketPeakInFlight = Math.max(
                    this.delayLossBucketPeakInFlight, this.delayRoundMaxInFlight);
            this.delayLossBucketMaximumCwnd = Math.max(this.delayLossBucketMaximumCwnd, this.cwnd);
            this.delayLossBucketMaximumRttRatio = Math.max(
                    this.delayLossBucketMaximumRttRatio, this.delayRoundMaximumRttRatio);
            this.delayLossBucketEvidenceActionable &= this.delayRoundEvidenceActionable;
            this.evaluateLossBucket();
            this.updateDelayHoldClearProgress();
        }
        this.resetRoundLossEvidence();
    }

    private void evaluateLossBucket() {
        long hardTotalPackets = saturatingAdd(this.lossBucketDeliveredPackets, this.lossBucketLostPackets);
        long hardTotalBytes = saturatingAdd(this.lossBucketDeliveredBytes, this.lossBucketLostBytes);
        long delayTotalPackets = saturatingAdd(
                this.delayLossBucketDeliveredPackets, this.delayLossBucketLostPackets);
        long delayTotalBytes = saturatingAdd(this.delayLossBucketDeliveredBytes, this.delayLossBucketLostBytes);
        if ((hardTotalPackets == 0L || hardTotalBytes == 0L)
                && (delayTotalPackets == 0L || delayTotalBytes == 0L)) {
            return;
        }

        double hardPacketLossRate = hardTotalPackets == 0L
                ? 0D : (double) this.lossBucketLostPackets / hardTotalPackets;
        double hardByteLossRate = hardTotalBytes == 0L
                ? 0D : (double) this.lossBucketLostBytes / hardTotalBytes;
        double hardControlLossRate = Math.max(hardPacketLossRate, hardByteLossRate);
        boolean hardSignal = this.lossBucketLostPackets >= HARD_LOSS_MINIMUM_LOST_PACKETS
                && hardControlLossRate >= HARD_LOSS_THRESHOLD;

        double delayPacketLossRate = delayTotalPackets == 0L
                ? 0D : (double) this.delayLossBucketLostPackets / delayTotalPackets;
        double delayByteLossRate = delayTotalBytes == 0L
                ? 0D : (double) this.delayLossBucketLostBytes / delayTotalBytes;
        double delayControlLossRate = Math.max(delayPacketLossRate, delayByteLossRate);
        boolean stablePathForDelayEvidence = this.isPathDelayEvidenceActionable();
        boolean actionableDelayEvidence = this.delayLossBucketEvidenceActionable
                && !this.startup && stablePathForDelayEvidence;
        boolean inflatedDelay = this.delayLossBucketMaximumRttRatio >= DELAY_INFLATION_THRESHOLD;
        boolean delayPressure = delayTotalPackets >= DELAY_LOSS_MINIMUM_PACKETS
                && this.delayLossBucketLostPackets >= DELAY_LOSS_MINIMUM_LOST_PACKETS
                && delayControlLossRate > LOSS_THRESHOLD && inflatedDelay;
        boolean delaySignal = delayPressure && actionableDelayEvidence;
        boolean congestionSignal = hardSignal || delaySignal;

        if (congestionSignal) {
            if (this.lossState == LossState.ARMED) {
                int signalPeakInFlight = hardSignal
                        ? this.lossBucketPeakInFlight : this.delayLossBucketPeakInFlight;
                double signalMaximumCwnd = hardSignal
                        ? this.lossBucketMaximumCwnd : this.delayLossBucketMaximumCwnd;
                double peakBound = signalPeakInFlight > 0
                        ? Math.min(this.cwnd, signalPeakInFlight) : this.cwnd;
                this.lossRecoveryCwnd = Math.max(peakBound, signalMaximumCwnd);
                double lossBeta = hardSignal ? HARD_LOSS_BETA : DELAY_LOSS_BETA;
                double responseBasis = peakBound;
                if (!hardSignal || this.startup
                        || this.pathState == PathState.DRAIN || this.pathState == PathState.SAMPLE) {
                    // A discovery flight can be held at two MTUs by the model itself. Turning that artificial
                    // flight into a finite loss cap prevents the packet-timed startup rounds needed to discover
                    // the replacement path. A DELAY response has the same problem after the short delivery filter
                    // self-clocks below its useful range: its long-lived HOLD would make that accidental small
                    // flight permanent. Keep the ordinary beta response, but apply these cases to at least the
                    // standards-sized initial window. HARD loss outside discovery keeps its smaller observed basis.
                    responseBasis = Math.max(responseBasis, this.initialCwnd);
                }
                this.cwnd = Math.max(this.minimumCwnd, responseBasis * lossBeta);
                this.inflightLimit = this.cwnd;
                if (hardSignal) {
                    this.startup = false;
                }
                this.lossState = LossState.HOLD;
                this.lossHoldKind = hardSignal ? LossHoldKind.HARD : LossHoldKind.DELAY;
                this.lossResponseCount = saturatingAdd(this.lossResponseCount, 1L);
                if (hardSignal) {
                    this.hardLossResponseCount = saturatingAdd(this.hardLossResponseCount, 1L);
                } else {
                    this.delayLossResponseCount = saturatingAdd(this.delayLossResponseCount, 1L);
                }
            }
            if (!hardSignal && delaySignal) {
                // Once an epoch includes delay-qualified moderate loss, loss above the normal threshold must
                // actually clear before fluctuating RTT can rearm another response.
                this.lossHoldKind = LossHoldKind.DELAY;
            }
            this.lossClearBuckets = 0;
            this.lossClearPackets = 0L;
            this.resetLossBuckets();
            return;
        }

        boolean continuingDelayEpoch = this.lossState == LossState.HOLD
                && this.lossHoldKind == LossHoldKind.DELAY
                && delayTotalPackets >= DELAY_LOSS_MINIMUM_PACKETS
                && delayControlLossRate > LOSS_THRESHOLD;
        boolean completeClearBucket = delayTotalPackets >= DELAY_LOSS_MINIMUM_PACKETS
                || (delayTotalPackets >= LOSS_BUCKET_MINIMUM_PACKETS
                && this.delayLossBucketLostPackets == 0L);
        if (completeClearBucket && !actionableDelayEvidence) {
            // Suppressed/startup evidence cannot either cut or release a held loss epoch.
            this.lossClearBuckets = 0;
            this.lossClearPackets = 0L;
            this.resetDelayLossBucket();
        } else if (delayPressure || continuingDelayEpoch) {
            // Startup/path validation owns suppressed pressure; continuing loss also cannot clear a DELAY hold.
            // Neither case is evidence that the active episode ended.
            this.lossClearBuckets = 0;
            this.lossClearPackets = 0L;
            this.resetDelayLossBucket();
        } else if (this.lossState == LossState.HOLD && this.lossHoldKind == LossHoldKind.DELAY) {
            // A stationary path close to the two-percent DELAY threshold naturally produces alternating
            // above/below-threshold 128-packet samples. Treating the lower samples as clear repeatedly splits one
            // continuing loss episode. A DELAY hold therefore rearms only after two disjoint 256-packet windows
            // with no validated loss at all. Any loss resets clear progress; smaller samples remain accumulated.
            if (delayTotalPackets >= DELAY_CLEAR_BUCKET_MINIMUM_PACKETS) {
                this.resetDelayLossBucket();
            }
        } else {
            if (completeClearBucket) {
                if (this.lossState == LossState.HOLD
                        && ++this.lossClearBuckets >= LOSS_CLEAR_BUCKETS_TO_RELEASE) {
                    this.releaseLossHold();
                }
                this.resetDelayLossBucket();
            }
        }

        boolean completeHardBucket = hardTotalPackets >= DELAY_LOSS_MINIMUM_PACKETS
                || (hardTotalPackets >= LOSS_BUCKET_MINIMUM_PACKETS && this.lossBucketLostPackets == 0L);
        if (completeHardBucket) {
            this.resetHardLossBucket();
        }
    }

    private void updateDelayHoldClearProgress() {
        if (this.lossState != LossState.HOLD || this.lossHoldKind != LossHoldKind.DELAY) {
            return;
        }
        if (!this.delayRoundEvidenceActionable || this.delayRoundLostPackets > 0L) {
            this.lossClearBuckets = 0;
            this.lossClearPackets = 0L;
            return;
        }
        this.lossClearPackets = saturatingAdd(this.lossClearPackets, this.delayRoundDeliveredPackets);
        while (this.lossClearPackets >= DELAY_CLEAR_BUCKET_MINIMUM_PACKETS) {
            this.lossClearPackets -= DELAY_CLEAR_BUCKET_MINIMUM_PACKETS;
            if (++this.lossClearBuckets >= LOSS_CLEAR_BUCKETS_TO_RELEASE) {
                this.releaseLossHold();
                return;
            }
        }
    }

    private void releaseLossHold() {
        this.inflightLimit = Double.POSITIVE_INFINITY;
        this.lossState = LossState.ARMED;
        this.lossHoldKind = LossHoldKind.NONE;
        this.lossClearBuckets = 0;
        this.lossClearPackets = 0L;
        // The finite cap can leave the bandwidth filter self-consistently below the path's capacity. Restore only
        // the window observed immediately before the cut, then re-enter bounded startup so clean delivery can
        // promptly rediscover the useful pre-loss rate.
        this.cwnd = Math.max(this.cwnd, Math.min(this.maximumCwnd, this.lossRecoveryCwnd));
        this.lossRecoveryCwnd = 0D;
        this.startup = true;
        this.fullBandwidthBytesPerMillis = 0D;
        this.fullBandwidthRounds = 0;
    }

    private void resetLossBuckets() {
        this.resetHardLossBucket();
        this.resetDelayLossBucket();
    }

    private void resetHardLossBucket() {
        this.lossBucketDeliveredPackets = 0L;
        this.lossBucketLostPackets = 0L;
        this.lossBucketDeliveredBytes = 0L;
        this.lossBucketLostBytes = 0L;
        this.lossBucketPeakInFlight = 0;
        this.lossBucketMaximumCwnd = 0D;
    }

    private void resetDelayLossBucket() {
        this.delayLossBucketDeliveredPackets = 0L;
        this.delayLossBucketLostPackets = 0L;
        this.delayLossBucketDeliveredBytes = 0L;
        this.delayLossBucketLostBytes = 0L;
        this.delayLossBucketPeakInFlight = 0;
        this.delayLossBucketMaximumCwnd = 0D;
        this.delayLossBucketMaximumRttRatio = -1D;
        this.delayLossBucketEvidenceActionable = true;
    }

    private void resetRoundLossEvidence() {
        this.resetHardRoundLossEvidence();
        this.resetDelayRoundLossEvidence();
    }

    private void resetHardRoundLossEvidence() {
        this.roundDeliveredPackets = 0L;
        this.roundLostPackets = 0L;
        this.roundDeliveredBytes = 0L;
        this.roundLostBytes = 0L;
        this.roundMaxInFlight = 0;
    }

    private void resetDelayRoundLossEvidence() {
        this.delayRoundDeliveredPackets = 0L;
        this.delayRoundLostPackets = 0L;
        this.delayRoundDeliveredBytes = 0L;
        this.delayRoundLostBytes = 0L;
        this.delayRoundMaxInFlight = 0;
        this.delayRoundMaximumRttRatio = -1D;
        this.delayRoundEvidenceActionable = true;
    }

    private void updateMaximumBandwidth() {
        double maximum = 0D;
        for (double sample : this.bandwidthFilter) {
            maximum = Math.max(maximum, sample);
        }
        this.maxBandwidthBytesPerMillis = maximum;
    }

    private void updateMinimumRtt(long nowMillis, long rttSampleMillis, int txInFlight,
                                  int currentBytesInFlight, long deliveredAtSend, long sendTimeMillis,
                                  boolean appLimited) {
        this.advancePathState(nowMillis);
        if (this.pathAttempts > 0 && deliveredAtSend <= this.pathProbeBoundaryDelivered) {
            // Keep the completed-attempt boundary through cooldown and retry. Otherwise an ACK that arrives at
            // the timeout instant can advance the state first, then erase that attempt using stale RTT evidence.
            return;
        }
        boolean probing = this.pathState == PathState.DRAIN || this.pathState == PathState.SAMPLE;
        boolean lowFlight = txInFlight <= this.minimumCwnd && currentBytesInFlight <= this.minimumCwnd;
        boolean idleLowFlight = lowFlight && appLimited;
        boolean lowFlightRefresh = this.minimumRttTimestampMillis >= 0L
                && nowMillis - this.minimumRttTimestampMillis >= MINIMUM_RTT_WINDOW_MILLIS
                && lowFlight;
        if (this.minimumRttMillis == Long.MAX_VALUE || rttSampleMillis < this.minimumRttMillis) {
            boolean materialLowerPath = this.minimumRttMillis != Long.MAX_VALUE
                    && this.minimumRttMillis >= pathStepThreshold(rttSampleMillis,
                    this.minimumRttUsesLowFlightEnvelope);
            if (probing) {
                this.restorePathProbeWindow();
            }
            if (materialLowerPath) {
                this.resetPathScopedLossEvidence();
                this.minimumRttUsesLowFlightEnvelope = false;
            }
            this.minimumRttMillis = rttSampleMillis;
            this.minimumRttTimestampMillis = nowMillis;
            this.resetPathTransition();
            return;
        }

        long pathStepThreshold = pathStepThreshold(this.minimumRttMillis, idleLowFlight);
        if (rttSampleMillis < pathStepThreshold) {
            if (probing) {
                this.restorePathProbeWindow();
            }
            if (lowFlightRefresh) {
                this.minimumRttMillis = rttSampleMillis;
                this.minimumRttTimestampMillis = nowMillis;
            }
            if (probing) {
                // A return to the known RTT rejects this drained candidate. Rate-limit another deliberate drain;
                // otherwise ordinary queue oscillation can hold a healthy shallow path at two MTUs indefinitely.
                this.enterRejectedPathCooldown(
                        nowMillis, Math.max(rttSampleMillis, this.pathSuspectRttMillis));
                return;
            }
            if (this.pathState == PathState.COOLDOWN || this.pathState == PathState.REJECTED_COOLDOWN) {
                return;
            }
            this.resetPathTransition(true);
            return;
        }

        if (this.pathState == PathState.COOLDOWN || this.pathState == PathState.REJECTED_COOLDOWN) {
            return;
        }
        if (this.pathState == PathState.STEADY) {
            this.beginPathSuspicion(nowMillis, rttSampleMillis, deliveredAtSend, idleLowFlight);
            return;
        }
        if (this.pathState == PathState.SUSPECT) {
            if (!this.observePathSuspicion(nowMillis, rttSampleMillis, deliveredAtSend, idleLowFlight)) {
                this.enterPathCooldown(nowMillis, rttSampleMillis, false);
                return;
            }
            if (this.pathSuspicionSamples >= PATH_SUSPICION_CONFIRMATION_SAMPLES) {
                this.beginPathDrain(nowMillis);
            }
            return;
        }

        this.observePathCandidate(nowMillis, rttSampleMillis, txInFlight, currentBytesInFlight,
                deliveredAtSend, sendTimeMillis, appLimited);
    }

    private void beginPathSuspicion(long nowMillis, long rttSampleMillis, long deliveredAtSend,
                                    boolean lowFlight) {
        this.invalidateLossEvidenceForPathTransition();
        this.pathState = PathState.SUSPECT;
        this.pathProbeUsesLowFlightEnvelope = lowFlight;
        this.pathSuspicionMinimumRttMillis = rttSampleMillis;
        this.pathSuspicionMaximumRttMillis = rttSampleMillis;
        this.pathSuspicionDeliveredAtSend = deliveredAtSend;
        this.pathSuspicionObservedAtMillis = nowMillis;
        this.pathSuspicionSamples = 1;
        this.pathSuspectRttMillis = rttSampleMillis;
        this.pathProbeDeadlineMillis = saturatingAdd(nowMillis, pathProbeTimeoutMillis(rttSampleMillis));
    }

    private boolean observePathSuspicion(long nowMillis, long rttSampleMillis, long deliveredAtSend,
                                         boolean lowFlight) {
        if (this.pathProbeUsesLowFlightEnvelope && !lowFlight) {
            return false;
        }
        long previousDeliveredAtSend = this.pathSuspicionDeliveredAtSend;
        long previousObservedAtMillis = this.pathSuspicionObservedAtMillis;
        long minimum = Math.min(this.pathSuspicionMinimumRttMillis, rttSampleMillis);
        long maximum = Math.max(this.pathSuspicionMaximumRttMillis, rttSampleMillis);
        if (!this.pathSamplesAreStable(minimum, maximum)) {
            return false;
        }
        if (deliveredAtSend <= previousDeliveredAtSend || nowMillis <= previousObservedAtMillis) {
            return true;
        }
        this.pathSuspicionMinimumRttMillis = minimum;
        this.pathSuspicionMaximumRttMillis = maximum;
        this.pathSuspicionDeliveredAtSend = deliveredAtSend;
        this.pathSuspicionObservedAtMillis = nowMillis;
        this.pathSuspicionSamples++;
        this.pathSuspectRttMillis = minimum;
        return true;
    }

    private void beginPathDrain(long nowMillis) {
        this.invalidateLossEvidenceForPathTransition();
        this.pathState = PathState.DRAIN;
        this.pathAttempts = Math.min(PATH_MAX_ATTEMPTS, this.pathAttempts + 1);
        this.pathPreProbeCwnd = this.cwnd;
        this.pathProbeBoundaryDelivered = this.deliveredBytes;
        this.pathProbeDeadlineMillis = saturatingAdd(nowMillis,
                pathProbeTimeoutMillis(this.pathSuspectRttMillis));
        this.pathDrainHoldMillis = clamp(this.pathSuspectRttMillis,
                PATH_DRAIN_HOLD_MIN_MILLIS, PATH_DRAIN_HOLD_MAX_MILLIS);
        this.pathLowFlightSinceMillis = -1L;
        this.cwnd = this.minimumCwnd;
        this.resetPathSuspicion();
        this.resetPathStepCandidate();
    }

    private void observePathCandidate(long nowMillis, long rttSampleMillis, int txInFlight,
                                      int currentBytesInFlight, long deliveredAtSend, long sendTimeMillis,
                                      boolean appLimited) {
        if (txInFlight > this.minimumCwnd || currentBytesInFlight > this.minimumCwnd
                || (this.pathProbeUsesLowFlightEnvelope && !appLimited)) {
            this.pathLowFlightSinceMillis = -1L;
            if (this.pathState == PathState.SAMPLE) {
                this.invalidateLossEvidenceForPathTransition();
                this.pathState = PathState.DRAIN;
                this.resetPathStepCandidate();
            }
            return;
        }
        if (this.pathLowFlightSinceMillis < 0L) {
            this.pathLowFlightSinceMillis = nowMillis;
            return;
        }
        if (sendTimeMillis < saturatingAdd(this.pathLowFlightSinceMillis, this.pathDrainHoldMillis)
                || deliveredAtSend <= this.pathProbeBoundaryDelivered) {
            return;
        }
        if (this.pathState == PathState.DRAIN) {
            this.invalidateLossEvidenceForPathTransition();
            this.pathState = PathState.SAMPLE;
            this.pathStepMinimumRttMillis = rttSampleMillis;
            this.pathStepMaximumRttMillis = rttSampleMillis;
            this.pathStepDeliveredAtSend = deliveredAtSend;
            this.pathStepObservedAtMillis = nowMillis;
            this.pathStepSamples = 1;
            return;
        }
        if (deliveredAtSend <= this.pathStepDeliveredAtSend || nowMillis <= this.pathStepObservedAtMillis) {
            return;
        }
        this.pathStepMinimumRttMillis = Math.min(this.pathStepMinimumRttMillis, rttSampleMillis);
        this.pathStepMaximumRttMillis = Math.max(this.pathStepMaximumRttMillis, rttSampleMillis);
        this.pathStepDeliveredAtSend = deliveredAtSend;
        this.pathStepObservedAtMillis = nowMillis;
        this.pathStepSamples++;
        if (!this.pathSamplesAreStable(this.pathStepMinimumRttMillis, this.pathStepMaximumRttMillis)) {
            this.failPathProbe(nowMillis);
            return;
        }
        if (this.pathStepSamples >= PATH_STEP_CONFIRMATION_SAMPLES) {
            this.acceptPathStep(nowMillis);
        }
    }

    private void acceptPathStep(long nowMillis) {
        // Sampling deliberately holds the flight at two MTUs. Restore the pre-probe safe window before restarting
        // discovery, otherwise a successfully identified high-RTT path must bootstrap from the probe floor.
        this.restorePathProbeWindow();
        this.minimumRttMillis = this.pathStepMinimumRttMillis;
        this.minimumRttTimestampMillis = nowMillis;
        this.minimumRttUsesLowFlightEnvelope = this.pathProbeUsesLowFlightEnvelope;
        // The new propagation delay changes the BDP. Retain the bandwidth seed and any loss cap, but restart
        // full-bandwidth discovery so a clean-handshake sample cannot declare the impaired path full prematurely.
        this.cwnd = Math.max(this.cwnd, Math.min(this.initialCwnd, this.inflightLimit));
        this.startup = true;
        this.fullBandwidthBytesPerMillis = 0D;
        this.fullBandwidthRounds = 0;
        // Delay-qualified evidence collected against the old RTT baseline cannot classify the accepted path.
        // Preserve any installed HOLD cap and path-independent HARD evidence.
        this.resetPathScopedLossEvidence();
        this.resetPathTransition();
    }

    private void advancePathState(long nowMillis) {
        if ((this.pathState == PathState.SUSPECT || this.pathState == PathState.DRAIN
                || this.pathState == PathState.SAMPLE) && nowMillis >= this.pathProbeDeadlineMillis) {
            if (this.pathState == PathState.SUSPECT) {
                this.invalidateLossEvidenceForPathTransition();
                this.pathState = PathState.STEADY;
                this.pathProbeUsesLowFlightEnvelope = false;
                this.resetPathSuspicion();
                this.pathProbeDeadlineMillis = -1L;
            } else {
                this.failPathProbe(nowMillis);
            }
        } else if ((this.pathState == PathState.COOLDOWN || this.pathState == PathState.REJECTED_COOLDOWN)
                && nowMillis >= this.pathCooldownUntilMillis) {
            if (!this.isPathDelayEvidenceActionable()) {
                this.invalidateLossEvidenceForPathTransition();
            }
            this.pathState = PathState.STEADY;
            this.pathCooldownUntilMillis = -1L;
        }
    }

    private void failPathProbe(long nowMillis) {
        this.restorePathProbeWindow();
        if (this.pathAttempts >= PATH_MAX_ATTEMPTS && !this.pathLossSuppressionExhausted) {
            // Three complete low-flight drains are enough to stop granting the candidate path immunity from
            // delay-qualified congestion evidence. Keep retrying the path validation after its bounded cooldown,
            // but never regain that suppression privilege until a path is accepted or the baseline is reset.
            this.resetPathScopedLossEvidence();
            this.pathLossSuppressionExhausted = true;
        }
        this.enterPathCooldown(nowMillis, this.pathSuspectRttMillis, true);
    }

    private void restorePathProbeWindow() {
        double safeOldTarget = this.initialCwnd;
        if (this.maxBandwidthBytesPerMillis > 0D && this.minimumRttMillis != Long.MAX_VALUE) {
            safeOldTarget = Math.max(safeOldTarget, Math.min(this.maximumCwnd,
                    this.maxBandwidthBytesPerMillis * Math.max(this.minimumRttMillis, this.sendQuantumMillis)
                            * CWND_GAIN));
        }
        double restored = Math.min(this.pathPreProbeCwnd, safeOldTarget);
        restored = Math.min(restored, this.inflightLimit);
        this.cwnd = Math.max(this.minimumCwnd, restored);
    }

    private void enterPathCooldown(long nowMillis, long rttSampleMillis, boolean completedAttempt) {
        this.invalidateLossEvidenceForPathTransition();
        this.pathState = PathState.COOLDOWN;
        this.pathCooldownUntilMillis = saturatingAdd(nowMillis, scaledAndClamped(rttSampleMillis,
                PATH_COOLDOWN_RTT_MULTIPLIER, PATH_COOLDOWN_MIN_MILLIS, PATH_COOLDOWN_MAX_MILLIS));
        this.pathProbeDeadlineMillis = -1L;
        this.pathLowFlightSinceMillis = -1L;
        if (!completedAttempt) {
            this.pathSuspectRttMillis = rttSampleMillis;
        }
        this.resetPathSuspicion();
        this.resetPathStepCandidate();
        this.pathProbeUsesLowFlightEnvelope = false;
    }

    private void enterRejectedPathCooldown(long nowMillis, long rttSampleMillis) {
        this.pathState = PathState.REJECTED_COOLDOWN;
        // The candidate survived the suspicion phase and a deliberate drain before the known baseline disproved
        // it. Repeating that drain on the shorter retry cooldown can keep a shallow, queued path at two MTUs for
        // most of its useful time. Wait one complete validation horizon; a genuinely lower RTT still takes the
        // immediate minimum-RTT branch above and cancels this cooldown.
        this.pathCooldownUntilMillis = saturatingAdd(nowMillis, pathProbeTimeoutMillis(rttSampleMillis));
        this.pathProbeDeadlineMillis = -1L;
        this.pathLowFlightSinceMillis = -1L;
        this.resetPathSuspicion();
        this.resetPathStepCandidate();
        this.pathProbeUsesLowFlightEnvelope = false;
    }

    private boolean pathSamplesAreStable(long minimum, long maximum) {
        double upperBound = Math.max(Math.ceil(minimum * PATH_STEP_STABILITY_MULTIPLIER),
                minimum + PATH_STEP_STABILITY_ABSOLUTE_DELTA_MILLIS);
        return maximum <= upperBound;
    }

    private static long pathStepThreshold(long minimumRttMillis, boolean lowFlight) {
        double multiplier = lowFlight ? LOW_FLIGHT_PATH_STEP_MULTIPLIER : PATH_STEP_MULTIPLIER;
        long absoluteDelta = lowFlight
                ? LOW_FLIGHT_PATH_STEP_ABSOLUTE_DELTA_MILLIS : PATH_STEP_ABSOLUTE_DELTA_MILLIS;
        return Math.max((long) Math.ceil(minimumRttMillis * multiplier), minimumRttMillis + absoluteDelta);
    }

    private void resetPathSuspicion() {
        this.pathSuspicionMinimumRttMillis = Long.MAX_VALUE;
        this.pathSuspicionMaximumRttMillis = -1L;
        this.pathSuspicionDeliveredAtSend = -1L;
        this.pathSuspicionObservedAtMillis = -1L;
        this.pathSuspicionSamples = 0;
    }

    private void resetPathStepCandidate() {
        this.pathStepMinimumRttMillis = Long.MAX_VALUE;
        this.pathStepMaximumRttMillis = -1L;
        this.pathStepDeliveredAtSend = -1L;
        this.pathStepObservedAtMillis = -1L;
        this.pathStepSamples = 0;
    }

    private void resetPathTransition() {
        this.resetPathTransition(false);
    }

    private void resetPathTransition(boolean preserveLossSuppressionExhaustion) {
        boolean keepSuppressionExhausted = preserveLossSuppressionExhaustion
                && this.pathLossSuppressionExhausted;
        int retainedPathAttempts = this.pathAttempts;
        if (!this.isPathDelayEvidenceActionable()) {
            this.invalidateLossEvidenceForPathTransition();
        }
        this.pathState = PathState.STEADY;
        this.pathSuspectRttMillis = -1L;
        this.pathProbeDeadlineMillis = -1L;
        this.pathCooldownUntilMillis = -1L;
        this.pathProbeBoundaryDelivered = -1L;
        this.pathLowFlightSinceMillis = -1L;
        this.pathDrainHoldMillis = 0L;
        this.pathPreProbeCwnd = 0D;
        this.pathAttempts = keepSuppressionExhausted ? retainedPathAttempts : 0;
        this.pathLossSuppressionExhausted = keepSuppressionExhausted;
        this.pathProbeUsesLowFlightEnvelope = false;
        this.resetPathSuspicion();
        this.resetPathStepCandidate();
    }

    private boolean isPathDelayEvidenceActionable() {
        return this.pathLossSuppressionExhausted || this.pathState == PathState.STEADY
                || this.pathState == PathState.REJECTED_COOLDOWN;
    }

    private void invalidateLossEvidenceForPathTransition() {
        if (this.pathLossSuppressionExhausted) {
            return;
        }
        this.resetPathScopedLossEvidence();
    }

    private void resetPathScopedLossEvidence() {
        this.resetDelayLossBucket();
        this.resetDelayRoundLossEvidence();
        this.lossClearBuckets = 0;
        this.lossClearPackets = 0L;
    }

    private static long pathProbeTimeoutMillis(long rttMillis) {
        return scaledAndClamped(rttMillis, PATH_TIMEOUT_RTT_MULTIPLIER,
                PATH_TIMEOUT_MIN_MILLIS, PATH_TIMEOUT_MAX_MILLIS);
    }

    private static long clamp(long value, long minimum, long maximum) {
        return Math.max(minimum, Math.min(maximum, value));
    }

    private static long scaledAndClamped(long value, long multiplier, long minimum, long maximum) {
        if (value >= (maximum + multiplier - 1L) / multiplier) {
            return maximum;
        }
        return clamp(value * multiplier, minimum, maximum);
    }

    private static long saturatingAdd(long value, long positiveIncrement) {
        return value >= Long.MAX_VALUE - positiveIncrement ? Long.MAX_VALUE : value + positiveIncrement;
    }

    private void updateFullBandwidth(boolean newRound, long completedRoundDeliveredBytes) {
        if (!this.startup || !newRound || completedRoundDeliveredBytes < STARTUP_MINIMUM_ROUND_MTUS * (long) this.mtu
                || this.maxBandwidthBytesPerMillis <= 0D) {
            return;
        }
        if (this.fullBandwidthBytesPerMillis == 0D
                || this.maxBandwidthBytesPerMillis >= this.fullBandwidthBytesPerMillis * FULL_BANDWIDTH_GROWTH) {
            this.fullBandwidthBytesPerMillis = this.maxBandwidthBytesPerMillis;
            this.fullBandwidthRounds = 0;
        } else if (++this.fullBandwidthRounds >= FULL_BANDWIDTH_ROUNDS) {
            this.startup = false;
        }
    }

    private void updateCongestionWindow(int acknowledgedBytes) {
        if (this.maxBandwidthBytesPerMillis <= 0D || this.minimumRttMillis == Long.MAX_VALUE) {
            return;
        }
        double target = Math.max(this.minimumCwnd, Math.min(this.maximumCwnd,
                this.maxBandwidthBytesPerMillis * Math.max(this.minimumRttMillis, this.sendQuantumMillis)
                        * CWND_GAIN));
        if (this.lossState == LossState.HOLD && this.lossHoldKind == LossHoldKind.DELAY
                && Double.isFinite(this.inflightLimit)) {
            // DELAY installs one explicit beta-reduced flight for the continuing congestion epoch. Letting the
            // short bandwidth filter decay against that smaller self-clocked flight applies a second, hidden
            // reduction even though HOLD deliberately forbids another loss response. Retain the installed flight
            // as the epoch's model floor; deliberate path drains and persistent congestion still collapse below it.
            target = Math.max(target, this.inflightLimit);
        }
        if (this.pathState == PathState.DRAIN || this.pathState == PathState.SAMPLE) {
            target = this.minimumCwnd;
        }
        target = Math.min(target, this.inflightLimit);
        if (this.cwnd < target) {
            this.cwnd = Math.min(target, this.cwnd + acknowledgedBytes);
        } else if (this.startup) {
            // Early delivery samples cover only a fraction of the initial flight. They must not collapse the
            // standards-sized initial window before a packet-timed round has measured the path.
            this.cwnd = Math.max(this.cwnd, this.minimumCwnd);
        } else if (this.cwnd > target) {
            // Avoid an abrupt model-only flight retirement; converge by at most newly delivered bytes per ACK.
            this.cwnd = Math.max(target, this.cwnd - acknowledgedBytes);
        }
        this.cwnd = Math.max(this.minimumCwnd, Math.min(this.maximumCwnd, this.cwnd));
    }

    private void refillPacingTokens(long nowMillis) {
        double burst = this.maximumBurstBytes();
        if (this.pacingUpdatedAtMillis < 0L) {
            this.pacingUpdatedAtMillis = nowMillis;
            this.pacingTokens = Math.min(burst, 2D * this.mtu);
            return;
        }
        if (nowMillis > this.pacingUpdatedAtMillis) {
            this.pacingTokens = Math.min(burst, this.pacingTokens
                    + (nowMillis - this.pacingUpdatedAtMillis) * this.pacingRateBytesPerMillis());
            this.pacingUpdatedAtMillis = nowMillis;
        } else {
            this.pacingTokens = Math.min(this.pacingTokens, burst);
        }
    }

    private double pacingRateBytesPerMillis() {
        double rate;
        if (this.maxBandwidthBytesPerMillis > 0D) {
            rate = this.maxBandwidthBytesPerMillis
                    * (this.startup ? STARTUP_PACING_GAIN : this.steadyPacingGain());
        } else {
            rate = this.cwnd / INITIAL_RTT_MILLIS * STARTUP_PACING_GAIN;
        }
        if (this.startup || this.pathState != PathState.STEADY) {
            // Discovery must be able to exercise the standards-sized initial window within one event-loop send
            // quantum. The congestion window still bounds path drains and persistent congestion at two MTUs,
            // while maximumBurstBytes independently caps any one activation at eight MTUs.
            rate = Math.max(rate, this.initialCwnd / this.sendQuantumMillis);
        }
        return rate;
    }

    private double steadyPacingGain() {
        int phase = (int) (this.roundCount & 7L);
        if (phase == 0) {
            return PROBE_UP_PACING_GAIN;
        }
        if (phase == 1) {
            return PROBE_DOWN_PACING_GAIN;
        }
        return 1D;
    }

    private double maximumBurstBytes() {
        long creditedQuanta = this.continuouslyBacklogged ? 2L : 1L;
        double pacedQuantum = this.pacingRateBytesPerMillis() * this.sendQuantumMillis * creditedQuanta + this.mtu;
        return Math.max(2D * this.mtu, Math.min(MAX_BURST_DATAGRAMS * (double) this.mtu, pacedQuantum));
    }

    void onSenderIdle() {
        this.continuouslyBacklogged = false;
    }

    SendState captureSendState(RakDatagramPacket datagram) {
        SendState state = new SendState();
        this.captureSendState(datagram, state);
        return state;
    }

    void captureSendState(RakDatagramPacket datagram, SendState state) {
        state.pacingTokens = this.pacingTokens;
        state.pacingUpdatedAtMillis = this.pacingUpdatedAtMillis;
        state.firstSendTimeMillis = this.firstSendTimeMillis;
        state.deliveredTimeMillis = this.deliveredTimeMillis;
        state.deliveredBytesAtSend = datagram.getDeliveredBytesAtSend();
        state.deliveredTimeAtSend = datagram.getDeliveredTimeAtSend();
        state.packetFirstSendTime = datagram.getFirstSendTime();
        state.modelSendTime = datagram.getModelSendTime();
        state.modelTxInFlight = datagram.getModelTxInFlight();
        state.modelSampleValid = datagram.isModelSampleValid();
        state.modelAppLimited = datagram.isModelAppLimited();
        state.modelLossClassified = datagram.isModelLossClassified();
        state.continuouslyBacklogged = this.continuouslyBacklogged;
    }

    void restoreSendState(RakDatagramPacket datagram, SendState state) {
        this.pacingTokens = state.pacingTokens;
        this.pacingUpdatedAtMillis = state.pacingUpdatedAtMillis;
        this.firstSendTimeMillis = state.firstSendTimeMillis;
        this.deliveredTimeMillis = state.deliveredTimeMillis;
        this.continuouslyBacklogged = state.continuouslyBacklogged;
        if (state.modelSampleValid) {
            datagram.setModelSendState(state.deliveredBytesAtSend, state.deliveredTimeAtSend,
                    state.packetFirstSendTime, state.modelSendTime, state.modelTxInFlight, state.modelAppLimited);
        } else {
            datagram.clearModelSendState();
        }
        datagram.setModelLossClassified(state.modelLossClassified);
    }

    double getCongestionWindow() {
        return this.cwnd;
    }

    double getMaxBandwidthBytesPerMillis() {
        return this.maxBandwidthBytesPerMillis > 0D ? this.maxBandwidthBytesPerMillis : -1D;
    }

    long getMinimumRttMillis() {
        return this.minimumRttMillis == Long.MAX_VALUE ? -1L : this.minimumRttMillis;
    }

    long getMinimumRttTimestampMillis() {
        return this.minimumRttTimestampMillis;
    }

    double getPacingRateBytesPerMillis() {
        return this.pacingRateBytesPerMillis();
    }

    boolean isStartup() {
        return this.startup;
    }

    boolean isPersistentCongestion() {
        return this.persistentCongestion;
    }

    int getPathAttempts() {
        return this.pathAttempts;
    }

    long getLossResponseCount() {
        return this.lossResponseCount;
    }

    double getInflightLimit() {
        return this.inflightLimit;
    }

    boolean isLossResponseHeld() {
        return this.lossState == LossState.HOLD;
    }

    long getHardLossResponseCount() {
        return this.hardLossResponseCount;
    }

    long getDelayLossResponseCount() {
        return this.delayLossResponseCount;
    }

    double getRecentLossRate() {
        return this.recentLossRate;
    }

    long getRoundCount() {
        return this.roundCount;
    }

    private enum PathState {
        STEADY,
        SUSPECT,
        DRAIN,
        SAMPLE,
        COOLDOWN,
        REJECTED_COOLDOWN
    }

    private enum LossState {
        ARMED,
        HOLD
    }

    private enum LossHoldKind {
        NONE,
        HARD,
        DELAY
    }

    static final class SendState {
        private double pacingTokens;
        private long pacingUpdatedAtMillis;
        private long firstSendTimeMillis;
        private long deliveredTimeMillis;
        private long deliveredBytesAtSend;
        private long deliveredTimeAtSend;
        private long packetFirstSendTime;
        private long modelSendTime;
        private int modelTxInFlight;
        private boolean modelSampleValid;
        private boolean modelAppLimited;
        private boolean modelLossClassified;
        private boolean continuouslyBacklogged;

        SendState() {
        }
    }

    static final class DatagramSample {
        private final int size;
        private final long deliveredBytesAtSend;
        private final long deliveredTimeAtSend;
        private final long firstSendTime;
        private final long sendTime;
        private final int txInFlight;
        private final boolean appLimited;

        private DatagramSample(int size, long deliveredBytesAtSend, long deliveredTimeAtSend,
                               long firstSendTime, long sendTime, int txInFlight, boolean appLimited) {
            this.size = size;
            this.deliveredBytesAtSend = deliveredBytesAtSend;
            this.deliveredTimeAtSend = deliveredTimeAtSend;
            this.firstSendTime = firstSendTime;
            this.sendTime = sendTime;
            this.txInFlight = txInFlight;
            this.appLimited = appLimited;
        }

        int size() {
            return this.size;
        }

        long sendTime() {
            return this.sendTime;
        }
    }

    static final class UnreliableSendState {
        private final double pacingTokens;
        private final long pacingUpdatedAtMillis;
        private final long firstSendTimeMillis;
        private final long deliveredTimeMillis;
        private final boolean continuouslyBacklogged;

        private UnreliableSendState(double pacingTokens, long pacingUpdatedAtMillis, long firstSendTimeMillis,
                                    long deliveredTimeMillis, boolean continuouslyBacklogged) {
            this.pacingTokens = pacingTokens;
            this.pacingUpdatedAtMillis = pacingUpdatedAtMillis;
            this.firstSendTimeMillis = firstSendTimeMillis;
            this.deliveredTimeMillis = deliveredTimeMillis;
            this.continuouslyBacklogged = continuouslyBacklogged;
        }
    }
}
