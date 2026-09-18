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

package org.cloudburstmc.netty.channel.raknet.config;

import org.cloudburstmc.netty.channel.raknet.RakState;

public interface RakChannelMetrics {

    default void bytesIn(int count) {
    }

    default void bytesOut(int count) {
    }

    default void rakDatagramsIn(int count) {
    }

    default void rakDatagramsOut(int count) {
    }

    default void encapsulatedIn(int count) {
    }

    default void encapsulatedOut(int count) {
    }

    default void rakStaleDatagrams(int count) {
    }

    default void ackIn(int count) {
    }

    default void ackOut(int count) {
    }

    default void nackOut(int count) {
    }

    default void nackIn(int count) {
    }

    default void stateChange(RakState state) {
    }

    default void queuedPacketBytes(int count) {
    }

    /**
     * Invoked for every RakNet data datagram write. Retransmission attempts start at {@code 1}; an original
     * transmission has attempt {@code 0}. The send type is a bounded dimension suitable for counters; implementations
     * can also sum {@code bytes} and histogram retransmission attempts without reconstructing them from packet events.
     *
     * @param sendType why the datagram was written
     * @param bytes encoded datagram size in bytes
     * @param retransmissionAttempt zero for an original transmission, otherwise the one-based attempt number
     * @param bytesInFlight congestion-controlled physical datagram bytes; in model mode this includes tracked
     *                      unreliable datagrams as well as reliable attempts, including the newly tracked send
     */
    default void rakDatagramSent(RakDatagramSendType sendType, int bytes, int retransmissionAttempt,
                                 int bytesInFlight) {
    }

    /**
     * Invoked when an acknowledgement releases a tracked reliable datagram.
     *
     * @param bytes acknowledged datagram size in bytes
     * @param retransmissionAttempt zero when the datagram was never retransmitted, otherwise its final attempt number
     * @param observedAtMillis wall-clock time of the acknowledgement, in milliseconds since the Unix epoch
     * @param previousAckProgressAtMillis the previous progress timestamp, or {@code -1} when unavailable
     * @param recoveryStartedAtMillis the start of the current observed recovery epoch, or {@code -1} when the
     *                                sender has no observed recovery epoch active
     */
    default void rakAcknowledgementProgress(int bytes, int retransmissionAttempt, long observedAtMillis,
                                            long previousAckProgressAtMillis, long recoveryStartedAtMillis) {
    }

    /**
     * Reports a point-in-time view of sender recovery state. RTT values are {@code -1} until sampled and recovery
     * timestamps are {@code -1} when unavailable or inactive. Periodic snapshots are limited to at most one every
     * 100 milliseconds per session; recovery transitions and the terminal snapshot are reported immediately.
     *
     * @param observedAtMillis wall-clock observation time, in milliseconds since the Unix epoch
     * @param bytesInFlight congestion-controlled physical datagram bytes; in model mode this includes tracked
     *                      unreliable datagrams as well as reliable attempts
     * @param congestionWindow congestion window in bytes
     * @param slowStartThreshold slow-start threshold in bytes
     * @param smoothedRtt smoothed round-trip time in milliseconds, or {@code -1} until sampled
     * @param rttVariance round-trip time variation in milliseconds, or {@code -1} until sampled
     * @param retransmissionTimeout current retransmission timeout in milliseconds
     * @param retransmittedDatagramsInFlight tracked datagrams retransmitted at least once and not yet acknowledged
     * @param lastAckProgressAtMillis most recent ACK progress time in milliseconds since the Unix epoch
     * @param recoveryStartedAtMillis active recovery epoch start in milliseconds since the Unix epoch, or {@code -1}
     */
    default void rakRecoveryState(long observedAtMillis, int bytesInFlight, double congestionWindow,
                                  double slowStartThreshold, double smoothedRtt, double rttVariance,
                                  long retransmissionTimeout, int retransmittedDatagramsInFlight,
                                  long lastAckProgressAtMillis, long recoveryStartedAtMillis) {
    }

    /**
     * Reports the bounded-cardinality state of the experimental sender delivery model. This callback has the same
     * at-most-once-per-100-ms periodic cadence as {@link #rakRecoveryState}; exporters should aggregate it using the
     * same bounded cohort labels rather than remote-address labels. The callback is emitted only for model-based
     * sessions; delivery rate and minimum RTT are {@code -1} until sampled.
     *
     * @param observedAtMillis wall-clock observation time, in milliseconds since the Unix epoch
     * @param estimatedDeliveryRateBytesPerSecond maximum filtered delivery rate, or {@code -1} until sampled
     * @param pacingRateBytesPerSecond current sender pacing rate
     * @param minimumRttMillis filtered minimum RTT, or {@code -1} until sampled
     * @param recentLossRate most recent packet-round loss fraction
     * @param packetRound completed packet-timed round count
     * @param startup whether the controller is still in startup
     * @param persistentCongestion whether persistent no-progress congestion is active
     */
    default void rakCongestionModelState(long observedAtMillis, double estimatedDeliveryRateBytesPerSecond,
                                         double pacingRateBytesPerSecond, long minimumRttMillis,
                                         double recentLossRate, long packetRound, boolean startup,
                                         boolean persistentCongestion) {
    }

    /**
     * Reports the bounded-cardinality state of the experimental sender delivery model, including cumulative
     * loss-response counters. The counters are lifetime-monotonic for one session and identify reductions caused by
     * path-independent hard-loss evidence separately from reductions caused by delay-qualified loss evidence.
     *
     * <p>The default implementation delegates to the original eight-argument callback so existing metrics
     * implementations continue to receive model snapshots without a source or binary compatibility break.</p>
     *
     * @param observedAtMillis wall-clock observation time, in milliseconds since the Unix epoch
     * @param estimatedDeliveryRateBytesPerSecond maximum filtered delivery rate, or {@code -1} until sampled
     * @param pacingRateBytesPerSecond current sender pacing rate
     * @param minimumRttMillis filtered minimum RTT, or {@code -1} until sampled
     * @param recentLossRate most recent packet-round loss fraction
     * @param packetRound completed packet-timed round count
     * @param startup whether the controller is still in startup
     * @param persistentCongestion whether persistent no-progress congestion is active
     * @param hardLossResponseCount cumulative hard-loss reductions in this session
     * @param delayLossResponseCount cumulative delay-qualified loss reductions in this session
     */
    default void rakCongestionModelState(long observedAtMillis, double estimatedDeliveryRateBytesPerSecond,
                                         double pacingRateBytesPerSecond, long minimumRttMillis,
                                         double recentLossRate, long packetRound, boolean startup,
                                         boolean persistentCongestion, long hardLossResponseCount,
                                         long delayLossResponseCount) {
        this.rakCongestionModelState(observedAtMillis, estimatedDeliveryRateBytesPerSecond,
                pacingRateBytesPerSecond, minimumRttMillis, recentLossRate, packetRound, startup,
                persistentCongestion);
    }

    /**
     * Invoked after the terminal recovery snapshot when the session closes. Gauge exporters should remove any
     * channel-local state retained for this session.
     *
     * @param observedAtMillis wall-clock close time, in milliseconds since the Unix epoch
     */
    default void rakRecoveryStateClosed(long observedAtMillis) {
    }
}
