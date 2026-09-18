/*
 * Copyright 2024 CloudburstMC
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

import org.cloudburstmc.netty.channel.raknet.RakChildChannel;
import org.cloudburstmc.netty.channel.raknet.RakState;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Server-wide RakNet metrics callbacks.
 *
 * <p>The recovery callbacks carry a channel so implementations can maintain correct aggregate gauges as channels
 * open, change, and close. Prometheus exporters should aggregate these values server-wide or by a bounded cohort.
 * Remote addresses, GUIDs, and other peer identifiers must not be used as metric labels because that creates
 * unbounded cardinality.</p>
 */
public interface RakServerMetrics {

    default void channelOpen(InetSocketAddress address) {
    }

    default void channelClose(InetSocketAddress address) {
    }

    default void unconnectedPing(InetSocketAddress address) {
    }

    default void connectionInitPacket(InetSocketAddress address, int packetId) {
    }

    default void addressBlocked(InetSocketAddress address) {
    }

    default void addressUnblocked(InetSocketAddress address) {
    }

    default void invalidCookie(InetSocketAddress address) {
    }
 
    default void bytesIn(RakChildChannel channel, int count) {
    }

    default void bytesOut(RakChildChannel channel, int count) {
    }

    default void rakDatagramsIn(RakChildChannel channel, int count) {
    }

    default void rakDatagramsOut(RakChildChannel channel, int count) {
    }

    default void encapsulatedIn(RakChildChannel channel, int count) {
    }

    default void encapsulatedOut(RakChildChannel channel, int count) {
    }

    default void rakStaleDatagrams(RakChildChannel channel, int count) {
    }

    default void ackIn(RakChildChannel channel, int count) {
    }

    default void ackOut(RakChildChannel channel, int count) {
    }

    default void nackOut(RakChildChannel channel, int count) {
    }

    default void nackIn(RakChildChannel channel, int count) {
    }

    default void stateChange(RakChildChannel channel, RakState state) {
    }

    default void queuedPacketBytes(RakChildChannel channel, int count) {
    }

    /**
     * Counter source for original, NACK-retransmitted, and timeout-retransmitted data datagrams and bytes.
     *
     * @param channel channel that sent the datagram
     * @param sendType why the datagram was written
     * @param bytes encoded datagram size in bytes
     * @param retransmissionAttempt zero for an original transmission, otherwise the one-based attempt number
     * @param bytesInFlight congestion-controlled physical datagram bytes; in model mode this includes tracked
     *                      unreliable datagrams as well as reliable attempts
     */
    default void rakDatagramSent(RakChildChannel channel, RakDatagramSendType sendType, int bytes,
                                 int retransmissionAttempt, int bytesInFlight) {
    }

    /**
     * ACK progress event. Exporters can observe the ACK gap and recovery age by subtracting the supplied timestamps
     * from {@code observedAtMillis}; unavailable timestamps are {@code -1}.
     *
     * @param channel channel that received ACK progress
     * @param bytes acknowledged datagram size in bytes
     * @param retransmissionAttempt zero when the datagram was never retransmitted, otherwise its final attempt number
     * @param observedAtMillis wall-clock time of the acknowledgement, in milliseconds since the Unix epoch
     * @param previousAckProgressAtMillis previous ACK progress time, or {@code -1} when unavailable
     * @param recoveryStartedAtMillis active recovery epoch start, or {@code -1} when inactive
     */
    default void rakAcknowledgementProgress(RakChildChannel channel, int bytes, int retransmissionAttempt,
                                            long observedAtMillis, long previousAckProgressAtMillis,
                                            long recoveryStartedAtMillis) {
    }

    /**
     * Gauge source for per-channel recovery state. Periodic snapshots are limited to 10 Hz per channel, with immediate
     * recovery-transition and terminal snapshots, so implementations may safely maintain aggregate gauges in a
     * channel-keyed map.
     *
     * @param channel observed channel
     * @param observedAtMillis wall-clock observation time, in milliseconds since the Unix epoch
     * @param bytesInFlight congestion-controlled physical datagram bytes; in model mode this includes tracked
     *                      unreliable datagrams as well as reliable attempts
     * @param congestionWindow congestion window in bytes
     * @param slowStartThreshold slow-start threshold in bytes
     * @param smoothedRtt smoothed round-trip time in milliseconds, or {@code -1} until sampled
     * @param rttVariance round-trip time variation in milliseconds, or {@code -1} until sampled
     * @param retransmissionTimeout current retransmission timeout in milliseconds
     * @param retransmittedDatagramsInFlight retransmitted datagrams not yet acknowledged
     * @param lastAckProgressAtMillis most recent ACK progress time, in milliseconds since the Unix epoch
     * @param recoveryStartedAtMillis active recovery epoch start, or {@code -1} when inactive
     */
    default void rakRecoveryState(RakChildChannel channel, long observedAtMillis, int bytesInFlight,
                                  double congestionWindow, double slowStartThreshold, double smoothedRtt,
                                  double rttVariance, long retransmissionTimeout,
                                  int retransmittedDatagramsInFlight, long lastAckProgressAtMillis,
                                  long recoveryStartedAtMillis) {
    }

    /**
     * Server-scoped counterpart to {@link RakChannelMetrics#rakCongestionModelState}.
     *
     * @param channel observed child channel
     * @param observedAtMillis wall-clock observation time, in milliseconds since the Unix epoch
     * @param estimatedDeliveryRateBytesPerSecond maximum filtered delivery rate, or {@code -1} until sampled
     * @param pacingRateBytesPerSecond current sender pacing rate
     * @param minimumRttMillis filtered minimum RTT, or {@code -1} until sampled
     * @param recentLossRate most recent packet-round loss fraction
     * @param packetRound completed packet-timed round count
     * @param startup whether the controller is still in startup
     * @param persistentCongestion whether persistent no-progress congestion is active
     */
    default void rakCongestionModelState(RakChildChannel channel, long observedAtMillis,
                                         double estimatedDeliveryRateBytesPerSecond,
                                         double pacingRateBytesPerSecond, long minimumRttMillis,
                                         double recentLossRate, long packetRound, boolean startup,
                                         boolean persistentCongestion) {
    }

    /**
     * Server-scoped counterpart to the cumulative loss-response overload of
     * {@link RakChannelMetrics#rakCongestionModelState(long, double, double, long, double, long, boolean, boolean,
     * long, long)}. The default implementation delegates to the original callback for compatibility with existing
     * server metrics implementations.
     *
     * @param channel observed child channel
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
    default void rakCongestionModelState(RakChildChannel channel, long observedAtMillis,
                                         double estimatedDeliveryRateBytesPerSecond,
                                         double pacingRateBytesPerSecond, long minimumRttMillis,
                                         double recentLossRate, long packetRound, boolean startup,
                                         boolean persistentCongestion, long hardLossResponseCount,
                                         long delayLossResponseCount) {
        this.rakCongestionModelState(channel, observedAtMillis, estimatedDeliveryRateBytesPerSecond,
                pacingRateBytesPerSecond, minimumRttMillis, recentLossRate, packetRound, startup,
                persistentCongestion);
    }

    /**
     * Signals that the channel's terminal recovery snapshot has been delivered. Exporters should remove the channel
     * from any map used to compute aggregate gauges.
     *
     * @param channel closed channel
     * @param observedAtMillis wall-clock close time, in milliseconds since the Unix epoch
     */
    default void rakRecoveryStateClosed(RakChildChannel channel, long observedAtMillis) {
    }
}
