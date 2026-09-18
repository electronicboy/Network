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
import org.cloudburstmc.netty.channel.raknet.packet.RakDatagramPacket;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/** Event-loop-confined PTO timer state for bounded recovery. */
final class RakBoundedRecovery {
    static final int MAX_NACK_DATAGRAMS_PER_FLUSH = 2;
    static final long MAX_BACKED_OFF_RTO_MILLIS = 8_000L;

    private final LongSupplier clock;
    private final LongSupplier jitterSource;
    private long nextProbeAtMillis = -1L;
    private long probePacingUntilMillis = -1L;
    private RakDatagramPacket probeAnchor;
    private int ptoBackoff;
    private boolean closed;

    RakBoundedRecovery(LongSupplier clock) {
        this(clock, () -> ThreadLocalRandom.current().nextLong());
    }

    RakBoundedRecovery(LongSupplier clock, LongSupplier jitterSource) {
        this.clock = clock;
        this.jitterSource = jitterSource;
    }

    long currentTimeMillis() {
        return this.clock.getAsLong();
    }

    void onReliableSend(RakSlidingWindow window, RakDatagramPacket datagram) {
        if (this.closed) {
            return;
        }
        this.scheduleAttemptDeadline(window, datagram);
        if (this.probeAnchor == null || isEarlier(datagram, this.probeAnchor)) {
            this.probeAnchor = datagram;
        }
        this.updateProbeDeadline(window);
    }

    void onNackRetransmission(RakSlidingWindow window, RakDatagramPacket datagram,
                              Collection<RakDatagramPacket> outstanding) {
        if (this.closed) {
            return;
        }
        // A NACK retransmission gets its own loss deadline. It does not move an older outstanding attempt's
        // deadline or reset the global PTO backoff.
        this.scheduleAttemptDeadline(window, datagram);
        if (datagram == this.probeAnchor) {
            this.selectProbeAnchor(outstanding);
        } else if (this.probeAnchor == null || isEarlier(datagram, this.probeAnchor)) {
            this.probeAnchor = datagram;
        }
        this.updateProbeDeadline(window);
    }

    boolean scheduleNack(RakDatagramPacket datagram) {
        if (this.closed || datagram.isRetransmissionPending()) {
            return false;
        }
        datagram.setRetransmissionPending(true);
        return true;
    }

    void onAcknowledgementProgress(RakSlidingWindow window, RakDatagramPacket acknowledged,
                                   Collection<RakDatagramPacket> outstanding) {
        if (this.closed) {
            return;
        }
        this.ptoBackoff = 0;
        this.probePacingUntilMillis = -1L;
        // Recompute from the immutable deadlines of the attempts that remain outstanding. An ACK for a later
        // datagram must never turn an older attempt's deadline into now + RTO.
        if (acknowledged == this.probeAnchor || this.probeAnchor == null
                || !this.probeAnchor.isReliableOutstanding()) {
            this.selectProbeAnchor(outstanding);
        }
        this.updateProbeDeadline(window);
    }

    boolean isProbeDue(RakSlidingWindow window) {
        return !this.closed && window.getUnackedBytes() > 0 && this.nextProbeAtMillis != -1L
                && this.currentTimeMillis() >= this.nextProbeAtMillis;
    }

    RakDatagramPacket getProbeAnchor() {
        return this.probeAnchor;
    }

    void onProbeSent(RakSlidingWindow window, RakDatagramPacket datagram,
                     Collection<RakDatagramPacket> outstanding) {
        if (this.closed) {
            return;
        }
        if (this.ptoBackoff < 30) {
            this.ptoBackoff++;
        }
        this.probePacingUntilMillis = this.scheduleAttemptDeadline(window, datagram);
        this.selectProbeAnchor(outstanding);
        this.updateProbeDeadline(window);
    }

    void onProbeDeferred(RakSlidingWindow window, Collection<RakDatagramPacket> outstanding) {
        if (!this.closed) {
            this.probePacingUntilMillis = this.newDeadline(window);
            if (this.probeAnchor == null || !this.probeAnchor.isReliableOutstanding()) {
                this.selectProbeAnchor(outstanding);
            }
            this.updateProbeDeadline(window);
        }
    }

    void refreshProbeDeadline(RakSlidingWindow window, Collection<RakDatagramPacket> outstanding) {
        this.selectProbeAnchor(outstanding);
        this.updateProbeDeadline(window);
    }

    private void selectProbeAnchor(Collection<RakDatagramPacket> outstanding) {
        this.probeAnchor = null;
        for (RakDatagramPacket datagram : outstanding) {
            if (datagram.isReliableOutstanding()
                    && (this.probeAnchor == null || isEarlier(datagram, this.probeAnchor))) {
                this.probeAnchor = datagram;
            }
        }
    }

    private void updateProbeDeadline(RakSlidingWindow window) {
        if (this.closed || window.getUnackedBytes() == 0) {
            this.nextProbeAtMillis = -1L;
            this.probePacingUntilMillis = -1L;
            this.probeAnchor = null;
            return;
        }

        if (this.probeAnchor == null) {
            this.nextProbeAtMillis = -1L;
            return;
        }
        this.nextProbeAtMillis = Math.max(this.probeAnchor.getNextSend(), this.probePacingUntilMillis);
    }

    private static boolean isEarlier(RakDatagramPacket first, RakDatagramPacket second) {
        return first.getNextSend() < second.getNextSend()
                || (first.getNextSend() == second.getNextSend()
                && first.getSendOrdinal() < second.getSendOrdinal());
    }

    private long scheduleAttemptDeadline(RakSlidingWindow window, RakDatagramPacket datagram) {
        long deadline = this.newDeadline(window);
        datagram.setNextSend(deadline);
        return deadline;
    }

    private long newDeadline(RakSlidingWindow window) {
        long rto = this.getEffectiveRtoMillis(window);
        long jitterRange = Math.max(1L, rto / 10L);
        long jitter = Math.floorMod(this.jitterSource.getAsLong(), jitterRange);
        return this.currentTimeMillis() + rto + jitter;
    }

    long getEffectiveRtoMillis(RakSlidingWindow window) {
        long rto = window.getRtoForRetransmission();
        for (int i = 0; i < this.ptoBackoff && rto < MAX_BACKED_OFF_RTO_MILLIS; i++) {
            rto = Math.min(MAX_BACKED_OFF_RTO_MILLIS, rto * 2L);
        }
        return rto;
    }

    long getNextProbeAtMillis() {
        return this.nextProbeAtMillis;
    }

    int getPtoBackoff() {
        return this.ptoBackoff;
    }

    void close() {
        this.closed = true;
        this.nextProbeAtMillis = -1L;
        this.probePacingUntilMillis = -1L;
        this.probeAnchor = null;
        this.ptoBackoff = 0;
    }

    static FlushBudget nackFlushBudget(int mtu) {
        return new FlushBudget(MAX_NACK_DATAGRAMS_PER_FLUSH,
                MAX_NACK_DATAGRAMS_PER_FLUSH * mtu);
    }

    static FlushBudget ptoFlushBudget(int mtu) {
        return new FlushBudget(1, mtu);
    }

    static final class FlushBudget {
        private final int maxDatagrams;
        private final int maxBytes;
        private int datagrams;
        private int bytes;

        private FlushBudget(int maxDatagrams, int maxBytes) {
            this.maxDatagrams = maxDatagrams;
            this.maxBytes = maxBytes;
        }

        boolean canConsume(int size) {
            return this.datagrams < this.maxDatagrams && this.bytes + size <= this.maxBytes;
        }

        void consume(int size) {
            if (!this.canConsume(size)) {
                throw new IllegalStateException("Recovery flush budget exceeded");
            }
            this.datagrams++;
            this.bytes += size;
        }

        int getDatagrams() {
            return this.datagrams;
        }
    }
}
