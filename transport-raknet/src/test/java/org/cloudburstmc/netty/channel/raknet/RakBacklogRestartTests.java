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
import java.util.List;

/**
 * A sender sitting on a queue it cannot drain has to go back to probing.
 *
 * <p>Every other route into startup is triggered by loss or by a change of path. A session that
 * settles below what the path will carry, with nothing lost, has none of them, and a unit pacing
 * gain cannot climb out on its own: pacing at exactly the estimate means the delivery measured is
 * the estimate.</p>
 */
public class RakBacklogRestartTests {
    private static final int MTU = 1_200;
    private static final long RTT_MILLIS = 20L;
    /** More waiting than any window this test reaches can carry - seconds of queued work. */
    private static final int DEEP_QUEUE_BYTES = 4_000_000;

    @Test
    public void aSenderBackloggedWithoutLossGoesBackToProbing() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> live = new ArrayList<>();
        try {
            long now = 100L;
            int sequence = 0;

            // Settle the controller out of startup on a steady rate. The queue runs dry at the
            // end of every burst, so this phase is not itself backlogged.
            for (int round = 0; round < 60; round++) {
                List<RakDatagramPacket> flight = new ArrayList<>();
                for (int i = 0; i < 8; i++) {
                    flight.add(send(window, live, sequence++, now, i == 7));
                }
                now += RTT_MILLIS;
                for (RakDatagramPacket datagram : flight) {
                    window.onAck(now, datagram, RTT_MILLIS);
                }
            }
            Assertions.assertFalse(window.isModelStartup(),
                    "the sender never left startup, so the test proves nothing");
            double settledPacing = window.getModelPacingRateBytesPerMillis();

            // Keep it continuously backlogged - the application never runs dry - with no loss.
            // Sample every round: startup ends itself once growth plateaus, which on this
            // synthetic path it immediately does, so the observable is that it was entered at
            // all, not that it is still in force at the end.
            boolean probedAgain = false;
            double peakPacing = settledPacing;
            int wireSize = wireSize();
            for (int round = 0; round < 90; round++) {
                // A stalled sender is pressed against the window: it fills whatever the
                // controller will allow and still has more waiting.
                List<RakDatagramPacket> flight = new ArrayList<>();
                do {
                    flight.add(send(window, live, sequence++, now, false, DEEP_QUEUE_BYTES));
                } while (flight.size() < 256 && window.canSendBoundedRecovery(wireSize, now));
                now += RTT_MILLIS;
                for (RakDatagramPacket datagram : flight) {
                    window.onAck(now, datagram, RTT_MILLIS);
                }
                probedAgain |= window.isModelStartup();
                peakPacing = Math.max(peakPacing, window.getModelPacingRateBytesPerMillis());
            }

            Assertions.assertTrue(probedAgain,
                    "a sender backlogged for ninety rounds with nothing lost never went back to "
                            + "probing, so it can only climb at the in-flight headroom and a "
                            + "stalled queue takes seconds to drain");
            double observedPeak = peakPacing;
            Assertions.assertTrue(observedPeak > settledPacing,
                    () -> "went back to probing but never paced above the settled "
                            + settledPacing + " B/ms (peak " + observedPeak + " B/ms)");
        } finally {
            for (RakDatagramPacket datagram : live) {
                if (datagram.refCnt() > 0) {
                    datagram.release();
                }
            }
            window.close();
        }
    }

    @Test
    public void aSenderThatKeepsRunningDryIsLeftAlone() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> live = new ArrayList<>();
        try {
            long now = 100L;
            int sequence = 0;
            for (int round = 0; round < 60; round++) {
                List<RakDatagramPacket> flight = new ArrayList<>();
                for (int i = 0; i < 8; i++) {
                    flight.add(send(window, live, sequence++, now, i == 7));
                }
                now += RTT_MILLIS;
                for (RakDatagramPacket datagram : flight) {
                    window.onAck(now, datagram, RTT_MILLIS);
                }
            }
            Assertions.assertFalse(window.isModelStartup(), "the sender never left startup");

            // Same traffic and the same duration as the backlogged case; the only difference is
            // that the queue still empties on the last datagram of every burst.
            boolean sawStartup = false;
            for (int round = 0; round < 90; round++) {
                List<RakDatagramPacket> flight = new ArrayList<>();
                for (int i = 0; i < 8; i++) {
                    flight.add(send(window, live, sequence++, now, i == 7));
                }
                now += RTT_MILLIS;
                for (RakDatagramPacket datagram : flight) {
                    window.onAck(now, datagram, RTT_MILLIS);
                }
                sawStartup |= window.isModelStartup();
            }

            Assertions.assertFalse(sawStartup,
                    "an application that keeps running dry is not backlogged and must not be "
                            + "pushed back into startup");
        } finally {
            for (RakDatagramPacket datagram : live) {
                if (datagram.refCnt() > 0) {
                    datagram.release();
                }
            }
            window.close();
        }
    }

    private static RakDatagramPacket send(RakSlidingWindow window, List<RakDatagramPacket> live,
                                          int sequence, long now, boolean applicationDrained) {
        return send(window, live, sequence, now, applicationDrained, 0);
    }

    private static RakDatagramPacket send(RakSlidingWindow window, List<RakDatagramPacket> live,
                                          int sequence, long now, boolean applicationDrained,
                                          int queuedBytes) {
        RakDatagramPacket datagram = datagram();
        datagram.setSequenceIndex(sequence);
        datagram.setSendOrdinal(sequence);
        datagram.setSendTime(now);
        window.onReliableSend(datagram, applicationDrained, queuedBytes);
        live.add(datagram);
        return datagram;
    }

    private static int wireSize() {
        RakDatagramPacket datagram = datagram();
        try {
            return datagram.getSize();
        } finally {
            datagram.release();
        }
    }

    private static RakDatagramPacket datagram() {
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setReliability(RakReliability.RELIABLE);
        packet.setBuffer(Unpooled.buffer(1_000).writeZero(1_000));
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        if (!datagram.tryAddPacket(packet, MTU)) {
            packet.release();
            datagram.release();
            throw new AssertionError("test datagram does not fit MTU");
        }
        return datagram;
    }
}
