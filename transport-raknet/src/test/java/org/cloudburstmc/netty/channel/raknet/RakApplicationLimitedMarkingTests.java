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
 * A sender that trickles must not teach the delivery estimator that the path is slow.
 *
 * <p>The only thing the session codec can observe per datagram is whether its send queue happens
 * to be empty at that instant, so in a burst of {@code b} datagrams only the last one looks
 * application limited. If the leading {@code b - 1} are taken as genuine measurements they
 * describe the size of the burst rather than the capacity of the path, and the windowed maximum
 * follows the application down.</p>
 */
public class RakApplicationLimitedMarkingTests {
    private static final int MTU = 1_200;
    private static final int SATURATED_FLIGHT = 16;
    private static final int TRICKLE_BURST = 3;
    private static final long RTT_MILLIS = 20L;

    @Test
    public void trickleAfterSaturationDoesNotDrainTheDeliveryEstimate() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> live = new ArrayList<>();
        try {
            long now = 100L;
            int sequence = 0;

            // The application always has more queued, so nothing is application limited and the
            // estimator gets to see what the path can actually carry.
            for (int round = 0; round < 12; round++) {
                List<RakDatagramPacket> flight = new ArrayList<>();
                for (int i = 0; i < SATURATED_FLIGHT; i++) {
                    flight.add(send(window, live, sequence++, now, false));
                }
                now += RTT_MILLIS;
                for (RakDatagramPacket datagram : flight) {
                    window.onAck(now, datagram, RTT_MILLIS);
                }
            }
            double saturated = window.getModelBandwidthBytesPerMillis();
            Assertions.assertTrue(saturated > 0D, "the saturated phase learned no bandwidth at all");

            // Now the application drops to a trickle: a small burst every 25 ms, with only the
            // final datagram of each burst finding the send queue empty. The path is unchanged
            // and nothing is lost.
            for (int burst = 0; burst < 40; burst++) {
                List<RakDatagramPacket> flight = new ArrayList<>();
                for (int i = 0; i < TRICKLE_BURST; i++) {
                    flight.add(send(window, live, sequence++, now, i == TRICKLE_BURST - 1));
                }
                now += RTT_MILLIS;
                for (RakDatagramPacket datagram : flight) {
                    window.onAck(now, datagram, RTT_MILLIS);
                }
                now += 5L;
            }

            double trickled = window.getModelBandwidthBytesPerMillis();
            Assertions.assertTrue(trickled >= saturated * 0.9D,
                    () -> "an idle sender talked the estimator down from " + saturated + " B/ms to "
                            + trickled + " B/ms without a single loss: the leading datagrams of each "
                            + "burst were recorded as measurements of the path");
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
        RakDatagramPacket datagram = datagram();
        datagram.setSequenceIndex(sequence);
        datagram.setSendOrdinal(sequence);
        datagram.setSendTime(now);
        window.onReliableSend(datagram, applicationDrained);
        live.add(datagram);
        return datagram;
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
