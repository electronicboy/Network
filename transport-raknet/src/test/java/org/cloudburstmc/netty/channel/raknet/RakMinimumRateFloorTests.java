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
 * A session that trickles for a few minutes must still be able to absorb the next burst.
 *
 * <p>Games run a few minutes, and after the join burst there is rarely enough traffic to keep a
 * bandwidth estimate alive. The protocol assumes a player can carry 1.5 Mbit/s, so the window may
 * not shrink below what that implies while the path has given no reason to doubt it.</p>
 */
public class RakMinimumRateFloorTests {
    private static final int MTU = 1_200;
    private static final long RTT_MILLIS = 20L;
    /** 1.5 Mbit/s = 187.5 B/ms; with CWND_GAIN = 2.0 over a 20 ms path that is 7,500 bytes. */
    private static final double FLOOR_CWND_BYTES = 187.5D * RTT_MILLIS * 2.0D;

    @Test
    public void aSessionThatDemonstratedTheRateKeepsAWindowWorthIt() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> live = new ArrayList<>();
        try {
            long now = 100L;
            int sequence = 0;
            // The join burst: enough per round to leave startup and to demonstrate well over
            // 1.5 Mbit/s on this path.
            for (int round = 0; round < 60; round++) {
                List<RakDatagramPacket> flight = new ArrayList<>();
                for (int i = 0; i < 8; i++) {
                    flight.add(send(window, live, sequence++, now, i == 7, 1_000));
                }
                now += RTT_MILLIS;
                for (RakDatagramPacket datagram : flight) {
                    window.onAck(now, datagram, RTT_MILLIS);
                }
            }
            Assertions.assertFalse(window.isModelStartup(), "the session never left startup");

            // Then minutes of in-game trickle: small bursts, queue dry only on the last datagram
            // of each, which is all the codec can observe. That partial marking is what lets the
            // estimate decay in the first place - mark every ack application limited and it
            // simply freezes, which is not what production does.
            for (int round = 0; round < 400; round++) {
                List<RakDatagramPacket> flight = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    flight.add(send(window, live, sequence++, now, i == 2, 120));
                }
                now += RTT_MILLIS;
                for (RakDatagramPacket datagram : flight) {
                    window.onAck(now, datagram, RTT_MILLIS);
                }
            }

            double cwnd = window.getCongestionWindow();
            Assertions.assertTrue(cwnd >= FLOOR_CWND_BYTES * 0.95D,
                    () -> "a session that had already carried the assumed rate let its window "
                            + "decay to " + cwnd + " bytes while idle, under the " + FLOOR_CWND_BYTES
                            + " that 1.5 Mbit/s implies, so the next burst drains through that");
        } finally {
            release(live);
            window.close();
        }
    }

    @Test
    public void aSessionThatNeverReachedTheRateIsNotLifted() {
        // A long path where leaving startup does not require carrying 1.5 Mbit/s, so the session
        // settles having demonstrated far less. Assuming the rate anyway would only queue in the
        // network - which measurably costs goodput rather than saving it.
        final long slowRtt = 100L;
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        List<RakDatagramPacket> live = new ArrayList<>();
        try {
            long now = 100L;
            int sequence = 0;
            for (int round = 0; round < 80; round++) {
                List<RakDatagramPacket> flight = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    flight.add(send(window, live, sequence++, now, i == 4, 1_000));
                }
                now += slowRtt;
                for (RakDatagramPacket datagram : flight) {
                    window.onAck(now, datagram, slowRtt);
                }
            }
            Assertions.assertFalse(window.isModelStartup(), "the session never left startup");

            double demonstrated = window.getModelBandwidthBytesPerMillis();
            Assertions.assertTrue(demonstrated < 187.5D,
                    () -> "fixture must never reach the floor rate, but measured " + demonstrated);

            double cwnd = window.getCongestionWindow();
            double flooredWindow = 187.5D * slowRtt * 2.0D;
            Assertions.assertTrue(cwnd < flooredWindow * 0.9D,
                    () -> "a session that never carried 1.5 Mbit/s was still given a " + cwnd
                            + " byte window, near the " + flooredWindow + " that rate implies");
        } finally {
            release(live);
            window.close();
        }
    }

    private static void release(List<RakDatagramPacket> live) {
        for (RakDatagramPacket datagram : live) {
            if (datagram.refCnt() > 0) {
                datagram.release();
            }
        }
    }

    private static RakDatagramPacket send(RakSlidingWindow window, List<RakDatagramPacket> live,
                                          int sequence, long now, boolean applicationDrained,
                                          int payloadBytes) {
        RakDatagramPacket datagram = datagram(payloadBytes);
        datagram.setSequenceIndex(sequence);
        datagram.setSendOrdinal(sequence);
        datagram.setSendTime(now);
        window.onReliableSend(datagram, applicationDrained);
        live.add(datagram);
        return datagram;
    }

    private static RakDatagramPacket datagram(int payloadBytes) {
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setReliability(RakReliability.RELIABLE);
        packet.setBuffer(Unpooled.buffer(payloadBytes).writeZero(payloadBytes));
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        if (!datagram.tryAddPacket(packet, MTU)) {
            packet.release();
            datagram.release();
            throw new AssertionError("test datagram does not fit MTU");
        }
        return datagram;
    }
}
