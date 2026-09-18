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

public class RakSlidingWindowBoundedTests {
    private static final int MTU = 1_200;

    @Test
    public void separatesLogicalOutstandingFromPhysicalFlight() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        RakDatagramPacket datagram = datagram(200);
        try {
            window.onReliableSend(datagram);
            Assertions.assertEquals(datagram.getSize(), window.getUnackedBytes());
            Assertions.assertEquals(datagram.getSize(), window.getBytesInFlight());

            window.onBoundedLoss(datagram, 0L);
            Assertions.assertEquals(datagram.getSize(), window.getUnackedBytes());
            Assertions.assertEquals(0, window.getBytesInFlight());

            window.onBoundedRetransmit(datagram, false);
            Assertions.assertEquals(datagram.getSize(), window.getBytesInFlight());
            datagram.markRetransmitted();
            window.onAck(1_000L, datagram, 1L);

            Assertions.assertEquals(0, window.getUnackedBytes());
            Assertions.assertEquals(0, window.getBytesInFlight());
        } finally {
            datagram.release();
        }
    }

    @Test
    public void appliesKarnAndDoesNotSampleRetransmittedAcknowledgements() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        RakDatagramPacket original = datagram(100);
        RakDatagramPacket retransmitted = datagram(100);
        try {
            original.setSequenceIndex(0);
            original.setSendOrdinal(0L);
            original.setSendTime(100L);
            window.onReliableSend(original);
            window.onAck(200L, original, 1L);
            Assertions.assertEquals(100.0D, window.getRTT());

            retransmitted.setSequenceIndex(1);
            retransmitted.setSendOrdinal(1L);
            retransmitted.setSendTime(300L);
            window.onReliableSend(retransmitted);
            window.onBoundedLoss(retransmitted, 1L);
            window.onBoundedRetransmit(retransmitted, false);
            retransmitted.setSendTime(500L);
            retransmitted.markRetransmitted();
            window.onAck(1_500L, retransmitted, 2L);

            Assertions.assertEquals(100.0D, window.getRTT(),
                    "ambiguous retransmission ACK must not inflate SRTT");
            Assertions.assertEquals(100.0D, window.getRttDeviation());
        } finally {
            original.release();
            retransmitted.release();
        }
    }

    @Test
    public void isolatedNacksDoNotCollapseWindowDuringRecoveryEpoch() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        RakDatagramPacket seed = datagram(100);
        RakDatagramPacket first = datagram(100);
        RakDatagramPacket second = datagram(100);
        try {
            seed.setSequenceIndex(0);
            seed.setSendOrdinal(0L);
            seed.setSendTime(0L);
            window.onReliableSend(seed);
            window.onAck(100L, seed, 1L);
            double initialWindow = window.getCongestionWindow();
            Assertions.assertEquals(12_000.0D, initialWindow);

            first.setSequenceIndex(1);
            first.setSendOrdinal(1L);
            second.setSequenceIndex(2);
            second.setSendOrdinal(2L);
            window.onReliableSend(first);
            window.onReliableSend(second);

            Assertions.assertTrue(window.onBoundedLoss(first, 2L));
            Assertions.assertFalse(window.onBoundedLoss(second, 2L));
            Assertions.assertEquals(initialWindow, window.getCongestionWindow(),
                    "isolated packet loss must not slow an otherwise healthy path");
            Assertions.assertTrue(window.isInRecovery());

            window.onAck(200L, first, 3L);
            Assertions.assertTrue(window.isInRecovery(), "pre-boundary ACK is not a new recovery epoch");
            second.setSendOrdinal(3L);
            second.markRetransmitted();
            window.onAck(300L, second, 4L);
            Assertions.assertFalse(window.isInRecovery(), "post-boundary ACK ends the epoch");
        } finally {
            seed.release();
            first.release();
            second.release();
        }
    }

    @Test
    public void permitsOnlyOneCwndChargedProbe() {
        RakSlidingWindow window = new RakSlidingWindow(MTU);
        RakDatagramPacket first = datagram(1_160);
        RakDatagramPacket blocker = datagram(1_160);
        RakDatagramPacket probe = datagram(100);
        try {
            window.onPersistentCongestion();
            window.onReliableSend(first);
            window.onReliableSend(blocker);
            window.onReliableSend(probe);
            window.onBoundedLoss(first, 1L);
            window.onBoundedLoss(probe, 1L);
            window.onBoundedRetransmit(first, false);

            Assertions.assertFalse(window.canSendBoundedRecovery(probe.getSize()));
            Assertions.assertTrue(window.canSendBoundedProbe(probe.getSize()));
            window.onBoundedRetransmit(probe, true);
            Assertions.assertFalse(window.canSendBoundedProbe(probe.getSize()));

            probe.markRetransmitted();
            window.onAck(100L, probe, 2L);
            Assertions.assertEquals(0, window.getRecoveryProbeBytes());
        } finally {
            first.release();
            blocker.release();
            probe.release();
        }
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
