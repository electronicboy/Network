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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

public class RakBoundedRecoveryTests {

    @Test
    public void backsOffOnePtoToCapAndAckProgressResetsIt() {
        AtomicLong clock = new AtomicLong();
        RakBoundedRecovery recovery = new RakBoundedRecovery(clock::get, () -> 99L);
        RakSlidingWindow window = new RakSlidingWindow(1_200);
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        RakDatagramPacket acknowledgedLaterDatagram = RakDatagramPacket.newInstance();
        try {
            window.onReliableSend(datagram);
            recovery.onReliableSend(window, datagram);
            Assertions.assertEquals(1_099L, recovery.getNextProbeAtMillis());

            clock.set(500L);
            Assertions.assertTrue(recovery.scheduleNack(datagram));
            Assertions.assertFalse(recovery.scheduleNack(datagram), "duplicate NACK must be idempotent");
            recovery.onNackRetransmission(window, datagram, Collections.singletonList(datagram));
            Assertions.assertEquals(1_599L, recovery.getNextProbeAtMillis(),
                    "a transmitted NACK recovery gets a fresh per-attempt loss deadline");

            clock.set(1_598L);
            Assertions.assertFalse(recovery.isProbeDue(window));
            clock.set(1_599L);
            Assertions.assertTrue(recovery.isProbeDue(window));
            recovery.onProbeSent(window, datagram, Collections.singletonList(datagram));
            Assertions.assertEquals(1, recovery.getPtoBackoff());
            Assertions.assertEquals(3_698L, recovery.getNextProbeAtMillis());
            clock.set(1_609L);
            Assertions.assertFalse(recovery.isProbeDue(window),
                    "a PTO handoff is paced by backed-off RTO rather than repeating on the next 10 ms tick");

            clock.set(3_698L);
            recovery.onProbeSent(window, datagram, Collections.singletonList(datagram));
            Assertions.assertEquals(7_797L, recovery.getNextProbeAtMillis());
            clock.set(7_797L);
            recovery.onProbeSent(window, datagram, Collections.singletonList(datagram));
            Assertions.assertEquals(15_896L, recovery.getNextProbeAtMillis());
            Assertions.assertEquals(RakBoundedRecovery.MAX_BACKED_OFF_RTO_MILLIS,
                    recovery.getEffectiveRtoMillis(window));

            clock.set(20_000L);
            recovery.onAcknowledgementProgress(window, acknowledgedLaterDatagram,
                    Collections.singletonList(datagram));
            Assertions.assertEquals(0, recovery.getPtoBackoff());
            Assertions.assertEquals(15_896L, recovery.getNextProbeAtMillis(),
                    "ACK progress for another datagram must not postpone this attempt's existing deadline");
            Assertions.assertTrue(recovery.isProbeDue(window));

            recovery.close();
            Assertions.assertEquals(-1L, recovery.getNextProbeAtMillis());
            Assertions.assertFalse(recovery.isProbeDue(window));
        } finally {
            datagram.release();
            acknowledgedLaterDatagram.release();
        }
    }

    @Test
    public void jitterIsAlwaysNonNegativeAndBelowTenPercent() {
        AtomicLong clock = new AtomicLong(10_000L);
        RakSlidingWindow window = new RakSlidingWindow(1_200);
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        try {
            window.onReliableSend(datagram);
            RakBoundedRecovery recovery = new RakBoundedRecovery(clock::get, () -> Long.MIN_VALUE);
            recovery.onReliableSend(window, datagram);

            long delay = recovery.getNextProbeAtMillis() - clock.get();
            Assertions.assertTrue(delay >= 1_000L);
            Assertions.assertTrue(delay < 1_100L);
        } finally {
            datagram.release();
        }
    }

    @Test
    public void nackFlushBudgetIsTwoDatagramsAndTwoMtus() {
        RakBoundedRecovery.FlushBudget budget = RakBoundedRecovery.nackFlushBudget(1_200);

        Assertions.assertTrue(budget.canConsume(1_200));
        budget.consume(1_200);
        Assertions.assertTrue(budget.canConsume(1_200));
        budget.consume(1_200);
        Assertions.assertEquals(2, budget.getDatagrams());
        Assertions.assertFalse(budget.canConsume(1));

        RakBoundedRecovery.FlushBudget byteBudget = RakBoundedRecovery.nackFlushBudget(1_200);
        Assertions.assertFalse(byteBudget.canConsume(2_401));
    }
}
