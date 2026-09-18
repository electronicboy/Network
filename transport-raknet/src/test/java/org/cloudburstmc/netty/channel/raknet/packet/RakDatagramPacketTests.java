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

package org.cloudburstmc.netty.channel.raknet.packet;

import io.netty.buffer.Unpooled;
import org.cloudburstmc.netty.channel.raknet.RakConstants;
import org.cloudburstmc.netty.channel.raknet.RakReliability;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RakDatagramPacketTests {

    @Test
    public void packetListMutationsMaintainCachedSizeAcrossRecycle() {
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        EncapsulatedPacket first = packet(100);
        EncapsulatedPacket second = packet(200);
        EncapsulatedPacket replacement = packet(50);
        try {
            int header = RakConstants.RAKNET_DATAGRAM_HEADER_SIZE;
            Assertions.assertEquals(header, datagram.getSize());

            datagram.getPackets().add(first.retain());
            Assertions.assertEquals(header + first.getSize(), datagram.getSize());

            datagram.getPackets().add(second.retain());
            Assertions.assertEquals(header + first.getSize() + second.getSize(), datagram.getSize());

            EncapsulatedPacket replaced = datagram.getPackets().set(0, replacement.retain());
            replaced.release();
            Assertions.assertEquals(header + replacement.getSize() + second.getSize(), datagram.getSize());

            datagram.getPackets().remove(1).release();
            Assertions.assertEquals(header + replacement.getSize(), datagram.getSize());
        } finally {
            datagram.release();
            first.release();
            second.release();
            replacement.release();
        }

        RakDatagramPacket recycled = RakDatagramPacket.newInstance();
        try {
            Assertions.assertEquals(RakConstants.RAKNET_DATAGRAM_HEADER_SIZE, recycled.getSize());
            Assertions.assertTrue(recycled.getPackets().isEmpty());
        } finally {
            recycled.release();
        }
    }

    private static EncapsulatedPacket packet(int payloadSize) {
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setReliability(RakReliability.RELIABLE_ORDERED);
        packet.setBuffer(Unpooled.buffer(payloadSize).writeZero(payloadSize));
        return packet;
    }
}
