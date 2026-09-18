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

package org.cloudburstmc.netty.channel.raknet.packet;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.internal.ObjectPool;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;

import static org.cloudburstmc.netty.channel.raknet.RakConstants.*;

public class RakDatagramPacket extends AbstractReferenceCounted {

    private static final ObjectPool<RakDatagramPacket> RECYCLER = ObjectPool.newPool(RakDatagramPacket::new);

    private final ObjectPool.Handle<RakDatagramPacket> handle;
    private final List<EncapsulatedPacket> packets = new PacketList();
    private int size = RAKNET_DATAGRAM_HEADER_SIZE;
    private byte flags = FLAG_VALID | FLAG_NEEDS_B_AND_AS;
    private long sendTime;
    private long nextSend;
    private int sequenceIndex = -1;
    private int retransmissionCount;
    private long sendOrdinal = -1;
    private boolean reliableOutstanding;
    private boolean inFlight;
    private boolean retransmissionPending;
    private boolean recoveryProbe;
    private long deliveredBytesAtSend;
    private long deliveredTimeAtSend;
    private long firstSendTime;
    private long modelSendTime;
    private int modelTxInFlight;
    private boolean modelSampleValid;
    private boolean modelAppLimited;
    private boolean modelLossClassified;

    public static RakDatagramPacket newInstance() {
        return RECYCLER.get();
    }

    private RakDatagramPacket(ObjectPool.Handle<RakDatagramPacket> handle) {
        this.handle = handle;
    }

    @Override
    public RakDatagramPacket retain() {
        super.retain();
        return this;
    }

    @Override
    public RakDatagramPacket retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public RakDatagramPacket touch(Object hint) {
        for (EncapsulatedPacket packet : this.packets) {
            packet.touch(hint);
        }
        return this;
    }

    public boolean tryAddPacket(EncapsulatedPacket packet, int mtu) {
        if (this.size + packet.getSize() > mtu - RAKNET_DATAGRAM_HEADER_SIZE) {
            return false;
        }

        this.packets.add(packet);
        if (packet.isSplit()) {
            flags |= FLAG_CONTINUOUS_SEND;
        }
        return true;
    }

    @Override
    public boolean release() {
        return super.release();
    }

    @Override
    protected void deallocate() {
        for (EncapsulatedPacket packet : this.packets) {
            packet.release();
        }
        this.packets.clear();
        this.size = RAKNET_DATAGRAM_HEADER_SIZE;
        this.flags = FLAG_VALID | FLAG_NEEDS_B_AND_AS;
        this.sendTime = 0;
        this.nextSend = 0;
        this.sequenceIndex = -1;
        this.retransmissionCount = 0;
        this.sendOrdinal = -1;
        this.reliableOutstanding = false;
        this.inFlight = false;
        this.retransmissionPending = false;
        this.recoveryProbe = false;
        this.deliveredBytesAtSend = 0;
        this.deliveredTimeAtSend = 0;
        this.firstSendTime = 0;
        this.modelSendTime = 0;
        this.modelTxInFlight = 0;
        this.modelSampleValid = false;
        this.modelAppLimited = false;
        this.modelLossClassified = false;
        setRefCnt(1);
        this.handle.recycle(this);
    }

    public int getSize() {
        return this.size;
    }

    public List<EncapsulatedPacket> getPackets() {
        return this.packets;
    }

    public byte getFlags() {
        return this.flags;
    }

    public void setFlags(byte flags) {
        this.flags = flags;
    }

    public long getSendTime() {
        return sendTime;
    }

    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }

    public long getNextSend() {
        return this.nextSend;
    }

    public void setNextSend(long nextSend) {
        this.nextSend = nextSend;
    }

    public int getSequenceIndex() {
        return this.sequenceIndex;
    }

    public void setSequenceIndex(int sequenceIndex) {
        this.sequenceIndex = sequenceIndex;
    }

    public int getRetransmissionCount() {
        return this.retransmissionCount;
    }

    /**
     * Marks this datagram as retransmitted and returns the one-based attempt number.
     */
    public int markRetransmitted() {
        return ++this.retransmissionCount;
    }

    /** Restores the attempt counter when a retransmission cannot be handed to the channel. */
    public void restoreRetransmissionCount(int retransmissionCount) {
        if (retransmissionCount < 0 || retransmissionCount > this.retransmissionCount) {
            throw new IllegalArgumentException("Invalid retransmission count rollback");
        }
        this.retransmissionCount = retransmissionCount;
    }

    public long getSendOrdinal() {
        return this.sendOrdinal;
    }

    public void setSendOrdinal(long sendOrdinal) {
        this.sendOrdinal = sendOrdinal;
    }

    public boolean isReliableOutstanding() {
        return this.reliableOutstanding;
    }

    public void setReliableOutstanding(boolean reliableOutstanding) {
        this.reliableOutstanding = reliableOutstanding;
    }

    public boolean isInFlight() {
        return this.inFlight;
    }

    public void setInFlight(boolean inFlight) {
        this.inFlight = inFlight;
    }

    public boolean isRetransmissionPending() {
        return this.retransmissionPending;
    }

    public void setRetransmissionPending(boolean retransmissionPending) {
        this.retransmissionPending = retransmissionPending;
    }

    public boolean isRecoveryProbe() {
        return this.recoveryProbe;
    }

    public void setRecoveryProbe(boolean recoveryProbe) {
        this.recoveryProbe = recoveryProbe;
    }

    public long getDeliveredBytesAtSend() {
        return this.deliveredBytesAtSend;
    }

    public long getDeliveredTimeAtSend() {
        return this.deliveredTimeAtSend;
    }

    public long getFirstSendTime() {
        return this.firstSendTime;
    }

    public long getModelSendTime() {
        return this.modelSendTime;
    }

    public int getModelTxInFlight() {
        return this.modelTxInFlight;
    }

    public boolean isModelSampleValid() {
        return this.modelSampleValid;
    }

    public boolean isModelAppLimited() {
        return this.modelAppLimited;
    }

    public boolean isModelLossClassified() {
        return this.modelLossClassified;
    }

    public void setModelLossClassified(boolean modelLossClassified) {
        this.modelLossClassified = modelLossClassified;
    }

    public void setModelSendState(long deliveredBytesAtSend, long deliveredTimeAtSend, long firstSendTime,
                                  long modelSendTime, int modelTxInFlight, boolean modelAppLimited) {
        this.deliveredBytesAtSend = deliveredBytesAtSend;
        this.deliveredTimeAtSend = deliveredTimeAtSend;
        this.firstSendTime = firstSendTime;
        this.modelSendTime = modelSendTime;
        this.modelTxInFlight = modelTxInFlight;
        this.modelSampleValid = true;
        this.modelAppLimited = modelAppLimited;
        this.modelLossClassified = false;
    }

    public void clearModelSendState() {
        this.deliveredBytesAtSend = 0;
        this.deliveredTimeAtSend = 0;
        this.firstSendTime = 0;
        this.modelSendTime = 0;
        this.modelTxInFlight = 0;
        this.modelSampleValid = false;
        this.modelAppLimited = false;
        this.modelLossClassified = false;
    }

    @Override
    public String toString() {
        return "RakDatagramPacket{" +
                "handle=" + handle +
                ", packets=" + packets +
                ", flags=" + flags +
                ", sendTime=" + sendTime +
                ", nextSend=" + nextSend +
                ", sequenceIndex=" + sequenceIndex +
                ", retransmissionCount=" + retransmissionCount +
                ", sendOrdinal=" + sendOrdinal +
                ", reliableOutstanding=" + reliableOutstanding +
                ", inFlight=" + inFlight +
                ", retransmissionPending=" + retransmissionPending +
                ", recoveryProbe=" + recoveryProbe +
                '}';
    }

    /** Keeps the cached datagram size correct for callers that mutate the exposed packet list. */
    private final class PacketList extends AbstractList<EncapsulatedPacket> implements RandomAccess {
        private final ArrayList<EncapsulatedPacket> delegate = new ArrayList<>();

        @Override
        public EncapsulatedPacket get(int index) {
            return this.delegate.get(index);
        }

        @Override
        public int size() {
            return this.delegate.size();
        }

        @Override
        public void add(int index, EncapsulatedPacket packet) {
            this.delegate.add(index, packet);
            RakDatagramPacket.this.size += packet.getSize();
            this.modCount++;
        }

        @Override
        public EncapsulatedPacket set(int index, EncapsulatedPacket packet) {
            EncapsulatedPacket replaced = this.delegate.set(index, packet);
            RakDatagramPacket.this.size += packet.getSize() - replaced.getSize();
            return replaced;
        }

        @Override
        public EncapsulatedPacket remove(int index) {
            EncapsulatedPacket removed = this.delegate.remove(index);
            RakDatagramPacket.this.size -= removed.getSize();
            this.modCount++;
            return removed;
        }

        @Override
        public void clear() {
            if (this.delegate.isEmpty()) {
                return;
            }
            this.delegate.clear();
            RakDatagramPacket.this.size = RAKNET_DATAGRAM_HEADER_SIZE;
            this.modCount++;
        }
    }
}
