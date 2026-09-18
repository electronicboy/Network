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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.EventLoop;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.util.ReferenceCountUtil;

import java.nio.channels.ClosedChannelException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;

/** Coalesces one child channel's cross-event-loop writes into bounded fair parent-loop turns. */
final class RakChildWriteHandoff {
    static final int MAX_WRITES_PER_TURN = 64;
    static final int MAX_BYTES_PER_TURN = 64 * 1024;

    private final EventLoop parentEventLoop;
    private final Function<Object, ChannelFuture> write;
    private final Runnable flush;
    private final BooleanSupplier acceptingWrites;
    private final IntSupplier lowWaterMark;
    private final IntSupplier highWaterMark;
    private final IntSupplier maximumPendingBytes;
    private final Consumer<Boolean> writabilityChanged;
    private final Consumer<Throwable> failed;
    private final Runnable emptyQueueObserved;
    private final Queue<PendingWrite> pendingWrites = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean drainScheduled = new AtomicBoolean();
    private final Runnable drainTask = this::drain;

    private long pendingBytes;
    private boolean writable = true;
    private volatile boolean closed;

    RakChildWriteHandoff(EventLoop parentEventLoop, Function<Object, ChannelFuture> write, Runnable flush,
                         BooleanSupplier acceptingWrites, IntSupplier lowWaterMark, IntSupplier highWaterMark,
                         IntSupplier maximumPendingBytes,
                         Consumer<Boolean> writabilityChanged, Consumer<Throwable> failed) {
        this(parentEventLoop, write, flush, acceptingWrites, lowWaterMark, highWaterMark, maximumPendingBytes,
                writabilityChanged, failed, null);
    }

    RakChildWriteHandoff(EventLoop parentEventLoop, Function<Object, ChannelFuture> write, Runnable flush,
                         BooleanSupplier acceptingWrites, IntSupplier lowWaterMark, IntSupplier highWaterMark,
                         IntSupplier maximumPendingBytes, Consumer<Boolean> writabilityChanged,
                         Consumer<Throwable> failed, Runnable emptyQueueObserved) {
        this.parentEventLoop = parentEventLoop;
        this.write = write;
        this.flush = flush;
        this.acceptingWrites = acceptingWrites;
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
        this.maximumPendingBytes = maximumPendingBytes;
        this.writabilityChanged = writabilityChanged;
        this.failed = failed;
        this.emptyQueueObserved = emptyQueueObserved;
    }

    void enqueue(Object message) throws ClosedChannelException {
        if (this.closed || !this.acceptingWrites.getAsBoolean()) {
            throw new ClosedChannelException();
        }

        Object retained = ReferenceCountUtil.retain(message);
        int size = messageSize(message);
        try {
            this.reservePendingBytes(size);
        } catch (RuntimeException throwable) {
            ReferenceCountUtil.release(retained);
            this.fail(throwable);
            throw throwable;
        }
        this.pendingWrites.offer(new PendingWrite(retained, size));

        if (this.closed || !this.acceptingWrites.getAsBoolean()) {
            this.scheduleDrain(false);
            throw new ClosedChannelException();
        }
        this.scheduleDrain(true);
    }

    synchronized int pendingBytes() {
        return (int) Math.min(Integer.MAX_VALUE, this.pendingBytes);
    }

    void close() {
        this.closed = true;
        this.scheduleDrain(false);
    }

    private void scheduleDrain(boolean propagateRejection) {
        if (!this.drainScheduled.compareAndSet(false, true)) {
            return;
        }
        this.executeDrain(propagateRejection);
    }

    private void executeDrain(boolean propagateRejection) {
        try {
            this.parentEventLoop.execute(this.drainTask);
        } catch (Throwable throwable) {
            this.drainScheduled.set(false);
            this.fail(throwable);
            if (propagateRejection) {
                throw throwable;
            }
        }
    }

    private void drain() {
        int drained = 0;
        int drainedBytes = 0;
        boolean wrote = false;
        Throwable failure = null;
        while (drained < MAX_WRITES_PER_TURN) {
            PendingWrite next = this.pendingWrites.peek();
            if (next == null) {
                break;
            }
            // One oversized application write is indivisible here, but once a turn owns an entry it must not pull
            // another entry that would take the captured payload total beyond the byte fairness budget.
            if (drained > 0 && (long) drainedBytes + next.size > MAX_BYTES_PER_TURN) {
                break;
            }
            PendingWrite pending = this.pendingWrites.poll();
            if (pending == null) {
                continue;
            }
            drained++;
            drainedBytes = saturatingAdd(drainedBytes, pending.size);
            this.releasePendingBytes(pending.size);
            if (this.closed || !this.acceptingWrites.getAsBoolean()) {
                ReferenceCountUtil.release(pending.message);
                continue;
            }
            try {
                // Invocation transfers ownership to the pipeline. A failed future or synchronous pipeline failure
                // must not cause this handoff to release the current message a second time.
                ChannelFuture future = this.write.apply(pending.message);
                wrote = true;
                // A null future denotes a write made with the pipeline's reusable void promise. Such failures are
                // propagated through exceptionCaught instead of being observed by this handoff.
                if (future == null) {
                    continue;
                }
                if (future.isDone()) {
                    if (!future.isSuccess()) {
                        failure = future.cause();
                        break;
                    }
                } else {
                    future.addListener(completed -> {
                        if (!completed.isSuccess()) {
                            this.fail(completed.cause());
                        }
                    });
                }
            } catch (Throwable throwable) {
                failure = throwable;
                break;
            }
        }

        if (wrote) {
            try {
                this.flush.run();
            } catch (Throwable throwable) {
                failure = throwable;
            }
        }
        if (failure != null) {
            this.fail(failure);
            return;
        }
        if (this.closed || !this.acceptingWrites.getAsBoolean()) {
            this.releasePendingWrites();
            this.drainScheduled.set(false);
            return;
        }
        if (!this.pendingWrites.isEmpty()) {
            // Keep the scheduled token while placing the next bounded turn at the event-loop tail. Other children,
            // timers, ACK processing, and socket reads therefore get a chance to run between large write batches.
            this.executeDrain(false);
            return;
        }

        if (this.emptyQueueObserved != null) {
            this.emptyQueueObserved.run();
        }
        this.drainScheduled.set(false);
        // Close the producer race between the empty check and clearing the scheduled token.
        if (!this.pendingWrites.isEmpty()) {
            this.scheduleDrain(false);
        }
    }

    private void fail(Throwable throwable) {
        boolean notify = !this.closed;
        this.closed = true;
        this.releasePendingWrites();
        this.drainScheduled.set(false);
        if (notify) {
            this.failed.accept(throwable);
        }
    }

    private void releasePendingWrites() {
        PendingWrite pending;
        while ((pending = this.pendingWrites.poll()) != null) {
            this.releasePendingBytes(pending.size);
            ReferenceCountUtil.release(pending.message);
        }
    }

    private synchronized void reservePendingBytes(int size) {
        long updated = this.pendingBytes > Long.MAX_VALUE - size ? Long.MAX_VALUE : this.pendingBytes + size;
        int maximum = this.maximumPendingBytes.getAsInt();
        if (maximum > 0 && updated > maximum) {
            throw new PendingWriteQueueFullException(maximum);
        }
        this.pendingBytes = updated;
        this.updateWritability(updated);
    }

    private synchronized void releasePendingBytes(int size) {
        this.pendingBytes = Math.max(0L, this.pendingBytes - size);
        this.updateWritability(this.pendingBytes);
    }

    private void updateWritability(long queuedBytes) {
        if (queuedBytes > Math.max(1, this.highWaterMark.getAsInt())) {
            if (this.writable) {
                this.writable = false;
                this.notifyWritability(false);
            }
        } else if (queuedBytes == 0L || queuedBytes < Math.max(0, this.lowWaterMark.getAsInt())) {
            if (!this.writable) {
                this.writable = true;
                this.notifyWritability(true);
            }
        }
    }

    private void notifyWritability(boolean writable) {
        try {
            // Advisory backpressure must never compromise ownership/accounting when the child loop is closing or
            // rejects the corresponding channelWritabilityChanged task.
            this.writabilityChanged.accept(writable);
        } catch (Throwable ignored) {
            // The hard pending-byte ceiling and queue telemetry remain authoritative.
        }
    }

    private static int messageSize(Object message) {
        int readableBytes;
        if (message instanceof ByteBufHolder) {
            readableBytes = ((ByteBufHolder) message).content().readableBytes();
        } else if (message instanceof ByteBuf) {
            readableBytes = ((ByteBuf) message).readableBytes();
        } else {
            readableBytes = 0;
        }
        // Empty/control messages must still contribute a bounded queue unit.
        return Math.max(1, readableBytes);
    }

    private static int saturatingAdd(int first, int second) {
        return (int) Math.min(Integer.MAX_VALUE, (long) first + second);
    }

    private static final class PendingWrite {
        private final Object message;
        private final int size;

        private PendingWrite(Object message, int size) {
            this.message = message;
            this.size = size;
        }
    }

    static final class PendingWriteQueueFullException extends ChannelException {
        private PendingWriteQueueFullException(int maximumPendingBytes) {
            super("RakNet parent handoff exceeds " + maximumPendingBytes + " queued bytes");
        }
    }
}
