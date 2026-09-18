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
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.EventLoop;
import io.netty.channel.ChannelFuture;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

public class RakChildWriteHandoffTests {
    @Test
    public void voidPromiseStyleWriteDrainsWithoutListenerRegistration() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger flushes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        RakChildWriteHandoff handoff = handoff(parent, message -> {
            writes.incrementAndGet();
            ReferenceCountUtil.release(message);
            return null;
        }, flushes, ignored -> { }, ignored -> failures.incrementAndGet(), 16, 32);
        try {
            ByteBuf buffer = Unpooled.buffer(8).writeZero(8);
            handoff.enqueue(buffer);
            buffer.release();
            parent.runPendingTasks();

            Assertions.assertEquals(1, writes.get());
            Assertions.assertEquals(1, flushes.get());
            Assertions.assertEquals(0, failures.get());
            Assertions.assertEquals(0, handoff.pendingBytes());
        } finally {
            handoff.close();
            parent.runPendingTasks();
            parent.finishAndReleaseAll();
        }
    }

    @Test
    public void largeChildQueueYieldsAfterBoundedTurnsAndPreservesOrder() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        List<Integer> written = new ArrayList<>();
        AtomicInteger flushes = new AtomicInteger();
        AtomicInteger marker = new AtomicInteger(-1);
        RakChildWriteHandoff handoff = handoff(parent, message -> {
            ByteBuf buffer = (ByteBuf) message;
            written.add(buffer.readInt());
            buffer.release();
            return parent.newSucceededFuture();
        }, flushes, ignored -> { }, ignored -> { }, 32, 64);
        try {
            for (int i = 0; i < 130; i++) {
                ByteBuf buffer = Unpooled.buffer(4).writeInt(i);
                handoff.enqueue(buffer);
                buffer.release();
            }
            parent.eventLoop().execute(() -> marker.set(written.size()));
            parent.runPendingTasks();

            Assertions.assertEquals(RakChildWriteHandoff.MAX_WRITES_PER_TURN, marker.get(),
                    "the first child turn yields to work already queued on the parent loop");
            Assertions.assertEquals(130, written.size());
            for (int i = 0; i < written.size(); i++) {
                Assertions.assertEquals(i, written.get(i));
            }
            Assertions.assertEquals(3, flushes.get());
            Assertions.assertEquals(0, handoff.pendingBytes());
        } finally {
            handoff.close();
            parent.runPendingTasks();
            parent.finishAndReleaseAll();
        }
    }

    @Test
    public void pendingBytesDriveWatermarksAndCloseReleasesQueuedOwnership() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        List<Boolean> writability = new ArrayList<>();
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger flushes = new AtomicInteger();
        RakChildWriteHandoff handoff = handoff(parent, message -> {
            writes.incrementAndGet();
            ReferenceCountUtil.release(message);
            return parent.newSucceededFuture();
        }, flushes, writability::add, ignored -> { }, 16, 32);
        ByteBuf first = Unpooled.buffer(20).writeZero(20);
        ByteBuf second = Unpooled.buffer(20).writeZero(20);
        try {
            handoff.enqueue(first);
            first.release();
            handoff.enqueue(second);
            second.release();
            Assertions.assertEquals(40, handoff.pendingBytes());
            Assertions.assertEquals(Arrays.asList(false), writability);

            handoff.close();
            parent.runPendingTasks();
            Assertions.assertEquals(0, writes.get());
            Assertions.assertEquals(0, flushes.get());
            Assertions.assertEquals(0, handoff.pendingBytes());
            Assertions.assertEquals(Arrays.asList(false, true), writability);
            Assertions.assertEquals(0, first.refCnt());
            Assertions.assertEquals(0, second.refCnt());
        } finally {
            ReferenceCountUtil.safeRelease(first);
            ReferenceCountUtil.safeRelease(second);
            parent.finishAndReleaseAll();
        }
    }

    @Test
    public void parentWriteFailureReleasesCurrentAndRemainingMessagesOnce() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger flushes = new AtomicInteger();
        List<ByteBuf> buffers = Arrays.asList(
                Unpooled.buffer(8).writeZero(8),
                Unpooled.buffer(8).writeZero(8),
                Unpooled.buffer(8).writeZero(8));
        AtomicInteger handed = new AtomicInteger();
        RakChildWriteHandoff handoff = handoff(parent, message -> {
            ReferenceCountUtil.release(message);
            if (handed.incrementAndGet() == 2) {
                return parent.newFailedFuture(new IllegalStateException("write failure"));
            }
            return parent.newSucceededFuture();
        }, flushes, ignored -> { }, cause -> failures.incrementAndGet(), 16, 32);
        try {
            for (ByteBuf buffer : buffers) {
                handoff.enqueue(buffer);
                buffer.release();
            }
            parent.runPendingTasks();

            Assertions.assertEquals(1, failures.get());
            Assertions.assertEquals(2, handed.get());
            Assertions.assertEquals(1, flushes.get(), "the successfully handed prefix is flushed once");
            Assertions.assertEquals(0, handoff.pendingBytes());
            buffers.forEach(buffer -> Assertions.assertEquals(0, buffer.refCnt()));
        } finally {
            buffers.forEach(ReferenceCountUtil::safeRelease);
            parent.finishAndReleaseAll();
        }
    }

    @Test
    public void twoChildrenInterleaveAtMessageTurnBoundary() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        List<String> order = new ArrayList<>();
        AtomicInteger aFlushes = new AtomicInteger();
        AtomicInteger bFlushes = new AtomicInteger();
        RakChildWriteHandoff first = handoff(parent, message -> {
            ByteBuf buffer = (ByteBuf) message;
            order.add("a" + buffer.readInt());
            buffer.release();
            return parent.newSucceededFuture();
        }, aFlushes, ignored -> { }, ignored -> { }, 32, 64);
        RakChildWriteHandoff second = handoff(parent, message -> {
            order.add("b");
            ReferenceCountUtil.release(message);
            return parent.newSucceededFuture();
        }, bFlushes, ignored -> { }, ignored -> { }, 32, 64);
        try {
            for (int i = 0; i < 130; i++) {
                ByteBuf buffer = Unpooled.buffer(4).writeInt(i);
                first.enqueue(buffer);
                buffer.release();
            }
            ByteBuf other = Unpooled.buffer(1).writeByte(1);
            second.enqueue(other);
            other.release();
            parent.runPendingTasks();

            Assertions.assertEquals("a63", order.get(63));
            Assertions.assertEquals("b", order.get(64));
            Assertions.assertEquals("a64", order.get(65));
            Assertions.assertEquals(3, aFlushes.get());
            Assertions.assertEquals(1, bFlushes.get());
        } finally {
            first.close();
            second.close();
            parent.runPendingTasks();
            parent.finishAndReleaseAll();
        }
    }

    @Test
    public void largeMessagesYieldAtByteTurnBoundary() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger marker = new AtomicInteger(-1);
        AtomicInteger flushes = new AtomicInteger();
        RakChildWriteHandoff handoff = handoff(parent, message -> {
            writes.incrementAndGet();
            ReferenceCountUtil.release(message);
            return parent.newSucceededFuture();
        }, flushes, ignored -> { }, ignored -> { }, 32, 64);
        try {
            for (int i = 0; i < 40; i++) {
                ByteBuf buffer = Unpooled.buffer(2_048).writeZero(2_048);
                handoff.enqueue(buffer);
                buffer.release();
            }
            parent.eventLoop().execute(() -> marker.set(writes.get()));
            parent.runPendingTasks();
            Assertions.assertEquals(32, marker.get());
            Assertions.assertEquals(40, writes.get());
            Assertions.assertEquals(2, flushes.get());
        } finally {
            handoff.close();
            parent.runPendingTasks();
            parent.finishAndReleaseAll();
        }
    }

    @Test
    public void nextLargeMessageCannotOvershootByteTurnBoundary() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger marker = new AtomicInteger(-1);
        AtomicInteger flushes = new AtomicInteger();
        RakChildWriteHandoff handoff = handoff(parent, message -> {
            writes.incrementAndGet();
            ReferenceCountUtil.release(message);
            return parent.newSucceededFuture();
        }, flushes, ignored -> { }, ignored -> { }, 32, 64);
        try {
            for (int i = 0; i < 3; i++) {
                ByteBuf buffer = Unpooled.buffer(40 * 1024).writeZero(40 * 1024);
                handoff.enqueue(buffer);
                buffer.release();
            }
            parent.eventLoop().execute(() -> marker.set(writes.get()));
            parent.runPendingTasks();

            Assertions.assertEquals(1, marker.get());
            Assertions.assertEquals(3, writes.get());
            Assertions.assertEquals(3, flushes.get());
        } finally {
            handoff.close();
            parent.runPendingTasks();
            parent.finishAndReleaseAll();
        }
    }

    @Test
    public void enqueueDuringScheduledClearWindowCannotBeStranded() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        CountDownLatch emptyObserved = new CountDownLatch(1);
        CountDownLatch allowClear = new CountDownLatch(1);
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger flushes = new AtomicInteger();
        RakChildWriteHandoff handoff = new RakChildWriteHandoff(parent.eventLoop(), message -> {
            writes.incrementAndGet();
            ReferenceCountUtil.release(message);
            return parent.newSucceededFuture();
        }, flushes::incrementAndGet, () -> true, () -> 16, () -> 32, () -> 1024,
                ignored -> { }, ignored -> { }, () -> {
                    emptyObserved.countDown();
                    await(allowClear);
                });
        Thread parentRunner = null;
        try {
            ByteBuf first = Unpooled.buffer(1).writeByte(1);
            handoff.enqueue(first);
            first.release();
            parentRunner = new Thread(parent::runPendingTasks, "handoff-parent-runner");
            parentRunner.start();
            Assertions.assertTrue(emptyObserved.await(5, TimeUnit.SECONDS));

            ByteBuf raced = Unpooled.buffer(1).writeByte(2);
            handoff.enqueue(raced);
            raced.release();
            allowClear.countDown();
            parentRunner.join(5_000L);
            parent.runPendingTasks();

            Assertions.assertEquals(2, writes.get());
            Assertions.assertEquals(2, flushes.get());
            Assertions.assertEquals(0, handoff.pendingBytes());
        } finally {
            allowClear.countDown();
            if (parentRunner != null) {
                parentRunner.join(5_000L);
            }
            handoff.close();
            parent.runPendingTasks();
            parent.finishAndReleaseAll();
        }
    }

    @Test
    public void initialAndTailSchedulingRejectionReleaseAllOwnership() throws Exception {
        assertSchedulingRejectionReleasesAll(0, 1);
        assertSchedulingRejectionReleasesAll(1, 130);
    }

    @Test
    public void closeWhileParentOwnsCurrentEntryDoesNotDoubleRelease() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        CountDownLatch currentOwned = new CountDownLatch(1);
        CountDownLatch finishWrite = new CountDownLatch(1);
        AtomicInteger flushes = new AtomicInteger();
        List<ByteBuf> buffers = Arrays.asList(
                Unpooled.buffer(8).writeZero(8), Unpooled.buffer(8).writeZero(8));
        RakChildWriteHandoff handoff = handoff(parent, message -> {
            currentOwned.countDown();
            await(finishWrite);
            ReferenceCountUtil.release(message);
            return parent.newSucceededFuture();
        }, flushes, ignored -> { }, ignored -> { }, 16, 32);
        Thread parentRunner = null;
        try {
            for (ByteBuf buffer : buffers) {
                handoff.enqueue(buffer);
                buffer.release();
            }
            parentRunner = new Thread(parent::runPendingTasks, "handoff-close-runner");
            parentRunner.start();
            Assertions.assertTrue(currentOwned.await(5, TimeUnit.SECONDS));
            handoff.close();
            finishWrite.countDown();
            parentRunner.join(5_000L);
            parent.runPendingTasks();

            Assertions.assertEquals(0, handoff.pendingBytes());
            buffers.forEach(buffer -> Assertions.assertEquals(0, buffer.refCnt()));
        } finally {
            finishWrite.countDown();
            if (parentRunner != null) {
                parentRunner.join(5_000L);
            }
            buffers.forEach(ReferenceCountUtil::safeRelease);
            parent.finishAndReleaseAll();
        }
    }

    @Test
    public void hardPendingByteCeilingFailsAndReclaimsQueue() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger flushes = new AtomicInteger();
        RakChildWriteHandoff handoff = new RakChildWriteHandoff(parent.eventLoop(), message -> {
            ReferenceCountUtil.release(message);
            return parent.newSucceededFuture();
        }, flushes::incrementAndGet, () -> true,
                () -> 8, () -> 16, () -> 32, ignored -> { }, ignored -> failures.incrementAndGet());
        ByteBuf first = Unpooled.buffer(20).writeZero(20);
        ByteBuf overflow = Unpooled.buffer(20).writeZero(20);
        try {
            handoff.enqueue(first);
            first.release();
            Assertions.assertThrows(RakChildWriteHandoff.PendingWriteQueueFullException.class,
                    () -> handoff.enqueue(overflow));
            overflow.release();
            parent.runPendingTasks();

            Assertions.assertEquals(1, failures.get());
            Assertions.assertEquals(0, handoff.pendingBytes());
            Assertions.assertEquals(0, first.refCnt());
            Assertions.assertEquals(0, overflow.refCnt());
        } finally {
            ReferenceCountUtil.safeRelease(first);
            ReferenceCountUtil.safeRelease(overflow);
            parent.finishAndReleaseAll();
        }
    }

    @Test
    public void rejectedWritabilityNotificationCannotCorruptAccountingOrOwnership() throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        AtomicInteger writes = new AtomicInteger();
        RakChildWriteHandoff handoff = new RakChildWriteHandoff(parent.eventLoop(), message -> {
            writes.incrementAndGet();
            ReferenceCountUtil.release(message);
            return parent.newSucceededFuture();
        }, () -> { }, () -> true, () -> 8, () -> 16, () -> 1024,
                ignored -> {
                    throw new RejectedExecutionException("child loop stopped");
                }, ignored -> { });
        ByteBuf first = Unpooled.buffer(20).writeZero(20);
        ByteBuf second = Unpooled.buffer(20).writeZero(20);
        try {
            handoff.enqueue(first);
            first.release();
            handoff.enqueue(second);
            second.release();
            Assertions.assertEquals(40, handoff.pendingBytes());
            parent.runPendingTasks();

            Assertions.assertEquals(2, writes.get());
            Assertions.assertEquals(0, handoff.pendingBytes());
            Assertions.assertEquals(0, first.refCnt());
            Assertions.assertEquals(0, second.refCnt());
        } finally {
            ReferenceCountUtil.safeRelease(first);
            ReferenceCountUtil.safeRelease(second);
            handoff.close();
            parent.runPendingTasks();
            parent.finishAndReleaseAll();
        }
    }

    private static void assertSchedulingRejectionReleasesAll(int acceptedExecutions, int messages) throws Exception {
        EmbeddedChannel parent = new EmbeddedChannel();
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        EventLoop eventLoop = rejectingEventLoop(parent.eventLoop(), executions, acceptedExecutions);
        List<ByteBuf> buffers = new ArrayList<>();
        RakChildWriteHandoff handoff = new RakChildWriteHandoff(eventLoop, message -> {
            ReferenceCountUtil.release(message);
            return parent.newSucceededFuture();
        }, () -> { },
                () -> true, () -> 16, () -> 32, () -> 1024 * 1024,
                ignored -> { }, ignored -> failures.incrementAndGet());
        try {
            for (int i = 0; i < messages; i++) {
                ByteBuf buffer = Unpooled.buffer(8).writeZero(8);
                buffers.add(buffer);
                try {
                    handoff.enqueue(buffer);
                } catch (RejectedExecutionException ignored) {
                    // Initial rejection is propagated so the caller can fail the original ChannelPromise.
                } finally {
                    buffer.release();
                }
            }
            parent.runPendingTasks();
            Assertions.assertEquals(1, failures.get());
            Assertions.assertEquals(0, handoff.pendingBytes());
            buffers.forEach(buffer -> Assertions.assertEquals(0, buffer.refCnt()));
        } finally {
            buffers.forEach(ReferenceCountUtil::safeRelease);
            parent.finishAndReleaseAll();
        }
    }

    private static EventLoop rejectingEventLoop(EventLoop delegate, AtomicInteger executions, int accepted) {
        return (EventLoop) Proxy.newProxyInstance(EventLoop.class.getClassLoader(), new Class<?>[]{EventLoop.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("execute")) {
                        if (executions.getAndIncrement() >= accepted) {
                            throw new RejectedExecutionException("test rejection");
                        }
                        delegate.execute((Runnable) args[0]);
                        return null;
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                });
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for test latch");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AssertionError(exception);
        }
    }

    private static RakChildWriteHandoff handoff(EmbeddedChannel parent,
                                                 java.util.function.Function<Object, ChannelFuture> write,
                                                 AtomicInteger flushes,
                                                 java.util.function.Consumer<Boolean> writability,
                                                 java.util.function.Consumer<Throwable> failure,
                                                 int lowWaterMark, int highWaterMark) {
        return new RakChildWriteHandoff(parent.eventLoop(), write, flushes::incrementAndGet,
                () -> true, () -> lowWaterMark, () -> highWaterMark, () -> 1024 * 1024,
                writability, failure);
    }
}
