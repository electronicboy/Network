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

package org.cloudburstmc.netty.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public final class FastWeightedFairQueueTests {
    @Test
    public void selectsTheLeastWeightedLaneHeadAndPreservesTies() {
        FastWeightedFairQueue<String> queue = new FastWeightedFairQueue<>(4);
        try {
            queue.insert(10L, 2, "normal-first");
            queue.insert(10L, 2, "normal-second");
            queue.insert(3L, 1, "high");
            queue.insert(10L, 0, "immediate-tie");
            queue.insert(27L, 3, "low");

            Assertions.assertEquals(3L, queue.peekWeight());
            Assertions.assertEquals(1, queue.peekPriority());
            Assertions.assertEquals("high", queue.poll());
            Assertions.assertEquals("normal-first", queue.poll());
            Assertions.assertEquals("normal-second", queue.poll());
            Assertions.assertEquals("immediate-tie", queue.poll());
            Assertions.assertEquals("low", queue.poll());
            Assertions.assertTrue(queue.isEmpty());
        } finally {
            queue.release();
        }
    }

    @Test
    public void interleavedMonotonicLanesMatchStableWeightedOrder() {
        FastWeightedFairQueue<Integer> queue = new FastWeightedFairQueue<>(4);
        List<Expected> expected = new ArrayList<>();
        long[] laneWeights = {0L, 3L, 10L, 27L};
        long[] laneSteps = {1L, 5L, 14L, 35L};
        Random random = new Random(0x52_41_4b_4e_45_54L);
        try {
            for (int order = 0; order < 100_000; order++) {
                int lane = random.nextInt(4);
                long weight = laneWeights[lane];
                laneWeights[lane] += laneSteps[lane];
                queue.insert(weight, lane, order);
                expected.add(new Expected(weight, order));
            }
            expected.sort(Comparator.comparingLong((Expected item) -> item.weight)
                    .thenComparingInt(item -> item.order));

            for (Expected item : expected) {
                Assertions.assertEquals(item.order, queue.poll());
            }
            Assertions.assertEquals(0, queue.size());
            Assertions.assertNull(queue.peek());
        } finally {
            queue.release();
        }
    }

    @Test
    public void rejectsDescendingWeightWithoutCorruptingTheLane() {
        FastWeightedFairQueue<String> queue = new FastWeightedFairQueue<>(2);
        try {
            queue.insert(10L, 1, "first");
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> queue.insert(9L, 1, "invalid"));
            Assertions.assertEquals(1, queue.size());
            Assertions.assertEquals("first", queue.poll());
            queue.insert(1L, 1, "new-empty-epoch");
            Assertions.assertEquals("new-empty-epoch", queue.poll());
        } finally {
            queue.release();
        }
    }

    @Test
    public void releasingNonEmptyQueueRecyclesEveryInternalEntryOnce() {
        FastWeightedFairQueue<String> queue = new FastWeightedFairQueue<>(4);
        queue.insert(10L, 2, "normal");
        queue.insert(3L, 1, "high");

        Assertions.assertDoesNotThrow(() -> {
            queue.release();
        });
        Assertions.assertEquals(0, queue.refCnt());
    }

    private static final class Expected {
        private final long weight;
        private final int order;

        private Expected(long weight, int order) {
            this.weight = weight;
            this.order = order;
        }
    }
}
