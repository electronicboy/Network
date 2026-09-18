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

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.internal.ObjectPool;

import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A fixed-lane weighted fair queue.
 *
 * <p>Weights within a lane must be inserted in nondecreasing order. The queue keeps each lane in FIFO order and
 * selects the least-weighted lane head. This makes insertion O(1) and removal O(lanes), avoiding a binary-heap
 * operation for every element when the number of priorities is small and fixed.</p>
 *
 * @param <E> element type
 */
public final class FastWeightedFairQueue<E> extends AbstractReferenceCounted {
    private static final ObjectPool<Entry> RECYCLER = ObjectPool.newPool(Entry::new);

    private final Entry[] heads;
    private final Entry[] tails;
    private int selectedLane = -1;
    private int size;
    private long insertionOrder;

    public FastWeightedFairQueue(int lanes) {
        if (lanes <= 0) {
            throw new IllegalArgumentException("lanes must be positive");
        }
        this.heads = new Entry[lanes];
        this.tails = new Entry[lanes];
    }

    public void insert(long weight, int lane, E element) {
        Objects.requireNonNull(element, "element");
        this.checkLane(lane);

        Entry tail = this.tails[lane];
        if (tail != null && weight < tail.weight) {
            throw new IllegalArgumentException("weights within a lane must be nondecreasing");
        }

        Entry entry = RECYCLER.get();
        entry.element = element;
        entry.weight = weight;
        entry.lane = lane;
        entry.order = this.insertionOrder++;

        if (tail == null) {
            this.heads[lane] = entry;
        } else {
            tail.next = entry;
        }
        this.tails[lane] = entry;
        this.size++;

        if (this.selectedLane == -1 || compare(entry, this.heads[this.selectedLane]) < 0) {
            this.selectedLane = lane;
        }
    }

    @SuppressWarnings("unchecked")
    public E peek() {
        Entry entry = this.selectedEntry();
        return entry == null ? null : (E) entry.element;
    }

    public long peekWeight() {
        Entry entry = this.selectedEntry();
        if (entry == null) {
            throw new NoSuchElementException("Queue is empty");
        }
        return entry.weight;
    }

    public int peekPriority() {
        Entry entry = this.selectedEntry();
        if (entry == null) {
            throw new NoSuchElementException("Queue is empty");
        }
        return entry.lane;
    }

    @SuppressWarnings("unchecked")
    public E poll() {
        Entry entry = this.removeEntry();
        if (entry == null) {
            return null;
        }
        E element = (E) entry.element;
        entry.release();
        return element;
    }

    public void remove() {
        Entry entry = this.removeEntry();
        if (entry == null) {
            throw new NoSuchElementException("Queue is empty");
        }
        entry.release();
    }

    public int size() {
        return this.size;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isEmpty() {
        return this.size == 0;
    }

    private Entry selectedEntry() {
        return this.selectedLane == -1 ? null : this.heads[this.selectedLane];
    }

    private Entry removeEntry() {
        Entry entry = this.selectedEntry();
        if (entry == null) {
            return null;
        }

        int lane = this.selectedLane;
        Entry next = entry.next;
        entry.next = null;
        this.heads[lane] = next;
        if (next == null) {
            this.tails[lane] = null;
        }

        this.size--;
        if (this.size == 0) {
            this.selectedLane = -1;
            this.insertionOrder = 0L;
        } else {
            this.selectHead();
        }
        return entry;
    }

    private void selectHead() {
        int selected = -1;
        for (int lane = 0; lane < this.heads.length; lane++) {
            Entry candidate = this.heads[lane];
            if (candidate != null && (selected == -1 || compare(candidate, this.heads[selected]) < 0)) {
                selected = lane;
            }
        }
        this.selectedLane = selected;
    }

    private void checkLane(int lane) {
        if (lane < 0 || lane >= this.heads.length) {
            throw new IndexOutOfBoundsException("lane: " + lane);
        }
    }

    private static int compare(Entry first, Entry second) {
        int weightComparison = Long.compare(first.weight, second.weight);
        return weightComparison != 0 ? weightComparison : Long.compare(first.order, second.order);
    }

    @Override
    protected void deallocate() {
        Entry entry;
        while ((entry = this.removeEntry()) != null) {
            entry.release();
        }
    }

    @Override
    public FastWeightedFairQueue<E> touch(Object hint) {
        return this;
    }

    private static final class Entry extends AbstractReferenceCounted {
        private final ObjectPool.Handle<Entry> handle;
        private Object element;
        private Entry next;
        private long weight;
        private long order;
        private int lane;

        private Entry(ObjectPool.Handle<Entry> handle) {
            this.handle = handle;
        }

        @Override
        protected void deallocate() {
            this.setRefCnt(1);
            this.element = null;
            this.next = null;
            this.weight = 0L;
            this.order = 0L;
            this.lane = 0;
            this.handle.recycle(this);
        }

        @Override
        public Entry touch(Object hint) {
            return this;
        }
    }
}
