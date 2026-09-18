/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.cloudburstmc.netty.handler.codec.raknet.common;

import io.netty.channel.DefaultEventLoop;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class RakSessionTickerTests {

    @Test
    public void sameLoopAndIntervalShareOneCoordinatorUntilLastCancellation() {
        DefaultEventLoop eventLoop = new DefaultEventLoop();
        RakSessionTicker.Registration first = null;
        RakSessionTicker.Registration second = null;
        try {
            first = RakSessionTicker.register(eventLoop, 10, () -> { });
            second = RakSessionTicker.register(eventLoop, 10, () -> { });
            Assertions.assertEquals(1, RakSessionTicker.coordinatorCount(eventLoop, 10));

            first.cancel();
            first = null;
            Assertions.assertEquals(1, RakSessionTicker.coordinatorCount(eventLoop, 10));

            second.cancel();
            second = null;
            Assertions.assertEquals(0, RakSessionTicker.coordinatorCount(eventLoop, 10));
        } finally {
            if (first != null) {
                first.cancel();
            }
            if (second != null) {
                second.cancel();
            }
            eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
        }
    }

    @Test
    public void differentIntervalsUseDifferentCoordinators() {
        DefaultEventLoop eventLoop = new DefaultEventLoop();
        RakSessionTicker.Registration tenMillis = null;
        RakSessionTicker.Registration twentyMillis = null;
        try {
            tenMillis = RakSessionTicker.register(eventLoop, 10, () -> { });
            twentyMillis = RakSessionTicker.register(eventLoop, 20, () -> { });
            Assertions.assertEquals(1, RakSessionTicker.coordinatorCount(eventLoop, 10));
            Assertions.assertEquals(1, RakSessionTicker.coordinatorCount(eventLoop, 20));
        } finally {
            if (tenMillis != null) {
                tenMillis.cancel();
            }
            if (twentyMillis != null) {
                twentyMillis.cancel();
            }
            eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
        }
    }

    @Test
    public void ninthSessionRaisesPulseCadenceWithoutAddingCoordinator() {
        DefaultEventLoop eventLoop = new DefaultEventLoop();
        RakSessionTicker.Registration[] registrations =
                new RakSessionTicker.Registration[RakSessionTicker.MAX_SESSIONS_PER_PULSE + 1];
        try {
            for (int i = 0; i < registrations.length; i++) {
                registrations[i] = RakSessionTicker.register(eventLoop, 10, () -> { });
            }
            Assertions.assertEquals(1, RakSessionTicker.coordinatorCount(eventLoop, 10));
            Assertions.assertEquals(2, RakSessionTicker.pulsesPerInterval(eventLoop, 10));

            registrations[registrations.length - 1].cancel();
            registrations[registrations.length - 1] = null;
            Assertions.assertEquals(1, RakSessionTicker.coordinatorCount(eventLoop, 10));
            Assertions.assertEquals(1, RakSessionTicker.pulsesPerInterval(eventLoop, 10));
        } finally {
            for (RakSessionTicker.Registration registration : registrations) {
                if (registration != null) {
                    registration.cancel();
                }
            }
            eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
        }
    }
}
