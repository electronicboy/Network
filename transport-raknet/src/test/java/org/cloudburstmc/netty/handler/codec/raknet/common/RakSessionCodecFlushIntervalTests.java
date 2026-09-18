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

import org.cloudburstmc.netty.channel.raknet.config.RakChannelConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RakSessionCodecFlushIntervalTests {

    @Test
    public void captureHelperReturnsCurrentAutoFlushInterval() {
        AtomicBoolean autoFlush = new AtomicBoolean(true);
        AtomicInteger configuredFlushInterval = new AtomicInteger(7);
        RakChannelConfig config = config(autoFlush, configuredFlushInterval);
        int firstCapture = RakSessionCodec.captureFlushInterval(config);

        configuredFlushInterval.set(50);
        Assertions.assertEquals(7, firstCapture,
                "the previously returned primitive is an immutable snapshot");
        Assertions.assertEquals(50, RakSessionCodec.captureFlushInterval(config),
                "a new helper call reads the current option");
    }

    @Test
    public void captureHelperReturnsTenMillisecondMaintenanceTickForManualFlush() {
        AtomicBoolean autoFlush = new AtomicBoolean(false);
        AtomicInteger configuredFlushInterval = new AtomicInteger(3);
        RakChannelConfig config = config(autoFlush, configuredFlushInterval);

        Assertions.assertEquals(10, RakSessionCodec.captureFlushInterval(config));
        configuredFlushInterval.set(50);
        Assertions.assertEquals(10, RakSessionCodec.captureFlushInterval(config));
    }

    private static RakChannelConfig config(AtomicBoolean autoFlush, AtomicInteger flushInterval) {
        return (RakChannelConfig) Proxy.newProxyInstance(RakChannelConfig.class.getClassLoader(),
                new Class<?>[]{RakChannelConfig.class}, (proxy, method, args) -> {
                    if (method.getName().equals("isAutoFlush")) {
                        return autoFlush.get();
                    }
                    if (method.getName().equals("getFlushInterval")) {
                        return flushInterval.get();
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
