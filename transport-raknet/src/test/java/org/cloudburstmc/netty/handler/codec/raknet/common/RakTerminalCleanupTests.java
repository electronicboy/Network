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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.cloudburstmc.netty.channel.raknet.RakChannel;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelConfig;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelMetrics;
import org.cloudburstmc.netty.channel.raknet.packet.EncapsulatedPacket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

public class RakTerminalCleanupTests {
    @Test
    public void sessionCleanupBeforeActivationIsSafeWithMetricsConfigured() {
        RakChannelMetrics metrics = new RakChannelMetrics() { };
        RakChannelConfig config = proxy(RakChannelConfig.class, (method, args) ->
                method.getName().equals("getMetrics") ? metrics : defaultValue(method.getReturnType()));
        RakChannel channel = proxy(RakChannel.class, (method, args) ->
                method.getName().equals("config") ? config : defaultValue(method.getReturnType()));
        RakSessionCodec codec = new RakSessionCodec(channel);

        Assertions.assertDoesNotThrow(codec::closeAfterEventLoopTermination);
        Assertions.assertDoesNotThrow(codec::closeAfterEventLoopTermination);
    }

    @Test
    public void unhandledQueueCleanupAfterEventLoopTerminationReleasesPayload() {
        RakChannel channel = proxy(RakChannel.class, (method, args) ->
                method.getName().equals("isActive") ? false : defaultValue(method.getReturnType()));
        RakUnhandledMessagesQueue queue = new RakUnhandledMessagesQueue(channel);
        EmbeddedChannel embedded = new EmbeddedChannel(queue);
        ByteBuf payload = Unpooled.buffer(8).writeZero(8);
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setBuffer(payload);
        try {
            Assertions.assertFalse(embedded.writeInbound(packet));
            Assertions.assertEquals(1, payload.refCnt(), "the inactive queue owns the payload");

            queue.closeAfterEventLoopTermination();
            Assertions.assertEquals(0, payload.refCnt());
            Assertions.assertDoesNotThrow(queue::closeAfterEventLoopTermination);
        } finally {
            embedded.finishAndReleaseAll();
            if (payload.refCnt() > 0) {
                payload.release(payload.refCnt());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, args) -> invocation.invoke(method, args));
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        return '\0';
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(java.lang.reflect.Method method, Object[] arguments) throws Throwable;
    }
}
