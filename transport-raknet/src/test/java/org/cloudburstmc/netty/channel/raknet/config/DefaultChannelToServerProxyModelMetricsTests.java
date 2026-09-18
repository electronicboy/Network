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

package org.cloudburstmc.netty.channel.raknet.config;

import io.netty.channel.socket.nio.NioDatagramChannel;
import org.cloudburstmc.netty.channel.raknet.RakChildChannel;
import org.cloudburstmc.netty.channel.raknet.RakServerChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DefaultChannelToServerProxyModelMetricsTests {

    @Test
    public void extendedProxyStateDelegatesToExistingServerMetricsImplementation() {
        int[] legacyCallbacks = {0};
        RakServerMetrics legacyMetrics = new RakServerMetrics() {
            @Override
            public void rakCongestionModelState(RakChildChannel channel, long observedAtMillis,
                                                double estimatedDeliveryRateBytesPerSecond,
                                                double pacingRateBytesPerSecond, long minimumRttMillis,
                                                double recentLossRate, long packetRound, boolean startup,
                                                boolean persistentCongestion) {
                legacyCallbacks[0]++;
            }
        };
        NioDatagramChannel datagramChannel = new NioDatagramChannel();
        RakServerChannel parent = new RakServerChannel(datagramChannel);
        try {
            parent.config().setMetrics(legacyMetrics);
            DefaultChannelToServerProxyMetrics proxy = new DefaultChannelToServerProxyMetrics(parent, null);

            proxy.rakCongestionModelState(100L, 12_000D, 15_000D, 200L, 0.05D, 7L,
                    true, false, 3L, 2L);

            Assertions.assertEquals(1, legacyCallbacks[0]);
        } finally {
            datagramChannel.unsafe().closeForcibly();
        }
    }

    @Test
    public void forwardsModelMetricsWithoutChangingDimensions() {
        RecordingServerMetrics serverMetrics = new RecordingServerMetrics();
        NioDatagramChannel datagramChannel = new NioDatagramChannel();
        RakServerChannel parent = new RakServerChannel(datagramChannel);
        try {
            parent.config().setMetrics(serverMetrics);
            DefaultChannelToServerProxyMetrics proxy = new DefaultChannelToServerProxyMetrics(parent, null);

            proxy.rakCongestionModelState(100L, 12_000D, 15_000D, 200L, 0.05D, 7L,
                    true, false, 3L, 2L);
            Assertions.assertEquals(1, serverMetrics.modelStates);
            Assertions.assertEquals(12_000D, serverMetrics.deliveryRate);
            Assertions.assertEquals(15_000D, serverMetrics.pacingRate);
            Assertions.assertEquals(200L, serverMetrics.minimumRtt);
            Assertions.assertEquals(0.05D, serverMetrics.recentLossRate);
            Assertions.assertEquals(7L, serverMetrics.packetRound);
            Assertions.assertTrue(serverMetrics.startup);
            Assertions.assertEquals(3L, serverMetrics.hardLossResponses);
            Assertions.assertEquals(2L, serverMetrics.delayLossResponses);
        } finally {
            datagramChannel.unsafe().closeForcibly();
        }
    }

    private static final class RecordingServerMetrics implements RakServerMetrics {
        private int modelStates;
        private double deliveryRate;
        private double pacingRate;
        private long minimumRtt;
        private double recentLossRate;
        private long packetRound;
        private boolean startup;
        private long hardLossResponses;
        private long delayLossResponses;

        @Override
        public void rakCongestionModelState(RakChildChannel channel, long observedAtMillis,
                                            double estimatedDeliveryRateBytesPerSecond,
                                            double pacingRateBytesPerSecond, long minimumRttMillis,
                                            double recentLossRate, long packetRound, boolean startup,
                                            boolean persistentCongestion, long hardLossResponseCount,
                                            long delayLossResponseCount) {
            this.modelStates++;
            this.deliveryRate = estimatedDeliveryRateBytesPerSecond;
            this.pacingRate = pacingRateBytesPerSecond;
            this.minimumRtt = minimumRttMillis;
            this.recentLossRate = recentLossRate;
            this.packetRound = packetRound;
            this.startup = startup;
            this.hardLossResponses = hardLossResponseCount;
            this.delayLossResponses = delayLossResponseCount;
        }
    }
}
