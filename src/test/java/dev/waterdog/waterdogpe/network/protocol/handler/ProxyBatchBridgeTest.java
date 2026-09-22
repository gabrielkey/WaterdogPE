/*
 * Copyright 2026 WaterdogTEAM
 * Licensed under the GNU General Public License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.waterdog.waterdogpe.network.protocol.handler;

import dev.waterdog.waterdogpe.network.connection.ProxiedConnection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.cloudburstmc.protocol.bedrock.PacketDirection;
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodec;
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodecHelper;
import org.cloudburstmc.protocol.bedrock.codec.PacketSerializeException;
import org.cloudburstmc.protocol.bedrock.codec.v2168.Bedrock_v2168;
import org.cloudburstmc.protocol.bedrock.netty.BedrockBatchWrapper;
import org.cloudburstmc.protocol.bedrock.netty.BedrockPacketWrapper;
import org.cloudburstmc.protocol.bedrock.packet.BedrockPacket;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProxyBatchBridgeTest {

    private static final int PACKET_ID = 65;
    private static final String WIRE = "fdffffffdf800120010b141801";

    /**
     * Downstream gains payload fields between game versions and the codec catches up afterwards.
     * Until it does, a packet the proxy cannot read has to reach the other side anyway: throwing
     * here reaches the peer's exceptionCaught, which closes the connection with "Internal error".
     */
    @Test
    void relaysAPacketTheCodecCannotDecode() {
        BedrockCodecHelper helper = Bedrock_v2168.CODEC.createHelper();
        BedrockCodec codec = mock(BedrockCodec.class);
        when(codec.tryDecode(any(), any(), anyInt(), any()))
                .thenThrow(new PacketSerializeException("Error whilst deserializing EventPacket",
                        new ArrayIndexOutOfBoundsException("Index -10 out of bounds for length 26")));

        ProxyPacketHandler handler = mock(ProxyPacketHandler.class);
        ProxiedConnection source = mock(ProxiedConnection.class);
        when(source.getPacketDirection()).thenReturn(PacketDirection.CLIENT_BOUND);

        ProxyBatchBridge bridge = new ProxyBatchBridge(codec, helper, handler, PacketDirection.CLIENT_BOUND);

        ByteBuf packetBuffer = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(WIRE));
        BedrockBatchWrapper batch = BedrockBatchWrapper.newInstance();
        batch.getPackets().add(BedrockPacketWrapper.create(PACKET_ID, 0, 0, null, packetBuffer));

        try {
            assertDoesNotThrow(() -> bridge.onBedrockBatch(source, batch));

            verify(handler).sendProxiedBatch(batch);
            verify(handler, never()).handlePacket(any(BedrockPacket.class));

            assertEquals(1, batch.getPackets().size());
            BedrockPacketWrapper wrapper = batch.getPackets().get(0);
            assertSame(packetBuffer, wrapper.getPacketBuffer(), "the original bytes must be what goes out");
            assertEquals(WIRE, ByteBufUtil.hexDump(wrapper.getPacketBuffer()));
        } finally {
            batch.release();
        }
    }
}
