/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.fluentd.ingester.sender.heartbeat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDPHeartbeater extends InetSocketHeartbeater {
  private static final Logger LOG = LoggerFactory.getLogger(UDPHeartbeater.class);
  private final SocketAddress socketAddress;

  public UDPHeartbeater() {
    this(new Config());
  }

  public UDPHeartbeater(final Config config) {
    super(config);
    socketAddress = new InetSocketAddress(config.getHost(), config.getPort());
  }

  @Override
  protected void invoke() throws IOException {
    try (DatagramChannel datagramChannel = DatagramChannel.open()) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(0);
      datagramChannel.send(byteBuffer, socketAddress);
      datagramChannel.receive(byteBuffer);
      pong();
    }
  }

  @Override
  public String toString() {
    return "UDPHeartbeater{" + "socketAddress=" + socketAddress + "} " + super.toString();
  }

  public static class Config extends InetSocketHeartbeater.Config {}
}
