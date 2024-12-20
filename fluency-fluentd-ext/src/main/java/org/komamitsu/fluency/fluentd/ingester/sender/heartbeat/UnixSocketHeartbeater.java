/*
 * Copyright 2022 Mitsunori Komatsu (komamitsu)
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
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnixSocketHeartbeater extends Heartbeater {
  private static final Logger LOG = LoggerFactory.getLogger(UnixSocketHeartbeater.class);
  private final Config config;

  public UnixSocketHeartbeater() {
    this(new Config());
  }

  public UnixSocketHeartbeater(final Config config) {
    super(config);
    this.config = config;
  }

  @Override
  protected void invoke() throws IOException {
    try (SocketChannel socketChannel =
        SocketChannel.open(UnixDomainSocketAddress.of(config.getPath()))) {
      LOG.trace("UnixSocketHeartbeat: {}", socketChannel);
      pong();
    }
  }

  public Path getPath() {
    return config.getPath();
  }

  @Override
  public String toString() {
    return "UnixSocketHeartbeater{" + "config=" + config + "} " + super.toString();
  }

  public static class Config extends Heartbeater.Config {
    private Path path;

    public Path getPath() {
      return path;
    }

    public void setPath(Path path) {
      this.path = path;
    }
  }
}
