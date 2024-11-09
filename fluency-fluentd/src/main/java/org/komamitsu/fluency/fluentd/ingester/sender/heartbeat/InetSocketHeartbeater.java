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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InetSocketHeartbeater extends Heartbeater {
  private static final Logger LOG = LoggerFactory.getLogger(InetSocketHeartbeater.class);
  private final Config config;

  protected InetSocketHeartbeater(Config config) {
    super(config);
    this.config = config;
  }

  protected abstract void invoke() throws IOException;

  public String getHost() {
    return config.getHost();
  }

  public int getPort() {
    return config.getPort();
  }

  public int getIntervalMillis() {
    return config.getIntervalMillis();
  }

  @Override
  public String toString() {
    return "InetHeartbeater{" + "config=" + config + '}';
  }

  public interface Callback {
    void onHeartbeat();

    void onFailure(Throwable cause);
  }

  public static class Config extends Heartbeater.Config {
    private String host = "127.0.0.1";
    private int port = 24224;

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    @Override
    public String toString() {
      return "Config{" + "host='" + host + '\'' + ", port=" + port + "} " + super.toString();
    }
  }
}
