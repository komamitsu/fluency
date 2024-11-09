/*
 * Copyright 2019 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.fluency.fluentd.ingester.sender;

import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InetSocketSender<T> extends NetworkSender<T> {
  private static final Logger LOG = LoggerFactory.getLogger(InetSocketSender.class);
  private final Config config;

  public InetSocketSender(Config config, FailureDetector failureDetector) {
    super(config, failureDetector);
    this.config = config;
  }

  public String getHost() {
    return config.getHost();
  }

  public int getPort() {
    return config.getPort();
  }

  @Override
  public String toString() {
    return "NetworkSender{" + "config=" + config + "} " + super.toString();
  }

  public static class Config extends NetworkSender.Config {
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
