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

package org.komamitsu.fluency.ingester.fluentdsender.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class TCPHeartbeater
        extends Heartbeater
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPHeartbeater.class);
    private final Config config;

    protected TCPHeartbeater(final Config config)
            throws IOException
    {
        super(config.getBaseConfig());
        this.config = config;
    }

    @Override
    protected void invoke()
            throws IOException
    {
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open(new InetSocketAddress(config.getHost(), config.getPort()));
            LOG.trace("TCPHeartbeat: remotePort={}, localPort={}",
                    socketChannel.socket().getPort(), socketChannel.socket().getLocalPort());
            pong();
        }
        finally {
            if (socketChannel != null) {
                socketChannel.close();
            }
        }
    }

    @Override
    public String toString()
    {
        return "TCPHeartbeater{" +
                "config=" + config +
                "} " + super.toString();
    }

    public static class Config
            implements Instantiator
    {
        private final Heartbeater.Config baseConfig = new Heartbeater.Config();

        public Heartbeater.Config getBaseConfig()
        {
            return baseConfig;
        }

        public String getHost()
        {
            return baseConfig.getHost();
        }

        public int getPort()
        {
            return baseConfig.getPort();
        }

        public Config setIntervalMillis(int intervalMillis)
        {
            baseConfig.setIntervalMillis(intervalMillis);
            return this;
        }

        public int getIntervalMillis()
        {
            return baseConfig.getIntervalMillis();
        }

        public Config setHost(String host)
        {
            baseConfig.setHost(host);
            return this;
        }

        public Config setPort(int port)
        {
            baseConfig.setPort(port);
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "baseConfig=" + baseConfig +
                    '}';
        }

        @Override
        public TCPHeartbeater createInstance()
                throws IOException
        {
            return new TCPHeartbeater(this);
        }
    }

}

