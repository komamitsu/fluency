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

package org.komamitsu.fluency.ingester.sender.fluentd.heartbeat;

import org.komamitsu.fluency.ingester.sender.fluentd.SSLSocketBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocket;

import java.io.IOException;

public class SSLHeartbeater
        extends Heartbeater
{
    private static final Logger LOG = LoggerFactory.getLogger(SSLHeartbeater.class);
    private final Config config;
    private final SSLSocketBuilder sslSocketBuilder;

    protected SSLHeartbeater(final Config config)
            throws IOException
    {
        super(config.getBaseConfig());
        this.config = config;
        sslSocketBuilder = new SSLSocketBuilder(
                config.getHost(),
                config.getPort(),
                config.connectionTimeoutMilli,
                config.readTimeoutMilli);
    }

    @Override
    protected void invoke()
            throws IOException
    {
        SSLSocket sslSocket = null;
        try {
            sslSocket = sslSocketBuilder.build();
            LOG.trace("SSLHeartbeat: remotePort={}, localPort={}", sslSocket.getPort(), sslSocket.getLocalPort());
            // Try SSL handshake
            sslSocket.getSession();
            pong();
        }
        finally {
            if (sslSocket != null) {
                sslSocket.close();
            }
        }
    }

    @Override
    public String toString()
    {
        return "SSLHeartbeater{" +
                "config=" + config +
                ", sslSocketBuilder=" + sslSocketBuilder +
                "} " + super.toString();
    }

    public static class Config
            implements Instantiator
    {
        private final Heartbeater.Config baseConfig = new Heartbeater.Config();
        private int connectionTimeoutMilli = 5000;
        private int readTimeoutMilli = 5000;

        public Heartbeater.Config getBaseConfig()
        {
            return baseConfig;
        }

        public String getHost()
        {
            return baseConfig.getHost();
        }

        public Config setHost(String host)
        {
            baseConfig.setHost(host);
            return this;
        }

        public int getPort()
        {
            return baseConfig.getPort();
        }

        public Config setPort(int port)
        {
            baseConfig.setPort(port);
            return this;
        }

        public int getIntervalMillis()
        {
            return baseConfig.getIntervalMillis();
        }

        public Config setIntervalMillis(int intervalMillis)
        {
            baseConfig.setIntervalMillis(intervalMillis);
            return this;
        }

        public int getConnectionTimeoutMilli()
        {
            return connectionTimeoutMilli;
        }

        public Config setConnectionTimeoutMilli(int connectionTimeoutMilli)
        {
            this.connectionTimeoutMilli = connectionTimeoutMilli;
            return this;
        }

        public int getReadTimeoutMilli()
        {
            return readTimeoutMilli;
        }

        public Config setReadTimeoutMilli(int readTimeoutMilli)
        {
            this.readTimeoutMilli = readTimeoutMilli;
            return this;
        }

        @Override
        public SSLHeartbeater createInstance()
                throws IOException
        {
            return new SSLHeartbeater(this);
        }
    }
}

