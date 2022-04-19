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

import org.komamitsu.fluency.fluentd.ingester.sender.SSLSocketBuilder;
import org.komamitsu.fluency.validation.Validatable;
import org.komamitsu.fluency.validation.annotation.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.SocketFactory;

import java.io.IOException;

public class SSLHeartbeater
        extends Heartbeater
{
    private static final Logger LOG = LoggerFactory.getLogger(SSLHeartbeater.class);
    private final Config config;
    private final SSLSocketBuilder sslSocketBuilder;

    public SSLHeartbeater()
    {
        this(new Config());
    }

    public SSLHeartbeater(final Config config)
    {
        super(config);
        config.validateValues();
        this.config = config;
        sslSocketBuilder = new SSLSocketBuilder(
            config.getHost(),
            config.getPort(),
            config.connectionTimeoutMilli,
            config.readTimeoutMilli,
            config.getSslSocketFactory()
        );
    }

    @Override
    protected void invoke()
            throws IOException
    {
        try (SSLSocket sslSocket = sslSocketBuilder.build()) {
            LOG.trace("SSLHeartbeat: remotePort={}, localPort={}", sslSocket.getPort(), sslSocket.getLocalPort());
            // Try SSL handshake
            sslSocket.getSession();
            pong();
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
            extends Heartbeater.Config
            implements Validatable
    {
        @Min(10)
        private int connectionTimeoutMilli = 5000;
        @Min(10)
        private int readTimeoutMilli = 5000;

        public int getConnectionTimeoutMilli()
        {
            return connectionTimeoutMilli;
        }

        public void setConnectionTimeoutMilli(int connectionTimeoutMilli)
        {
            this.connectionTimeoutMilli = connectionTimeoutMilli;
        }

        public int getReadTimeoutMilli()
        {
            return readTimeoutMilli;
        }

        public void setReadTimeoutMilli(int readTimeoutMilli)
        {
            this.readTimeoutMilli = readTimeoutMilli;
        }

        private SocketFactory sslSocketFactory = SSLSocketFactory.getDefault();

        public SocketFactory getSslSocketFactory() {
            return sslSocketFactory;
        }

        public void setSslSocketFactory(SocketFactory sslSocketFactory) {
            this.sslSocketFactory = sslSocketFactory;
        }

        void validateValues()
        {
            validate();
        }
    }
}

