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

package org.komamitsu.fluency.ingester.sender.fluentd;

import org.komamitsu.fluency.ingester.sender.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiSender
        extends FluentdSender
{
    private static final Logger LOG = LoggerFactory.getLogger(MultiSender.class);
    private final List<FluentdSender> senders = new ArrayList<>();

    protected MultiSender(Config config)
    {
        super(config.getBaseConfig());
        for (Instantiator senderConfig : config.getSenderConfigs()) {
            senders.add(senderConfig.createInstance());
        }
    }

    @Override
    public boolean isAvailable()
    {
        return true;
    }

    @Override
    protected synchronized void sendInternal(List<ByteBuffer> buffers, byte[] ackToken)
            throws AllNodesUnavailableException
    {
        for (FluentdSender sender : senders) {
            boolean isAvailable = sender.isAvailable();
            LOG.trace("send(): sender={}, isAvailable={}", sender, isAvailable);
            if (isAvailable) {
                try {
                    if (ackToken == null) {
                        sender.send(buffers);
                    }
                    else {
                        sender.sendWithAck(buffers, ackToken);
                    }
                    return;
                }
                catch (IOException e) {
                    LOG.error("Failed to send: sender=" + sender + ". Trying to use next sender...", e);
                }
            }
        }
        throw new AllNodesUnavailableException("All nodes are unavailable");
    }

    @Override
    public void close()
            throws IOException
    {
        IOException firstException = null;
        for (FluentdSender sender : senders) {
            try {
                sender.close();
            }
            catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    public static class AllNodesUnavailableException
            extends IOException
    {
        public AllNodesUnavailableException(String s)
        {
            super(s);
        }
    }

    public List<FluentdSender> getSenders()
    {
        return Collections.unmodifiableList(senders);
    }

    @Override
    public String toString()
    {
        return "MultiSender{" +
                "senders=" + senders +
                "} " + super.toString();
    }

    public static class Config
            implements Instantiator
    {
        private final FluentdSender.Config baseConfig = new FluentdSender.Config();
        private final List<Instantiator> senderConfigs;

        public FluentdSender.Config getBaseConfig()
        {
            return baseConfig;
        }

        public ErrorHandler getErrorHandler()
        {
            return baseConfig.getErrorHandler();
        }

        public Config setErrorHandler(ErrorHandler errorHandler)
        {
            baseConfig.setErrorHandler(errorHandler);
            return this;
        }

        public Config(List<Instantiator> senderConfigs)
        {
            this.senderConfigs = senderConfigs;
        }

        public List<Instantiator> getSenderConfigs()
        {
            return senderConfigs;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "senderConfigs=" + senderConfigs +
                    "} " + super.toString();
        }

        @Override
        public MultiSender createInstance()
        {
            return new MultiSender(this);
        }
    }
}

