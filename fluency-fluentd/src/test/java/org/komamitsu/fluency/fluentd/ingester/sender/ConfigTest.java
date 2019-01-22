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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

class ConfigTest
{
    @Test
    void errorHandler()
            throws IOException
    {
        final AtomicBoolean errorOccurred = new AtomicBoolean();
        FluentdSender.Config config = new FluentdSender.Config();
        config.setErrorHandler(e -> errorOccurred.set(true));

        new DummySender(config, false).send(ByteBuffer.allocate(8));
        assertThat(errorOccurred.get(), is(false));

        try {
            new DummySender(config, true).send(ByteBuffer.allocate(8));
            fail();
        }
        catch (Exception e) {
            assertThat(errorOccurred.get(), is(true));
        }
    }

    static class DummySender
            extends FluentdSender
    {
        private final boolean shouldFail;

        protected DummySender(Config config, boolean shouldFail)
        {
            super(config);
            this.shouldFail = shouldFail;
        }

        @Override
        public boolean isAvailable()
        {
            return true;
        }

        @Override
        protected void sendInternal(List<ByteBuffer> buffers, byte[] ackToken)
                throws IOException
        {
            if (shouldFail) {
                throw new RuntimeException("Unexpected scheduled error occurred");
            }
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }
}