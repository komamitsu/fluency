package org.komamitsu.fluency.sender;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ConfigTest
{
    static class DummySender
        extends Sender
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

    @Test
    public void errorHandler()
            throws IOException
    {
        final AtomicBoolean errorOccurred = new AtomicBoolean();
        Sender.Config config = new Sender.Config().setSenderErrorHandler(new SenderErrorHandler()
        {
            @Override
            public void handle(Throwable e)
            {
                errorOccurred.set(true);
            }
        });

        new DummySender(config, false).send(ByteBuffer.allocate(8));
        assertThat(errorOccurred.get(), is(false));

        try {
            new DummySender(config, true).send(ByteBuffer.allocate(8));
            assertTrue(false);
        }
        catch (Exception e) {
            assertThat(errorOccurred.get(), is(true));
        }
    }
}