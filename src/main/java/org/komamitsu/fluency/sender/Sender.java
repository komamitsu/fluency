package org.komamitsu.fluency.sender;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface Sender
    extends Closeable
{
    void send(ByteBuffer data) throws IOException;
}
