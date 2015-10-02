package org.komamitsu.fluency.sender;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public interface Sender
    extends Closeable
{
    void send(ByteBuffer data) throws IOException;

    void send(List<ByteBuffer> dataList) throws IOException;

    void sendWithAck(List<ByteBuffer> dataList, String uuid)
            throws IOException;
}
