package org.komamitsu.fluency.sender;

import org.msgpack.core.annotations.Nullable;

import java.io.IOException;

public interface AckTokenSerDe
{
    byte[] pack(int size)
            throws IOException;

    byte[] packWithAckResponseToken(int size, @Nullable byte[] ackResponseToken)
            throws IOException;

    byte[] unpack(byte[] packedToken)
            throws IOException;
}
