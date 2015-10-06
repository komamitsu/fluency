package org.komamitsu.fluency.sender;

import java.io.IOException;

public interface AckTokenSerDe
{
    byte[] pack(byte[] token)
            throws IOException;

    byte[] unpack(byte[] packedToken)
            throws IOException;
}
