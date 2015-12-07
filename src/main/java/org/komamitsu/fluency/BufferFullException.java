package org.komamitsu.fluency;

import java.io.IOException;

public class BufferFullException
        extends IOException
{
    public BufferFullException(String s)
    {
        super(s);
    }
}
