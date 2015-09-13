package org.komamitsu.fluency.sender;

import java.io.IOException;

public class SenderException
    extends IOException
{
    public SenderException(String s, Throwable throwable)
    {
        super(s, throwable);
    }
}
