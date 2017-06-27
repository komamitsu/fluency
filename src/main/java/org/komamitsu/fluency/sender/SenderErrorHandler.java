package org.komamitsu.fluency.sender;

public interface SenderErrorHandler
{
    void handle(Throwable e);
}
