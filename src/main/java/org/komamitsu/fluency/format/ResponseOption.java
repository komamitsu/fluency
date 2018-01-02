package org.komamitsu.fluency.format;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

public class ResponseOption
{
    private final byte[] ack;

    public ResponseOption(@JsonProperty("ack") byte[] ack)
    {
        this.ack = ack;
    }

    @JsonProperty("ack")
    public byte[] getAck()
    {
        return ack;
    }

    @Override
    public String toString()
    {
        return "ResponseOption{" +
                "ack=" + Arrays.toString(ack) +
                '}';
    }
}
