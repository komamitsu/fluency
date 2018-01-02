package org.komamitsu.fluency.format;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RequestOption
{
    private final int size;
    private final byte[] chunk;

    public RequestOption(
            @JsonProperty("size") int size,
            @JsonProperty("chunk") byte[] chunk)
    {
        this.size = size;
        this.chunk = chunk;
    }

    @JsonProperty("size")
    public int getSize()
    {
        return size;
    }

    @JsonProperty("chunk")
    public byte[] getChunk()
    {
        return chunk;
    }

    @Override
    public String toString()
    {
        return "RequestOption{" +
                "size=" + size +
                ", chunk=" + Arrays.toString(chunk) +
                '}';
    }
}
