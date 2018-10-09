/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
