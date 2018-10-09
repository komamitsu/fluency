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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

public class Response
{
    private final byte[] ack;

    public Response(@JsonProperty("ack") byte[] ack)
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
