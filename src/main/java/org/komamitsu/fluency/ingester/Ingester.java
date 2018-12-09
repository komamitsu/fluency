/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.ingester;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.komamitsu.fluency.ingester.fluentdsender.FluentdSender;
import org.komamitsu.fluency.ingester.sender.Sender;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public interface Ingester
    extends Closeable
{
    void ingest(String tag, ByteBuffer dataBuffer)
            throws IOException;

    interface Config
    {}

    interface Instantiator<T extends Sender>
    {
        Ingester createInstance(T sender);
    }
}
