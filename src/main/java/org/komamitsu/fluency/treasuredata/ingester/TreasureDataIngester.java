/*
 * Copyright 2019 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.fluency.treasuredata.ingester;

import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.komamitsu.fluency.treasuredata.ingester.sender.TreasureDataSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TreasureDataIngester
        implements Ingester
{
    private static final Logger LOG = LoggerFactory.getLogger(TreasureDataIngester.class);
    private final Config config;
    private final TreasureDataSender sender;

    public TreasureDataIngester(Config config, TreasureDataSender sender)
    {
        this.config = config;
        this.sender = sender;
    }

    @Override
    public void ingest(String tag, ByteBuffer dataBuffer)
            throws IOException
    {
        sender.send(tag, dataBuffer);
    }

    @Override
    public Sender getSender()
    {
        return sender;
    }

    @Override
    public void close()
            throws IOException
    {
        sender.close();
    }

    public static class Config
        implements Instantiator<TreasureDataSender>
    {
        @Override
        public TreasureDataIngester createInstance(TreasureDataSender sender)
        {
            return new TreasureDataIngester(this, sender);
        }
    }
}
