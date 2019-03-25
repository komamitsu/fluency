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

package org.komamitsu.fluency.aws.s3.ingester;

import org.komamitsu.fluency.aws.s3.ingester.sender.AwsS3Sender;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AwsS3Ingester
        implements Ingester
{
    private static final Logger LOG = LoggerFactory.getLogger(AwsS3Ingester.class);
    private final AwsS3Sender sender;

    public AwsS3Ingester(AwsS3Sender sender)
    {
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
}
