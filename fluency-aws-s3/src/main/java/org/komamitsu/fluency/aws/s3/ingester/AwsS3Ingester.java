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
import org.komamitsu.fluency.validation.Validatable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;

public class AwsS3Ingester
        implements Ingester
{
    private static final Logger LOG = LoggerFactory.getLogger(AwsS3Ingester.class);
    private final Config config;
    private final AwsS3Sender sender;
    private final S3DestinationDecider s3DestinationDecider;

    public AwsS3Ingester(AwsS3Sender sender, S3DestinationDecider s3DestinationDecider)
    {
        this(new Config(), sender, s3DestinationDecider);
    }

    public AwsS3Ingester(Config config, AwsS3Sender sender, S3DestinationDecider s3DestinationDecider)
    {
        config.validateValues();

        this.config = config;
        this.sender = sender;
        this.s3DestinationDecider = s3DestinationDecider;
    }

    @Override
    public void ingest(String tag, ByteBuffer dataBuffer)
            throws IOException
    {
        S3DestinationDecider.S3Destination s3Destination =
                s3DestinationDecider.decide(tag, Instant.now());
        String bucket = s3Destination.getBucket();
        String key = s3Destination.getKeyBase() + config.getKeySuffix();

        sender.send(bucket, key, dataBuffer);
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
            implements Validatable
    {
        private String keySuffix;

        public String getKeySuffix()
        {
            return keySuffix;
        }

        public void setKeySuffix(String keySuffix)
        {
            this.keySuffix = keySuffix;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "keySuffix='" + keySuffix + '\'' +
                    '}';
        }

        void validateValues()
        {
            validate();

            if (keySuffix == null) {
                throw new IllegalArgumentException("`keySuffix` should be set");
            }
        }
    }
}
