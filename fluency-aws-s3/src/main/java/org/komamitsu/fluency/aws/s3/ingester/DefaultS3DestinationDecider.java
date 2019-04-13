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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DefaultS3DestinationDecider
        implements S3DestinationDecider
{
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm-ss-SSSSSS");
    private final Config config;

    public DefaultS3DestinationDecider()
    {
        this(new Config());
    }

    public DefaultS3DestinationDecider(Config config)
    {
        this.config = config;
    }

    protected String getBucket(String tag)
    {
        return tag;
    }

    protected String getKeyExceptSuffix(String tag, long time)
    {
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), config.getZoneId()).format(FORMATTER);
    }

    public S3Destination decide(String tag, long time)
    {
        return new S3Destination(getBucket(tag), getKeyExceptSuffix(tag, time));
    }

    public static class Config
    {
        private ZoneId zoneId = ZoneOffset.UTC;

        public ZoneId getZoneId()
        {
            return zoneId;
        }

        public void setZoneId(ZoneId zoneId)
        {
            this.zoneId = zoneId;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "zoneId=" + zoneId +
                    '}';
        }
    }
}
