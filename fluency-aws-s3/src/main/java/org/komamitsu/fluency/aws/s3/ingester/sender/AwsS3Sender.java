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

package org.komamitsu.fluency.aws.s3.ingester.sender;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.komamitsu.fluency.NonRetryableException;
import org.komamitsu.fluency.RetryableException;
import org.komamitsu.fluency.ingester.sender.ErrorHandler;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.komamitsu.fluency.validation.Validatable;
import org.komamitsu.fluency.validation.annotation.DecimalMin;
import org.komamitsu.fluency.validation.annotation.Min;
import org.msgpack.core.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

public class AwsS3Sender
        implements Closeable, Sender
{
    private static final Logger LOG = LoggerFactory.getLogger(AwsS3Sender.class);
    private final Config config;
    private final RetryPolicy retryPolicy;
    private final S3Client client;

    public AwsS3Sender()
    {
        this(new Config());
    }

    public AwsS3Sender(Config config)
    {
        config.validateValues();
        this.config = config;
        this.retryPolicy =
                new RetryPolicy().
                        retryOn(ex -> {
                            if (ex == null) {
                                // Success. Shouldn't retry.
                                return false;
                            }

                            ErrorHandler errorHandler = config.getErrorHandler();

                            if (errorHandler != null) {
                                errorHandler.handle(ex);
                            }

                            if (ex instanceof InterruptedException || ex instanceof NonRetryableException) {
                                return false;
                            }

                            return true;
                        }).
                        withBackoff(
                                getRetryInternalMs(),
                                getMaxRetryInternalMs(),
                                TimeUnit.MILLISECONDS,
                                getRetryFactor()).
                        withMaxRetries(getRetryMax());

        this.client = buildClient();
    }

    @VisibleForTesting
    protected S3Client buildClient()
    {
        S3ClientBuilder clientBuilder = S3Client.builder();
        if (config.getEndpoint() != null) {
            try {
                URI uri = new URI(config.getEndpoint());
                clientBuilder.endpointOverride(uri);
            }
            catch (URISyntaxException e) {
                throw new NonRetryableException(
                        String.format("Invalid endpoint. %s", config.getEndpoint()), e);
            }
        }

        if (config.getRegion() != null) {
            clientBuilder.region(Region.of(config.getRegion()));
        }

        if (config.getAwsAccessKeyId() != null && config.getAwsSecretAccessKey() != null) {
            AwsBasicCredentials credentials =
                    AwsBasicCredentials.create(config.getAwsAccessKeyId(), config.getAwsSecretAccessKey());
            clientBuilder.credentialsProvider(StaticCredentialsProvider.create(credentials));
        }

        return clientBuilder.build();
    }

    public S3Client getClient()
    {
        return client;
    }

    public int getRetryInternalMs()
    {
        return config.getRetryIntervalMs();
    }

    public int getMaxRetryInternalMs()
    {
        return config.getMaxRetryIntervalMs();
    }

    public float getRetryFactor()
    {
        return config.getRetryFactor();
    }

    public int getRetryMax()
    {
        return config.getRetryMax();
    }

    public int getWorkBufSize()
    {
        return config.getWorkBufSize();
    }

    private void copyStreams(InputStream in, OutputStream out)
            throws IOException
    {
        byte[] buf = new byte[getWorkBufSize()];
        while (true) {
            int readLen = in.read(buf);
            if (readLen < 0) {
                break;
            }
            out.write(buf, 0, readLen);
        }
    }

    private void uploadData(String bucket, String key, File file)
    {
        LOG.debug("Upload data to S3: bucket={}, key={}, fileSize={}",
                bucket, key, file.length());

        try {
            PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key);
            client.putObject(builder.build(), RequestBody.fromFile(file));
        }
        catch (NonRetryableException e) {
            throw e;
        }
        catch (Throwable e) {
            throw new RetryableException(
                    String.format("Failed to upload data. bucket=%s, key=%s", bucket, key), e);
        }
    }

    protected String getBucket(String tag)
    {
        return tag;
    }

    protected String getKeyPrefix(String tag)
    {
        // TODO: Reuse these instances
        // TODO: Make this configurable
        ZonedDateTime now = ZonedDateTime.now(Clock.systemUTC());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm-ss-SSSSSS");
        return now.format(formatter);
    }

    protected String getKeySuffix(String tag)
    {
        // TODO: Make this configurable
        return ".msgpack.gz";
    }

    public void send(String tag, ByteBuffer dataBuffer)
            throws IOException
    {
        // TODO: Support other format
        File file = File.createTempFile("tmp-fluency-", ".msgpack.gz");
        try {
            try (InputStream in = new ByteBufferBackedInputStream(dataBuffer);
                    OutputStream out = new GZIPOutputStream(
                            Files.newOutputStream(
                                    file.toPath(),
                                    StandardOpenOption.WRITE))) {

                copyStreams(in, out);
            }

            String bucket = getBucket(tag);
            String key = getKeyPrefix(tag) + getKeySuffix(tag);
            Failsafe.with(retryPolicy).run(() -> uploadData(bucket, key, file));
        }
        finally {
            if (!file.delete()) {
                LOG.warn("Failed to delete a temp file: {}", file.getAbsolutePath());
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
    }

    public static class Config
            extends Sender.Config
            implements Validatable
    {
        private String endpoint;
        private String region;
        private String awsAccessKeyId;
        private String awsSecretAccessKey;

        @Min(10)
        private int retryIntervalMs = 1000;
        @Min(10)
        private int maxRetryIntervalMs = 30000;
        @DecimalMin("1.0")
        private float retryFactor = 2;
        @Min(0)
        private int retryMax = 10;
        @Min(1024)
        private int workBufSize = 8192;

        public String getEndpoint()
        {
            return endpoint;
        }

        public void setEndpoint(String endpoint)
        {
            this.endpoint = endpoint;
        }

        public String getRegion()
        {
            return region;
        }

        public void setRegion(String region)
        {
            this.region = region;
        }

        public String getAwsAccessKeyId()
        {
            return awsAccessKeyId;
        }

        public void setAwsAccessKeyId(String awsAccessKeyId)
        {
            this.awsAccessKeyId = awsAccessKeyId;
        }

        public String getAwsSecretAccessKey()
        {
            return awsSecretAccessKey;
        }

        public void setAwsSecretAccessKey(String awsSecretAccessKey)
        {
            this.awsSecretAccessKey = awsSecretAccessKey;
        }

        public int getRetryIntervalMs()
        {
            return retryIntervalMs;
        }

        public void setRetryIntervalMs(int retryIntervalMs)
        {
            this.retryIntervalMs = retryIntervalMs;
        }

        public int getMaxRetryIntervalMs()
        {
            return maxRetryIntervalMs;
        }

        public void setMaxRetryIntervalMs(int maxRetryIntervalMs)
        {
            this.maxRetryIntervalMs = maxRetryIntervalMs;
        }

        public float getRetryFactor()
        {
            return retryFactor;
        }

        public void setRetryFactor(float retryFactor)
        {
            this.retryFactor = retryFactor;
        }

        public int getRetryMax()
        {
            return retryMax;
        }

        public void setRetryMax(int retryMax)
        {
            this.retryMax = retryMax;
        }

        public int getWorkBufSize()
        {
            return workBufSize;
        }

        public void setWorkBufSize(int workBufSize)
        {
            this.workBufSize = workBufSize;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "endpoint='" + endpoint + '\'' +
                    ", region='" + region + '\'' +
                    ", retryIntervalMs=" + retryIntervalMs +
                    ", maxRetryIntervalMs=" + maxRetryIntervalMs +
                    ", retryFactor=" + retryFactor +
                    ", retryMax=" + retryMax +
                    ", workBufSize=" + workBufSize +
                    "} " + super.toString();
        }

        void validateValues()
        {
            validate();
        }

    }
}
