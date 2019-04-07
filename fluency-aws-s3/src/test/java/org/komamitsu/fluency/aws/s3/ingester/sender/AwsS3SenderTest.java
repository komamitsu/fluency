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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class AwsS3SenderTest
{
    @Test
    void buildClientWithDefaults()
    {
        AwsS3Sender.Config config = new AwsS3Sender.Config();
        config.setKeySuffix("foo/bar");

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
        doReturn(s3Client).when(s3ClientBuilder).build();

        new AwsS3Sender(s3ClientBuilder, config);

        verify(s3ClientBuilder, times(1)).build();
        verify(s3ClientBuilder, times(0)).endpointOverride(any());
        verify(s3ClientBuilder, times(0)).region(any());
        verify(s3ClientBuilder, times(0)).credentialsProvider(any());
    }

    @Test
    void buildClientWithCustomizedConfig()
    {
        AwsS3Sender.Config config = new AwsS3Sender.Config();
        config.setKeySuffix("foo/bar");
        config.setEndpoint("https://another.s3endpoi.nt");
        config.setRegion("ap-northeast-1");
        config.setAwsAccessKeyId("foo");
        config.setAwsSecretAccessKey("bar");

        S3Client s3Client = mock(S3Client.class);
        S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
        doReturn(s3Client).when(s3ClientBuilder).build();
        doAnswer(invocation -> {
            AwsCredentialsProvider provider = invocation.getArgument(0);
            AwsCredentials awsCredentials = provider.resolveCredentials();
            assertEquals("foo", awsCredentials.accessKeyId());
            assertEquals("bar", awsCredentials.secretAccessKey());
            return null;
        }).when(s3ClientBuilder).credentialsProvider(any());

        new AwsS3Sender(s3ClientBuilder, config);

        verify(s3ClientBuilder, times(1)).build();
        verify(s3ClientBuilder, times(1)).endpointOverride(eq(URI.create("https://another.s3endpoi.nt")));
        verify(s3ClientBuilder, times(1)).region(eq(Region.AP_NORTHEAST_1));
        verify(s3ClientBuilder, times(1)).credentialsProvider(any());
    }
}