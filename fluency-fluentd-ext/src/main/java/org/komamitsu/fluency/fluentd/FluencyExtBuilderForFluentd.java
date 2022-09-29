/*
 * Copyright 2022 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.fluency.fluentd;

import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.fluentd.ingester.sender.FluentdSender;
import org.komamitsu.fluency.fluentd.ingester.sender.MultiSender;
import org.komamitsu.fluency.fluentd.ingester.sender.UnixSocketSender;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.UnixSocketHeartbeater;

import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class FluencyExtBuilderForFluentd
        extends FluencyBuilderForFluentd
{
    public Fluency buildFromUnixSocketPath(Path socketPath)
    {
        return buildFromIngester(
                recordFormatter,
                buildIngester(createBaseSender(socketPath, false)));
    }

    public Fluency buildFromUnixSocketPaths(List<Path> socketPaths)
    {
        return buildFromIngester(
                recordFormatter,
                buildIngester(new MultiSender(
                        socketPaths.stream().map(path -> createBaseSender(path, true)).toList())));
    }

    private FluentdSender createBaseSender(Path path, boolean withHeartBeater)
    {
        UnixSocketSender.Config senderConfig = new UnixSocketSender.Config();
        FailureDetector failureDetector = null;
        if (path != null) {
            senderConfig.setPath(path.toAbsolutePath());
        }
        if (withHeartBeater) {
            UnixSocketHeartbeater.Config hbConfig = new UnixSocketHeartbeater.Config();
            hbConfig.setPath(path.toAbsolutePath());
            UnixSocketHeartbeater heartbeater = new UnixSocketHeartbeater(hbConfig);
            failureDetector = new FailureDetector(new PhiAccrualFailureDetectStrategy(), heartbeater);
        }
        if (connectionTimeoutMilli != null) {
            senderConfig.setConnectionTimeoutMilli(connectionTimeoutMilli);
        }
        if (readTimeoutMilli != null) {
            senderConfig.setReadTimeoutMilli(readTimeoutMilli);
        }
        return new UnixSocketSender(senderConfig, failureDetector);
    }
}
