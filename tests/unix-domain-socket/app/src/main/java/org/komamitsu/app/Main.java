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

package org.komamitsu.app;

import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.fluentd.FluencyExtBuilderForFluentd;

import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {
        String host = args[0];
        UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(Paths.get(args[1]));
        String tag = args[2];

        FluencyExtBuilderForFluentd builder = new FluencyExtBuilderForFluentd();
        builder.setWaitUntilBufferFlushed(5000);
        builder.setWaitUntilFlusherTerminated(5000);
        builder.setSenderMaxRetryCount(8);
        Fluency fluency = builder.buildForUnixDomainSockets(Arrays.asList(socketAddress));

        Map<String, Object> event = new HashMap<>();
        event.put("name", "komamitsu");
        event.put("comment", "zzz");
        fluency.emit(tag, event);
        fluency.close();
    }
}
