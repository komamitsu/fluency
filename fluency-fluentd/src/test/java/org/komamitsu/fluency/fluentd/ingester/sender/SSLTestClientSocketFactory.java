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

package org.komamitsu.fluency.fluentd.ingester.sender;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.*;

public class SSLTestClientSocketFactory
{
    private static final String TRUSTSTORE_PASSWORD = "trustpassword";

    public SSLSocketFactory create()
    {
        String trustStorePath = SSLSocketBuilder.class.getClassLoader().getResource("truststore.jks").getFile();

        try (InputStream keystoreStream = new FileInputStream(trustStorePath)) {
            KeyStore keystore = KeyStore.getInstance("JKS");

            keystore.load(keystoreStream, TRUSTSTORE_PASSWORD.toCharArray());
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

            trustManagerFactory.init(keystore);

            SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
            sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());

            return sslContext.getSocketFactory();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to create SSLSocketFactory", e);
        }
    }
}