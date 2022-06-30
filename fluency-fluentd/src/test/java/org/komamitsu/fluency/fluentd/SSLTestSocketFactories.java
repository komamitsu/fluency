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

package org.komamitsu.fluency.fluentd;

import org.komamitsu.fluency.fluentd.ingester.sender.SSLSocketBuilder;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.*;
import java.security.cert.CertificateException;

public class SSLTestSocketFactories
{
    private static final String KEYSTORE_PASSWORD = "keypassword";
    private static final String KEY_PASSWORD = "keypassword";
    private static final String TRUSTSTORE_PASSWORD = "trustpassword";

    public static final SSLSocketFactory SSL_CLIENT_SOCKET_FACTORY = createClientSocketFactory();

    private static SSLSocketFactory createClientSocketFactory()
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

    public static SSLServerSocket createServerSocket()
            throws IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, KeyManagementException
    {
        String trustStorePath = SSLSocketBuilder.class.getClassLoader().getResource("truststore.jks").getFile();
        System.getProperties().setProperty("javax.net.ssl.trustStore", trustStorePath);

        String keyStorePath = SSLSocketBuilder.class.getClassLoader().getResource("keystore.jks").getFile();

        try (InputStream keystoreStream = new FileInputStream(keyStorePath)) {
            KeyStore keystore = KeyStore.getInstance("JKS");

            keystore.load(keystoreStream, KEYSTORE_PASSWORD.toCharArray());
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

            keyManagerFactory.init(keystore, KEY_PASSWORD.toCharArray());

            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());

            SSLServerSocket serverSocket = (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket();
            serverSocket.setEnabledCipherSuites(serverSocket.getSupportedCipherSuites());
            serverSocket.bind(new InetSocketAddress(0));

            return serverSocket;
        }
    }
}