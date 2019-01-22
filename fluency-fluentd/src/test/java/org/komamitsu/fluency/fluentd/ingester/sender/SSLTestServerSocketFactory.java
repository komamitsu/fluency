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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

public class SSLTestServerSocketFactory
{
    private static final String KEYSTORE_PASSWORD = "storepassword";
    private static final String KEY_PASSWORD = "keypassword";

    public SSLServerSocket create()
            throws IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, KeyManagementException
    {
        String trustStorePath = SSLSocketBuilder.class.getClassLoader().getResource("truststore.jks").getFile();
        System.getProperties().setProperty("javax.net.ssl.trustStore", trustStorePath);

        String keyStorePath = SSLSocketBuilder.class.getClassLoader().getResource("keystore.jks").getFile();

        InputStream keystoreStream = null;
        try {
            KeyStore keystore = KeyStore.getInstance("JKS");
            keystoreStream = new FileInputStream(new File(keyStorePath));

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
        finally {
            if (keystoreStream != null) {
                keystoreStream.close();
            }
        }
    }
}