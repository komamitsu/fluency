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

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SSLSocketBuilder
{
    private final String host;
    private final int port;
    private final int connectionTimeoutMilli;
    private final int readTimeoutMilli;

    public SSLSocketBuilder(String host, Integer port, int connectionTimeoutMilli, int readTimeoutMilli)
    {
        this.host = host;
        this.port = port;
        this.connectionTimeoutMilli = connectionTimeoutMilli;
        this.readTimeoutMilli = readTimeoutMilli;
    }

    public SSLSocket build()
            throws IOException
    {
        Socket socket = SSLSocketFactory.getDefault().createSocket();
        try {
            socket.connect(new InetSocketAddress(host, port), connectionTimeoutMilli);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(readTimeoutMilli);
        }
        catch (Throwable e) {
            socket.close();
            throw e;
        }
        return (SSLSocket) socket;
    }

    @Override
    public String toString()
    {
        return "SSLSocketBuilder{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
