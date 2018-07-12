/*
 * RED5 Open Source Media Server - https://github.com/Red5/
 * 
 * Copyright 2006-2016 by respective authors (see below). All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.red5.server.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * Utility for using HTTP connections.
 * 
 * @author The Red5 Project
 * @author Paul Gregoire (mondain@gmail.com)
 */
public class HttpConnectionUtil {

    private static Logger log = LoggerFactory.getLogger(HttpConnectionUtil.class);

    private static final String userAgent = "Mozilla/4.0 (compatible; Red5 Server)";

    private static ConnectionPool connectionManager;

    private static int connectionTimeout = 7000;

    static {
        // Create an HttpClient with the PoolingHttpClientConnectionManager.
        // This connection manager must be used if more than one thread will
        // be using the HttpClient.
        connectionManager = new ConnectionPool(40, 5, TimeUnit.MINUTES);
    }

    /**
     * Returns a client with all our selected properties / params.
     * 
     * @return client
     */
    public static final OkHttpClient getClient() {
        return getClient(connectionTimeout);
    }

    /**
     * Returns a client with all our selected properties / params.
     * 
     * @param timeout
     *            - socket timeout to set
     * @return client
     */
    public static final OkHttpClient getClient(long timeoutMsec) {
        Builder client = new OkHttpClient.Builder();
        // set the connection manager
        client.connectionPool(connectionManager);
        // dont retry
//        client.setRetryHandler(new DefaultHttpRequestRetryHandler(0, false));
        // establish a connection within x seconds
        client.connectTimeout(timeoutMsec, MILLISECONDS).readTimeout(timeoutMsec, MILLISECONDS).writeTimeout(timeoutMsec, MILLISECONDS);
        // no redirects
        client.followRedirects(false);
        // set custom ua
//        client.setUserAgent(userAgent);
        
        // set the proxy if the user has one set
        if ((System.getProperty("http.proxyHost") != null) && (System.getProperty("http.proxyPort") != null)) {
            client.proxy(new Proxy(Type.HTTP, InetSocketAddress.createUnresolved(System.getProperty("http.proxyHost"), Integer.parseInt(System.getProperty("http.proxyPort")))));
        }
        return client.build();
    }

    /**
     * Returns a client with all our selected properties / params and SSL enabled.
     * 
     * @return client
     */
    public static final OkHttpClient getSecureClient() {
        OkHttpClient.Builder client = new OkHttpClient.Builder();
        // set the ssl verifier to accept all
        client.hostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String x, SSLSession y) {
                return true;
            }
        });
        // set the connection manager
        client.connectionPool(connectionManager);
        // dont retry
//        client.setRetryHandler(new DefaultHttpRequestRetryHandler(0, false));
        // establish a connection within x seconds
        client.connectTimeout(connectionTimeout, MILLISECONDS).readTimeout(connectionTimeout, MILLISECONDS).writeTimeout(connectionTimeout, MILLISECONDS);
        // no redirects
        client.followRedirects(false);
        // set custom ua
//        client.setUserAgent(userAgent);
        return client.build();
    }

    /**
     * Logs details about the request error.
     * 
     * @param response
     *            http response
     * @throws IOException
     *             on IO error
     * @throws ParseException
     *             on parse error
     */
    public static void handleError(Response response) throws IOException {
        log.debug("{} {}", response.code(), response.message());
        ResponseBody entity = response.body();
        if (entity != null) {
            log.debug("{}", entity.string());
            entity.close();
        }
    }

    /**
     * @return the connectionTimeout
     */
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @param connectionTimeout
     *            the connectionTimeout to set
     */
    public void setConnectionTimeout(int connectionTimeout) {
        HttpConnectionUtil.connectionTimeout = connectionTimeout;
    }

}
