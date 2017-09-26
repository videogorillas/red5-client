/*
 * RED5 Open Source Flash Server - https://github.com/Red5/
 * 
 * Copyright 2006-2015 by respective authors (see below). All rights reserved.
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

package org.red5.client.net.rtmpt;

import static org.apache.commons.io.output.NullOutputStream.NULL_OUTPUT_STREAM;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.mina.core.buffer.IoBuffer;
import org.red5.client.net.rtmp.OutboundHandshake;
import org.red5.client.net.rtmp.RTMPConnManager;
import org.red5.server.api.Red5;
import org.red5.server.net.rtmp.RTMPConnection;
import org.red5.server.net.rtmp.codec.RTMP;
import org.red5.server.util.HttpConnectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.ByteString;

/**
 * Client connector for RTMPT
 * 
 * @author Anton Lebedevich (mabrek@gmail.com)
 * @author Paul Gregoire (mondain@gmail.com)
 */
public class RTMPTClientConnector extends Thread {

    private static final Logger log = LoggerFactory.getLogger(RTMPTClientConnector.class);

    protected static final String CONTENT_TYPE = "application/x-fcs";
    protected static final MediaType ContentType = MediaType.parse(CONTENT_TYPE);

    protected static final byte[] ZERO_REQUEST_ENTITY = new byte[] { 0 };

    /**
     * Size to split messages queue by, borrowed from RTMPTServlet.RESPONSE_TARGET_SIZE
     */
    protected static final int SEND_TARGET_SIZE = 32768;

    protected OkHttpClient httpClient;

    protected HttpUrl targetHost;

    protected RTMPTClient client;

    protected String sessionId;

    protected long messageCount = 1;

    protected volatile boolean stopRequested = false;

    {
        httpClient = HttpConnectionUtil.getClient();
    }

    protected RTMPTClientConnector() {
        // default ctor for extension purposes
    }

    public RTMPTClientConnector(String server, int port, RTMPTClient client) {
        targetHost = new HttpUrl.Builder().host(server).port(port).scheme("http").build();
        this.client = client;
    }

    @Override
    public void run() {
        Call call = null;
        try {
            RTMPTClientConnection conn = openConnection();
            // set a reference to the connection on the client
            client.setConnection((RTMPConnection) conn);
            // set thread local
            Red5.setConnectionLocal(conn);
            while (!conn.isClosing() && !stopRequested) {
                IoBuffer toSend = conn.getPendingMessages(SEND_TARGET_SIZE);
                int limit = toSend != null ? toSend.limit() : 0;
                Builder post = null;
                if (toSend != null && limit > 0) {
                    post = makePost("send");
                    post.post(RequestBody.create(ContentType, ByteString.read(toSend.asInputStream(), limit)));
                } else {
                    post = makePost("idle");
                    post.post(RequestBody.create(ContentType, ZERO_REQUEST_ENTITY));
                }
                // execute
                call = httpClient.newCall(post.build());
                Response response = call.execute();
                // check for error
                checkResponseCode(response);
                // handle data
                ResponseBody body = response.body();
                byte[] received = body == null ? new byte[0] : body.bytes();
                // wrap the bytes
                IoBuffer data = IoBuffer.wrap(received);
                log.debug("State: {}", RTMP.states[conn.getStateCode()]);
                // ensure handshake is done
                if (conn.hasAttribute(RTMPConnection.RTMP_HANDSHAKE)) {
                    client.messageReceived(data);
                    continue;
                }
                if (data.limit() > 0) {
                    data.skip(1); // XXX: polling interval lies in this byte
                }
                List<?> messages = conn.decode(data);
                if (messages == null || messages.isEmpty()) {
                    try {
                        // XXX handle polling delay
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        if (stopRequested) {
                            call.cancel();
                            break;
                        }
                    }
                    continue;
                }
                for (Object message : messages) {
                    try {
                        client.messageReceived(message);
                    } catch (Exception e) {
                        log.error("Could not process message", e);
                    }
                }
            }
            finalizeConnection();
            client.connectionClosed(conn);
        } catch (Throwable e) {
            log.debug("RTMPT handling exception", e);
            client.handleException(e);
            if (call != null) {
                call.cancel();
            }
        } finally {
            Red5.setConnectionLocal(null);
        }
    }

    /**
     * @return the sessionId
     */
    public String getSessionId() {
        return sessionId;
    }

    private RTMPTClientConnection openConnection() throws IOException {
        RTMPTClientConnection conn = null;
        Builder openPost = getPost(targetHost.resolve("/open/1"));
        setCommonHeaders(openPost);
        openPost.post(RequestBody.create(ContentType, ZERO_REQUEST_ENTITY));
        // execute
        Response response = httpClient.newCall(openPost.build()).execute();
        checkResponseCode(response);
        // get the response entity
        ResponseBody entity = response.body();
        if (entity != null) {
            String responseStr = entity.string();
            sessionId = responseStr.substring(0, responseStr.length() - 1);
            log.debug("Got an id {}", sessionId);
            // create a new connection
            conn = (RTMPTClientConnection) RTMPConnManager.getInstance().createConnection(RTMPTClientConnection.class, sessionId);
            log.debug("Got session id {} from connection", conn.getSessionId());
            // client state
            conn.setHandler(client);
            conn.setDecoder(client.getDecoder());
            conn.setEncoder(client.getEncoder());
            // create an outbound handshake
            OutboundHandshake outgoingHandshake = new OutboundHandshake();
            // set the handshake type
            outgoingHandshake.setHandshakeType(RTMPConnection.RTMP_NON_ENCRYPTED);
            // add the handshake
            conn.setAttribute(RTMPConnection.RTMP_HANDSHAKE, outgoingHandshake);
            log.debug("Handshake 1st phase");
            IoBuffer handshake = outgoingHandshake.generateClientRequest1();
            conn.writeRaw(handshake);
        }
        return conn;
    }

    protected void finalizeConnection() throws IOException {
        log.debug("Sending close post");
        Builder closePost = getPost(targetHost.resolve(makeUrl("close")));
        closePost.post(RequestBody.create(ContentType, ZERO_REQUEST_ENTITY));
        Response response = httpClient.newCall(closePost.build()).execute();
        ResponseBody body = response.body();
        if (body != null) {
            IOUtils.copy(body.byteStream(), NULL_OUTPUT_STREAM);
        }
    }

    protected static Request.Builder getPost(HttpUrl url) {
        return new Request.Builder().url(url);
        //        post.setProtocolVersion(HttpVersion.HTTP_1_1);
    }

    protected Request.Builder makePost(String command) {
        Builder post = getPost(targetHost.resolve(makeUrl(command)));
        setCommonHeaders(post);
        return post;
    }

    protected String makeUrl(String command) {
        // use message count from connection
        return String.format("/%s/%s/%s", command, sessionId, messageCount++);
    }

    protected static void setCommonHeaders(Request.Builder post) {
        post.addHeader("Connection", "Keep-Alive");
        post.addHeader("Cache-Control", "no-cache");
    }

    protected static void checkResponseCode(Response response) throws IOException {
        int code = response.code();
        if (code != 200) {
            ResponseBody body = response.body();
            throw new RuntimeException("Bad HTTP status returned, line: " + response.message() + "; body: " + (body == null ? "null" : body.string()));
        }
    }

    public void setStopRequested(boolean stopRequested) {
        this.stopRequested = stopRequested;
    }
}
