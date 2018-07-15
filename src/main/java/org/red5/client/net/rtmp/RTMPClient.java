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

package org.red5.client.net.rtmp;

import java.net.*;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.mina.core.future.CloseFuture;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.red5.io.utils.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.Dns;

/**
 * RTMP client implementation supporting "rtmp" and "rtmpe" protocols.
 * 
 * @author The Red5 Project
 * @author Christian Eckerle (ce@publishing-etc.de)
 * @author Joachim Bauch (jojo@struktur.de)
 * @author Paul Gregoire (mondain@gmail.com)
 * @author Steven Gong (steven.gong@gmail.com)
 * @author Anton Lebedevich (mabrek@gmail.com)
 * @author Tiago Daniel Jacobs (tiago@imdt.com.br)
 * @author Jon Valliere
 */
public class RTMPClient extends BaseRTMPClientHandler {
    private final static ExecutorService dnsExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        AtomicInteger counter = new AtomicInteger();
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("RTMPClient-dns-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    });
    private static final Logger log = LoggerFactory.getLogger(RTMPClient.class);
    private final Dns dns;

    // I/O handler
    protected RTMPMinaIoHandler ioHandler;

    // Socket connector, disposed on disconnect
    protected SocketConnector socketConnector;

    // ConnectFuture
    protected ConnectFuture future;

    // Connected IoSession
    IoSession session;

    protected long timeoutMsec = 7000L;

    /** Constructs a new RTMPClient. */
    public RTMPClient() {
        this(Dns.SYSTEM);
        ioHandler = new RTMPMinaIoHandler();
        ioHandler.setHandler(this);
    }

    /** Constructs a new RTMPClient. */
    public RTMPClient(Dns dns) {
        this.dns = dns;
        ioHandler = new RTMPMinaIoHandler();
        ioHandler.setHandler(this);
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, Object> makeDefaultConnectionParams(String server, int port, String application) {
        Map<String, Object> params = super.makeDefaultConnectionParams(server, port, application);
        if (!params.containsKey("tcUrl")) {
            params.put("tcUrl", String.format("%s://%s:%s/%s", protocol, server, port, application));
        }
        return params;
    }

    /** {@inheritDoc} */
    @Override
    protected void startConnector(String server, int port) {
        socketConnector = new NioSocketConnector();
        socketConnector.setConnectTimeoutMillis(timeoutMsec);
        SocketSessionConfig sessionConfig = socketConnector.getSessionConfig();
        int timeoutSec = Ints.checkedCast(timeoutMsec / 1000);
        sessionConfig.setBothIdleTime(timeoutSec);
        sessionConfig.setReaderIdleTime(timeoutSec);
        sessionConfig.setWriterIdleTime(timeoutSec);
        sessionConfig.setWriteTimeout(timeoutSec);
        socketConnector.setHandler(ioHandler);
        Inet4Address ipAddr = lookup(server);
        if (ipAddr == null) {
            return;
        }
        InetSocketAddress address = new InetSocketAddress(ipAddr, port);

        long start = System.currentTimeMillis();
        future = socketConnector.connect(address);
        long time = System.currentTimeMillis() - start;
        log.debug("socketConnector.connect {} took {}msec", address, time);
        future.addListener(new IoFutureListener<ConnectFuture>() {
            @Override
            public void operationComplete(ConnectFuture future) {
                try {
                    // will throw RuntimeException after connection error
                    session = future.getSession();
                } catch (Throwable e) {
                    socketConnector.dispose(false);
                    // if there isn't an ClientExceptionHandler set, a RuntimeException may be thrown in handleException
                    handleException(e);
                }
            }
        });
        // Now wait for the connect to be completed
        boolean finished = future.awaitUninterruptibly(timeoutMsec);
        if (!finished) {
            log.error("cant connect to {} connect timeout {} reached ", server, timeoutMsec);
            handleException(new ConnectException("Connection timed out."));
        }
    }

    /**
     * Try to find IPv4 address
     * due to bug in custom android firmware (Flyme)
     * also covers socket.connect timeout if server name is provided and dns is slow
     *
     * @param server
     * @return null if not found or timeout or ipv4 address of server
     */
    private Inet4Address lookup(String server) {
        Future<Inet4Address> ipaddrFuture = dnsExecutor.submit(new Callable<Inet4Address>() {
            @Override
            public Inet4Address call() throws Exception {
                for (InetAddress addr : dns.lookup(server)) {
                    if (addr instanceof Inet4Address)
                        return (Inet4Address) addr;
                }
                throw new UnknownHostException("No IPv4 address found for " + server);
            }
        });
        try {
            return ipaddrFuture.get(timeoutMsec, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.error("cant lookup ip addr of {}", server, e);
            } else {
                log.error("cant lookup ip addr of {} {}", server, getRootCause(e).toString());
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            handleException(e);
        }
        return null;
    }

    public static Throwable getRootCause(Throwable throwable) {
        Throwable cause;
        while ((cause = throwable.getCause()) != null) {
            throwable = cause;
        }
        return throwable;
    }

    /** {@inheritDoc} */
    @Override
    public void disconnect() {
        if (future != null) {
            try {
                // session will be null if connect failed
                if (session != null) {
                    // close, now
                    CloseFuture closeFuture = session.closeNow();
                    // now wait for the close to be completed
                    if (closeFuture.await(1000, TimeUnit.MILLISECONDS)) {
                        if (!future.isCanceled()) {
                            if (future.cancel()) {
                                log.debug("Connect future cancelled after close future");
                            }
                        }
                    }
                } else if (future.cancel()) {
                    log.debug("Connect future cancelled");
                }
            } catch (Exception e) {
                log.warn("Exception during disconnect", e);
            } finally {
                // we can now dispose the connector
                socketConnector.dispose(false);
            }
        }
        super.disconnect();
    }

    /**
     * Sets the RTMP protocol, the default is "rtmp". If "rtmps" or "rtmpt" are required, the appropriate client type should be selected.
     * 
     * @param protocol
     *            the protocol to set
     * @throws Exception
     */
    @Override
    public void setProtocol(String protocol) throws Exception {
        this.protocol = protocol;
        if ("rtmps".equals(protocol) || "rtmpt".equals(protocol) || "rtmpte".equals(protocol) || "rtmfp".equals(protocol)) {
            throw new Exception("Unsupported protocol specified, please use the correct client for the intended protocol.");
        }
    }

    @Override
    public void setTimeout(long time, TimeUnit unit) {
        this.timeoutMsec =  unit.toMillis(time);
    }

}
