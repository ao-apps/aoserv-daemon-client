package com.aoindustries.aoserv.daemon.client;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Protocol;
import com.aoindustries.aoserv.daemon.client.AOServDaemonProtocol;
import com.aoindustries.io.AOPool;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.profiler.Profiler;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.Security;
import javax.net.ssl.SSLSocketFactory;

/**
 * A <code>AOServConnector</code> provides the
 * connection between the object layer and the data.
 *
 * @author  AO Industries, Inc.
 */
final public class AOServDaemonConnection {

    /**
     * The connector that this connection is part of.
     */
    private final AOServDaemonConnector connector;
    
    /**
     * Keeps a flag of the connection status.
     */
    private boolean isClosed = true;
    
    /**
     * The socket to the server.
     */
    private final Socket socket;
    
    /**
     * The output stream to the server.
     */
    private final CompressedDataOutputStream out;
    
    /**
     * The input stream from the server.
     */
    private final CompressedDataInputStream in;

    /**
     * Creates a new <code>AOServConnection</code>.
     */
    protected AOServDaemonConnection(
        AOServDaemonConnector connector
    ) throws IOException {
        Profiler.startProfile(Profiler.FAST, AOServDaemonConnection.class, "<init>(AOServDaemonConnector)", null);
        try {
            this.connector=connector;
            if(connector.protocol.equals(Protocol.AOSERV_DAEMON)) {
                socket=new Socket();
                socket.setKeepAlive(true);
                socket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
                //socket.setTcpNoDelay(true);
                socket.bind(new InetSocketAddress(connector.local_ip, 0));
                socket.connect(new InetSocketAddress(connector.hostname, connector.port), AOPool.DEFAULT_CONNECT_TIMEOUT);
            } else if(connector.protocol.equals(Protocol.AOSERV_DAEMON_SSL)) {
                synchronized(connector.sslLock) {
                    if(!connector.sslProviderLoaded[0]) {
                        if(connector.trustStore!=null && connector.trustStore.length()>0) System.setProperty("javax.net.ssl.trustStore", connector.trustStore);
                        if(connector.trustStorePassword!=null && connector.trustStorePassword.length()>0) System.setProperty("javax.net.ssl.trustStorePassword", connector.trustStorePassword);
                        Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());
                        connector.sslProviderLoaded[0]=true;
                    }
                }
                SSLSocketFactory sslFact=(SSLSocketFactory)SSLSocketFactory.getDefault();
                Socket regSocket = new Socket();
                regSocket.bind(new InetSocketAddress(connector.local_ip, 0));
                regSocket.connect(new InetSocketAddress(connector.hostname, connector.port), AOPool.DEFAULT_CONNECT_TIMEOUT);
                regSocket.setKeepAlive(true);
                regSocket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
                //regSocket.setTcpNoDelay(true);
                socket=sslFact.createSocket(regSocket, connector.hostname, connector.port, true);
            } else throw new IllegalArgumentException("Unsupported protocol: "+connector.protocol);

            isClosed=false;
            out=new CompressedDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            in=new CompressedDataInputStream(new BufferedInputStream(socket.getInputStream()));
            
            // Write the version, then connector key
            out.writeUTF(AOServDaemonProtocol.CURRENT_VERSION);
            out.writeBoolean(connector.key!=null);
            if(connector.key!=null) out.writeUTF(connector.key);
            out.flush();
            
            // The first boolean will tell if the version is now allowed
            if(!in.readBoolean()) {
                // When not allowed, the server will write the version that is required
                String requiredVersion=in.readUTF();
                throw new IOException("Unsupported protocol version requested.  Requested version "+AOServDaemonProtocol.CURRENT_VERSION+", server requires version "+requiredVersion);
            }
            // Read if the connection is allowed
            if(!in.readBoolean()) {
                close();
                throw new IOException("Connection not allowed.");
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Closes this connection to the server
     * so that a reconnect is forced in the
     * future.
     */
    public void close() {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnection.class, "close()", null);
        try {
            if(in!=null) {
                try {
                    in.close();
                } catch(IOException err) {
                    connector.getErrorHandler().reportWarning(err, null);
                }
            }
            if(out!=null) {
                try {
                    out.close();
                } catch(IOException err) {
                    connector.getErrorHandler().reportWarning(err, null);
                }
            }
            if(socket!=null) {
                try {
                    socket.close();
                } catch(IOException err) {
                    connector.getErrorHandler().reportWarning(err, null);
                }
            }
            isClosed=true;
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Gets the stream to read from the server.
     */
    public CompressedDataInputStream getInputStream() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnection.class, "getInputStream()", null);
        try {
            return in;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Gets the stream to write to the server.
     */
    public CompressedDataOutputStream getOutputStream() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnection.class, "getOutputStream()", null);
        try {
            return out;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Determines if this connection has been closed.
     */
    boolean isClosed() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnection.class, "isClosed()", null);
        try {
            return isClosed;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
}