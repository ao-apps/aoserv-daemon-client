/*
 * aoserv-daemon-client - Java client for the AOServ Daemon.
 * Copyright (C) 2001-2009, 2016, 2017  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-daemon-client.
 *
 * aoserv-daemon-client is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-daemon-client is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-daemon-client.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.daemon.client;

import com.aoindustries.aoserv.client.Protocol;
import com.aoindustries.io.AOPool;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.logging.Level;
import javax.net.ssl.SSLSocketFactory;

/**
 * A <code>AOServDaemonConnection</code> provides the
 * connection between a client and a server-control daemon.
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
	protected AOServDaemonConnection(AOServDaemonConnector connector) throws InterruptedIOException, IOException {
		this.connector=connector;
		boolean successful = false;
		try {
			if(connector.protocol.equals(Protocol.AOSERV_DAEMON)) {
				assert connector.port.getProtocol() == com.aoindustries.net.Protocol.TCP;
				if(Thread.interrupted()) throw new InterruptedIOException();
				socket=new Socket();
				socket.setKeepAlive(true);
				socket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
				//socket.setTcpNoDelay(true);
				socket.bind(new InetSocketAddress(connector.local_ip.toString(), 0));
				socket.connect(new InetSocketAddress(connector.hostname.toString(), connector.port.getPort()), AOPool.DEFAULT_CONNECT_TIMEOUT);
			} else if(connector.protocol.equals(Protocol.AOSERV_DAEMON_SSL)) {
				assert connector.port.getProtocol() == com.aoindustries.net.Protocol.TCP;
				if(connector.trustStore!=null && connector.trustStore.length()>0) System.setProperty("javax.net.ssl.trustStore", connector.trustStore);
				if(connector.trustStorePassword!=null && connector.trustStorePassword.length()>0) System.setProperty("javax.net.ssl.trustStorePassword", connector.trustStorePassword);
				SSLSocketFactory sslFact=(SSLSocketFactory)SSLSocketFactory.getDefault();
				if(Thread.interrupted()) throw new InterruptedIOException();
				Socket regSocket = new Socket();
				regSocket.bind(new InetSocketAddress(connector.local_ip.toString(), 0));
				regSocket.connect(new InetSocketAddress(connector.hostname.toString(), connector.port.getPort()), AOPool.DEFAULT_CONNECT_TIMEOUT);
				regSocket.setKeepAlive(true);
				regSocket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
				//regSocket.setTcpNoDelay(true);
				socket=sslFact.createSocket(regSocket, connector.hostname.toString(), connector.port.getPort(), true);
			} else throw new IllegalArgumentException("Unsupported protocol: "+connector.protocol);

			if(Thread.interrupted()) throw new InterruptedIOException();

			isClosed=false;
			out=new CompressedDataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			in=new CompressedDataInputStream(new BufferedInputStream(socket.getInputStream()));

			// Write the version, then connector key
			out.writeUTF(AOServDaemonProtocol.Version.CURRENT_VERSION.getVersion());
			out.writeBoolean(connector.key!=null);
			if(connector.key!=null) out.writeUTF(connector.key);
			out.flush();

			// The first boolean will tell if the version is now allowed
			if(!in.readBoolean()) {
				// When not allowed, the server will write the version that is required
				String requiredVersion = in.readUTF();
				throw new IOException(
					"Unsupported protocol version requested.  Requested version "
						+ AOServDaemonProtocol.Version.CURRENT_VERSION.getVersion()
						+ ", server requires version "
						+ requiredVersion
						+ " or higher."
				);
			}
			// Read if the connection is allowed
			if(!in.readBoolean()) throw new IOException("Connection not allowed.");
			successful = true;
		} finally {
			if(!successful) close();
		}
	}

	/**
	 * Closes this connection to the server
	 * so that a reconnect is forced in the
	 * future.
	 */
	public void close() {
		if(in!=null) {
			try {
				in.close();
			} catch(IOException err) {
				connector.getLogger().log(Level.WARNING, null, err);
			}
		}
		if(out!=null) {
			try {
				out.close();
			} catch(IOException err) {
				connector.getLogger().log(Level.WARNING, null, err);
			}
		}
		if(socket!=null) {
			try {
				socket.close();
			} catch(IOException err) {
				connector.getLogger().log(Level.WARNING, null, err);
			}
		}
		isClosed=true;
	}

	/**
	 * Gets the stream to read from the server.
	 */
	public CompressedDataInputStream getInputStream() {
		return in;
	}

	/**
	 * Gets the stream to write to the server.
	 */
	public CompressedDataOutputStream getOutputStream() {
		return out;
	}

	/**
	 * Determines if this connection has been closed.
	 */
	boolean isClosed() {
		return isClosed;
	}
}
