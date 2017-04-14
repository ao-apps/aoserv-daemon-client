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
import com.aoindustries.util.AoArrays;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import javax.net.ssl.SSLSocketFactory;

/**
 * A connection between an AOServ Daemon Client and an AOServ Daemon.
 *
 * @author  AO Industries, Inc.
 */
final public class AOServDaemonConnection {

	/**
	 * The set of supported versions, with the most preferred versions first.
	 */
	private static final AOServDaemonProtocol.Version[] SUPPORTED_VERSIONS = {
		AOServDaemonProtocol.Version.VERSION_1_80_1,
		AOServDaemonProtocol.Version.VERSION_1_80_0,
		AOServDaemonProtocol.Version.VERSION_1_77
	};

	private static Socket connect(AOServDaemonConnector connector) throws IOException {
		if(connector.protocol.equals(Protocol.AOSERV_DAEMON)) {
			assert connector.port.getProtocol() == com.aoindustries.net.Protocol.TCP;
			Socket socket = new Socket();
			socket.setKeepAlive(true);
			socket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
			socket.setTcpNoDelay(true);
			if(!connector.local_ip.isUnspecified()) socket.bind(new InetSocketAddress(connector.local_ip.toString(), 0));
			socket.connect(new InetSocketAddress(connector.hostname.toString(), connector.port.getPort()), AOPool.DEFAULT_CONNECT_TIMEOUT);
			if(Thread.interrupted()) throw new InterruptedIOException();
			return socket;
		} else if(connector.protocol.equals(Protocol.AOSERV_DAEMON_SSL)) {
			assert connector.port.getProtocol() == com.aoindustries.net.Protocol.TCP;
			if(connector.trustStore != null && !connector.trustStore.isEmpty()) System.setProperty("javax.net.ssl.trustStore", connector.trustStore);
			if(connector.trustStorePassword != null && !connector.trustStorePassword.isEmpty()) System.setProperty("javax.net.ssl.trustStorePassword", connector.trustStorePassword);
			SSLSocketFactory sslFact = (SSLSocketFactory)SSLSocketFactory.getDefault();
			if(Thread.interrupted()) throw new InterruptedIOException();
			Socket regSocket = new Socket();
			regSocket.setKeepAlive(true);
			regSocket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
			regSocket.setTcpNoDelay(true);
			if(!connector.local_ip.isUnspecified()) regSocket.bind(new InetSocketAddress(connector.local_ip.toString(), 0));
			regSocket.connect(new InetSocketAddress(connector.hostname.toString(), connector.port.getPort()), AOPool.DEFAULT_CONNECT_TIMEOUT);
			if(Thread.interrupted()) throw new InterruptedIOException();
			Socket socket = sslFact.createSocket(regSocket, connector.hostname.toString(), connector.port.getPort(), true);
			if(Thread.interrupted()) throw new InterruptedIOException();
			return socket;
		} else throw new IllegalArgumentException("Unsupported protocol: "+connector.protocol);
	}

	/**
	 * Closes a socket while logging any {@link IOException} as a warning.
	 */
	private static void close(
		AOServDaemonConnector connector,
		InputStream i,
		OutputStream o,
		Socket s
	) {
		if(i != null) {
			try {
				i.close();
			} catch(IOException err) {
				connector.getLogger().log(Level.WARNING, null, err);
			}
		}
		if(o != null) {
			try {
				o.close();
			} catch(IOException err) {
				connector.getLogger().log(Level.WARNING, null, err);
			}
		}
		if(s != null) {
			try {
				s.close();
			} catch(IOException err) {
				connector.getLogger().log(Level.WARNING, null, err);
			}
		}
	}

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
	 * The negotiated protocol version.
	 */
	final AOServDaemonProtocol.Version protocolVersion;

	/**
	 * The first command sequence for this connection.
	 */
	//private final long startSeq;

	/**
	 * The next command sequence that will be sent.
	 */
	private final AtomicLong seq;

	/**
	 * Creates a new <code>AOServConnection</code>.
	 *
	 * // TODO: Once all daemons are running > version 1.77, can simplify this considerably
	 */
	protected AOServDaemonConnection(AOServDaemonConnector connector) throws InterruptedIOException, IOException {
		Socket newSocket = null;
		CompressedDataOutputStream newOut = null;
		CompressedDataInputStream newIn = null;
		AOServDaemonProtocol.Version selectedVersion = null;
		long newStartSeq = 0;
		boolean successful = false;
		try {
			newSocket = connect(connector);
			newOut = new CompressedDataOutputStream(new BufferedOutputStream(newSocket.getOutputStream()));
			newIn = new CompressedDataInputStream(new BufferedInputStream(newSocket.getInputStream()));

			// Write the most preferred version
			newOut.writeUTF(SUPPORTED_VERSIONS[0].getVersion());
			// Then connector key
			newOut.writeNullUTF(connector.key);
			// Now write additional versions.
			// This is done in this order for backwards compatibility to protocol 1.77 that only supported a single version.
			{
				newOut.writeCompressedInt(SUPPORTED_VERSIONS.length - 1);
				for(int i = 1; i < SUPPORTED_VERSIONS.length; i++) {
					newOut.writeUTF(SUPPORTED_VERSIONS[i].getVersion());
				}
			}
			newOut.flush();

			// The first boolean will tell if the version is now allowed
			if(newIn.readBoolean()) {
				// Protocol > 1.77 accepted
				// Read if the connection is allowed
				if(!newIn.readBoolean()) throw new IOException("Connection not allowed.");
				// Read the selected protocol version
				selectedVersion = AOServDaemonProtocol.Version.getVersion(newIn.readUTF());
				assert selectedVersion != AOServDaemonProtocol.Version.VERSION_1_77;
				newStartSeq = newIn.readLong();
			} else {
				// When not allowed, the server will write the set of supported versions
				String preferredVersion = newIn.readUTF();
				String[] extraVersions;
				if(preferredVersion.equals(AOServDaemonProtocol.Version.VERSION_1_77.getVersion())) {
					// Server 1.77 only sends the single preferred version
					extraVersions = AoArrays.EMPTY_STRING_ARRAY;
				} else {
					int numExtraVersions = newIn.readCompressedInt();
					extraVersions = new String[numExtraVersions];
					for(int i = 0; i < numExtraVersions; i++) {
						extraVersions[i] = newIn.readUTF();
					}
				}
				if(
					preferredVersion.equals(AOServDaemonProtocol.Version.VERSION_1_77.getVersion())
					&& AoArrays.indexOf(SUPPORTED_VERSIONS, AOServDaemonProtocol.Version.VERSION_1_77) != -1
				) {
					// Reconnect as forced protocol 1.77, since we already sent extra output incompatible with 1.77
					close(connector, newIn, newOut, newSocket);
					newSocket = connect(connector);
					newOut = new CompressedDataOutputStream(new BufferedOutputStream(newSocket.getOutputStream()));
					newIn = new CompressedDataInputStream(new BufferedInputStream(newSocket.getInputStream()));
					newOut.writeUTF(AOServDaemonProtocol.Version.VERSION_1_77.getVersion());
					newOut.writeNullUTF(connector.key);
					newOut.flush();
					if(!newIn.readBoolean()) {
						String requiredVersion = newIn.readUTF();
						throw new IOException(
							"Unsupported protocol version requested.  Requested version "
								+ AOServDaemonProtocol.Version.VERSION_1_77.getVersion()
								+ ", server requires version "
								+ requiredVersion
								+ "."
						);
					}
					// Read if the connection is allowed
					if(!newIn.readBoolean()) throw new IOException("Connection not allowed.");
					// Selected protocol version is forced 1.77 for this reconnect
					selectedVersion = AOServDaemonProtocol.Version.VERSION_1_77;
					newStartSeq = 0;
				} else {
					StringBuilder message = new StringBuilder();
					message.append("No compatible protocol versions.  Client prefers version ");
					message.append(SUPPORTED_VERSIONS[0].getVersion());
					if(SUPPORTED_VERSIONS.length > 1) {
						message.append(", but also supports versions [");
						for(int i = 1; i < SUPPORTED_VERSIONS.length; i++) {
							if(i > 1) message.append(", ");
							message.append(SUPPORTED_VERSIONS[i].getVersion());
						}
						message.append(']');
					}
					message.append(".  Server prefers version ");
					message.append(preferredVersion);
					if(extraVersions != null && extraVersions.length > 0) {
						message.append(", but also supports versions [");
						for(int i = 0; i < extraVersions.length; i++) {
							if(i > 0) message.append(", ");
							message.append(extraVersions[i]);
						}
						message.append(']');
					}
					message.append('.');
					throw new IOException(message.toString());
				}
			}
			successful = true;
		} finally {
			if(!successful) {
				close(connector, newIn, newOut, newSocket);
			}
		}
		assert successful;
		assert newSocket != null;
		assert newOut != null;
		assert newIn != null;
		assert selectedVersion != null;
		this.connector = connector;
		this.isClosed = false;
		this.socket = newSocket;
		this.out = newOut;
		this.in = newIn;
		this.protocolVersion = selectedVersion;
		//this.startSeq = newStartSeq;
		this.seq = new AtomicLong(newStartSeq);
	}

	/**
	 * Closes this connection to the server
	 * so that a reconnect is forced in the
	 * future.
	 */
	public void close() {
		close(connector, in, out, socket);
		isClosed = true;
	}

	private long currentSeq;

	/**
	 * Begins a task and gets the stream to write to the server.
	 */
	public CompressedDataOutputStream getRequestOut(int taskCode) throws IOException {
		// Increment sequence
		currentSeq = seq.getAndIncrement();
		// Send command sequence
		if(protocolVersion.compareTo(AOServDaemonProtocol.Version.VERSION_1_80_0) >= 0) {
			out.writeLong(currentSeq);
		}
		out.writeCompressedInt(taskCode);
		return out;
	}

	/**
	 * Gets the stream to read from the server.
	 */
	public CompressedDataInputStream getResponseIn() throws IOException {
		// Verify server sends matching sequence
		if(protocolVersion.compareTo(AOServDaemonProtocol.Version.VERSION_1_80_1) >= 0) {
			long serverSeq = in.readLong();
			if(serverSeq != currentSeq) throw new IOException("Sequence mismatch: " + serverSeq + " != " + currentSeq);
		}
		return in;
	}

	/**
	 * Determines if this connection has been closed.
	 */
	boolean isClosed() {
		return isClosed;
	}
}
