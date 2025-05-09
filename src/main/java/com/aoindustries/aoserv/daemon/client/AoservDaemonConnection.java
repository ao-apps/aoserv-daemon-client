/*
 * aoserv-daemon-client - Java client for the AOServ Daemon.
 * Copyright (C) 2001-2009, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2024, 2025  AO Industries, Inc.
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
 * along with aoserv-daemon-client.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoindustries.aoserv.daemon.client;

import com.aoapps.collections.AoArrays;
import com.aoapps.hodgepodge.io.AOPool;
import com.aoapps.hodgepodge.io.stream.StreamableInput;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.lang.AutoCloseables;
import com.aoapps.lang.EmptyArrays;
import com.aoapps.lang.Throwables;
import com.aoindustries.aoserv.client.net.AppProtocol;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLSocketFactory;

/**
 * A connection between an AOServ Daemon Client and an AOServ Daemon.
 *
 * @author  AO Industries, Inc.
 */
public final class AoservDaemonConnection implements Closeable {

  /**
   * The set of supported versions, with the most preferred versions first.
   */
  // Matches AoservDaemonServerThread.java
  private static final AoservDaemonProtocol.Version[] SUPPORTED_VERSIONS = {
      AoservDaemonProtocol.Version.VERSION_1_84_19,
      AoservDaemonProtocol.Version.VERSION_1_84_13,
      AoservDaemonProtocol.Version.VERSION_1_84_11,
      AoservDaemonProtocol.Version.VERSION_1_83_0,
      AoservDaemonProtocol.Version.VERSION_1_81_10,
      AoservDaemonProtocol.Version.VERSION_1_80_1,
      AoservDaemonProtocol.Version.VERSION_1_80_0,
      AoservDaemonProtocol.Version.VERSION_1_77
  };

  private static Socket connect(AoservDaemonConnector connector) throws IOException {
    if (connector.protocol.equals(AppProtocol.AOSERV_DAEMON)) {
      assert connector.port.getProtocol() == com.aoapps.net.Protocol.TCP;
      Socket socket = new Socket();
      try {
        socket.setKeepAlive(true);
        socket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
        socket.setTcpNoDelay(true);
        if (!connector.localIp.isUnspecified()) {
          socket.bind(new InetSocketAddress(connector.localIp.toString(), 0));
        }
        socket.connect(new InetSocketAddress(connector.hostname.toString(), connector.port.getPort()), AOPool.DEFAULT_CONNECT_TIMEOUT);
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedIOException();
        }
        return socket;
      } catch (Throwable t) {
        throw AutoCloseables.closeAndWrap(t, IOException.class, IOException::new, socket);
      }
    } else if (connector.protocol.equals(AppProtocol.AOSERV_DAEMON_SSL)) {
      assert connector.port.getProtocol() == com.aoapps.net.Protocol.TCP;
      if (connector.trustStore != null && !connector.trustStore.isEmpty()) {
        System.setProperty("javax.net.ssl.trustStore", connector.trustStore);
      }
      if (connector.trustStorePassword != null && !connector.trustStorePassword.isEmpty()) {
        System.setProperty("javax.net.ssl.trustStorePassword", connector.trustStorePassword);
      }
      SSLSocketFactory sslFact = (SSLSocketFactory) SSLSocketFactory.getDefault();
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedIOException();
      }
      Socket socket = new Socket();
      try {
        socket.setKeepAlive(true);
        socket.setSoLinger(true, AOPool.DEFAULT_SOCKET_SO_LINGER);
        socket.setTcpNoDelay(true);
        if (!connector.localIp.isUnspecified()) {
          socket.bind(new InetSocketAddress(connector.localIp.toString(), 0));
        }
        socket.connect(new InetSocketAddress(connector.hostname.toString(), connector.port.getPort()), AOPool.DEFAULT_CONNECT_TIMEOUT);
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedIOException();
        }
        socket = sslFact.createSocket(socket, connector.hostname.toString(), connector.port.getPort(), true);
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedIOException();
        }
        return socket;
      } catch (Throwable t) {
        throw AutoCloseables.closeAndWrap(t, IOException.class, IOException::new, socket);
      }
    } else {
      throw new IllegalArgumentException("Unsupported protocol: " + connector.protocol);
    }
  }

  /**
   * The connector that this connection is part of.
   */
  private final AoservDaemonConnector connector;

  /**
   * Keeps a flag of the connection status.
   */
  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  /**
   * The socket to the server.
   */
  private final Socket socket;

  /**
   * The output stream to the server.
   */
  private final StreamableOutput out;

  /**
   * The input stream from the server.
   */
  private final StreamableInput in;

  /**
   * The negotiated protocol version.
   */
  private final AoservDaemonProtocol.Version protocolVersion;

  ///**
  // * The first command sequence for this connection.
  // */
  //private final long startSeq;

  /**
   * The next command sequence that will be sent.
   */
  private final AtomicLong seq;

  /**
   * Creates a new <code>AoservConnection</code>.
   *
   * <p>TODO: Once all daemons are running &gt; version 1.77, can simplify this considerably</p>
   */
  protected AoservDaemonConnection(AoservDaemonConnector connector) throws IOException {
    Socket newSocket = null;
    StreamableOutput newOut = null;
    StreamableInput newIn = null;
    AoservDaemonProtocol.Version selectedVersion = null;
    long newStartSeq = 0;
    try {
      newSocket = connect(connector);
      newOut = new StreamableOutput(new BufferedOutputStream(newSocket.getOutputStream()));
      newIn = new StreamableInput(new BufferedInputStream(newSocket.getInputStream()));

      // Write the most preferred version
      newOut.writeUTF(SUPPORTED_VERSIONS[0].getVersion());
      // Then connector key
      if (connector.key == null) {
        newOut.writeShort(0);
      } else {
        final StreamableOutput newOutFinal = newOut;
        connector.key.accept(key -> {
          if (key.length == 0) {
            throw new AssertionError("key.length == 0");
          }
          if (key.length >= (1 << Short.SIZE)) {
            throw new AssertionError("key.length >= (1 << Short.SIZE)");
          }
          newOutFinal.writeShort(key.length);
          newOutFinal.write(key);
        });
      }
      // Now write additional versions.
      // This is done in this order for backwards compatibility to protocol 1.77 that only supported a single version.
      {
        newOut.writeCompressedInt(SUPPORTED_VERSIONS.length - 1);
        for (int i = 1; i < SUPPORTED_VERSIONS.length; i++) {
          newOut.writeUTF(SUPPORTED_VERSIONS[i].getVersion());
        }
      }
      newOut.flush();

      // The first boolean will tell if the version is now allowed
      if (newIn.readBoolean()) {
        // Protocol > 1.77 accepted
        // Read if the connection is allowed
        if (!newIn.readBoolean()) {
          throw new IOException("Connection not allowed.");
        }
        // Read the selected protocol version
        selectedVersion = AoservDaemonProtocol.Version.getVersion(newIn.readUTF());
        assert selectedVersion != AoservDaemonProtocol.Version.VERSION_1_77;
        newStartSeq = newIn.readLong();
      } else {
        // When not allowed, the server will write the set of supported versions
        String preferredVersion = newIn.readUTF();
        String[] extraVersions;
        if (preferredVersion.equals(AoservDaemonProtocol.Version.VERSION_1_77.getVersion())) {
          // Server 1.77 only sends the single preferred version
          extraVersions = EmptyArrays.EMPTY_STRING_ARRAY;
        } else {
          int numExtraVersions = newIn.readCompressedInt();
          extraVersions = new String[numExtraVersions];
          for (int i = 0; i < numExtraVersions; i++) {
            extraVersions[i] = newIn.readUTF();
          }
        }
        if (
            preferredVersion.equals(AoservDaemonProtocol.Version.VERSION_1_77.getVersion())
                && AoArrays.indexOf(SUPPORTED_VERSIONS, AoservDaemonProtocol.Version.VERSION_1_77) != -1
        ) {
          // Reconnect as forced protocol 1.77, since we already sent extra output incompatible with 1.77
          final Throwable closeT = AutoCloseables.closeAndCatch(newIn, newOut, newSocket);
          newIn = null;
          newOut = null;
          newSocket = null;
          if (closeT != null) {
            throw closeT;
          }
          newSocket = connect(connector);
          newOut = new StreamableOutput(new BufferedOutputStream(newSocket.getOutputStream()));
          newIn = new StreamableInput(new BufferedInputStream(newSocket.getInputStream()));
          newOut.writeUTF(AoservDaemonProtocol.Version.VERSION_1_77.getVersion());
          String daemonKey;
          if (connector.key == null) {
            daemonKey = null;
          } else {
            daemonKey = connector.key.invoke(Base64.getEncoder()::encodeToString);
          }
          newOut.writeNullUTF(daemonKey);
          newOut.flush();
          if (!newIn.readBoolean()) {
            String requiredVersion = newIn.readUTF();
            throw new IOException(
                "Unsupported protocol version requested.  Requested version "
                    + AoservDaemonProtocol.Version.VERSION_1_77.getVersion()
                    + ", server requires version "
                    + requiredVersion
                    + "."
            );
          }
          // Read if the connection is allowed
          if (!newIn.readBoolean()) {
            throw new IOException("Connection not allowed.");
          }
          // Selected protocol version is forced 1.77 for this reconnect
          selectedVersion = AoservDaemonProtocol.Version.VERSION_1_77;
          newStartSeq = 0;
        } else {
          StringBuilder message = new StringBuilder();
          message.append("No compatible protocol versions.  Client prefers version ");
          message.append(SUPPORTED_VERSIONS[0].getVersion());
          if (SUPPORTED_VERSIONS.length > 1) {
            message.append(", but also supports versions [");
            for (int i = 1; i < SUPPORTED_VERSIONS.length; i++) {
              if (i > 1) {
                message.append(", ");
              }
              message.append(SUPPORTED_VERSIONS[i].getVersion());
            }
            message.append(']');
          }
          message.append(".  Server prefers version ");
          message.append(preferredVersion);
          if (extraVersions != null && extraVersions.length > 0) {
            message.append(", but also supports versions [");
            for (int i = 0; i < extraVersions.length; i++) {
              if (i > 0) {
                message.append(", ");
              }
              message.append(extraVersions[i]);
            }
            message.append(']');
          }
          message.append('.');
          throw new IOException(message.toString());
        }
      }
      assert newSocket != null;
      assert newOut != null;
      assert newIn != null;
      assert selectedVersion != null;
      this.connector = connector;
      this.isClosed.set(false);
      this.socket = newSocket;
      this.out = newOut;
      this.in = newIn;
      this.protocolVersion = selectedVersion;
      //this.startSeq = newStartSeq;
      this.seq = new AtomicLong(newStartSeq);
    } catch (Throwable t) {
      throw AutoCloseables.closeAndWrap(t, IOException.class, IOException::new, newIn, newOut, newSocket);
    }
  }

  /**
   * Releases this connection back to the pool.
   *
   * @see  AoservDaemonConnector#release(com.aoindustries.aoserv.daemon.client.AoservDaemonConnection)
   */
  @Override
  public void close() throws IOException {
    connector.release(this);
  }

  /**
   * Closes this connection to the server so that a reconnect is forced in the future.
   * Adds any new throwables to {@code t0} via {@link Throwables#addSuppressed(java.lang.Throwable, java.lang.Throwable)}.
   */
  public Throwable abort(Throwable t0) {
    if (!isClosed.getAndSet(true)) {
      t0 = AutoCloseables.closeAndCatch(t0, in, out, socket);
    }
    return t0;
  }

  /**
   * Closes this connection to the server so that a reconnect is forced in the future.
   */
  public void abort() throws IOException {
    Throwable t0 = abort(null);
    if (t0 != null) {
      throw Throwables.wrap(t0, IOException.class, IOException::new);
    }
  }

  private long currentSeq;

  /**
   * Begins a task and gets the stream to write to the server.
   */
  public StreamableOutput getRequestOut(int taskCode) throws IOException {
    // Increment sequence
    currentSeq = seq.getAndIncrement();
    // Send command sequence
    if (protocolVersion.compareTo(AoservDaemonProtocol.Version.VERSION_1_80_0) >= 0) {
      out.writeLong(currentSeq);
    }
    out.writeCompressedInt(taskCode);
    return out;
  }

  /**
   * Gets the stream to read from the server.
   */
  public StreamableInput getResponseIn() throws IOException {
    // Verify server sends matching sequence
    if (protocolVersion.compareTo(AoservDaemonProtocol.Version.VERSION_1_80_1) >= 0) {
      long serverSeq = in.readLong();
      if (serverSeq != currentSeq) {
        throw new IOException("Sequence mismatch: " + serverSeq + " != " + currentSeq);
      }
    }
    return in;
  }

  /**
   * Gets the protocol negotiated for this connection.
   */
  public AoservDaemonProtocol.Version getProtocolVersion() {
    return protocolVersion;
  }

  /**
   * Determines if this connection has been closed.
   */
  boolean isClosed() {
    return isClosed.get();
  }
}
