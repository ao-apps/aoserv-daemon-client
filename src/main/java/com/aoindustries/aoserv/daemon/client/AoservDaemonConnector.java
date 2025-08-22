/*
 * aoserv-daemon-client - Java client for the AOServ Daemon.
 * Copyright (C) 2001-2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2025  AO Industries, Inc.
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

import com.aoapps.collections.AoCollections;
import com.aoapps.hodgepodge.io.stream.StreamableInput;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.hodgepodge.util.Tuple2;
import com.aoapps.lang.NullArgumentException;
import com.aoapps.lang.Throwables;
import com.aoapps.lang.util.BufferManager;
import com.aoapps.lang.validation.ValidationException;
import com.aoapps.net.HostAddress;
import com.aoapps.net.InetAddress;
import com.aoapps.net.Port;
import com.aoapps.security.UnprotectedKey;
import com.aoindustries.aoserv.client.backup.MysqlReplication;
import com.aoindustries.aoserv.client.email.InboxAttributes;
import com.aoindustries.aoserv.client.infrastructure.VirtualServer;
import com.aoindustries.aoserv.client.linux.PosixPath;
import com.aoindustries.aoserv.client.monitoring.AlertLevel;
import com.aoindustries.aoserv.client.mysql.Database.CheckTableResult;
import com.aoindustries.aoserv.client.mysql.Database.Engine;
import com.aoindustries.aoserv.client.mysql.Database.TableStatus;
import com.aoindustries.aoserv.client.mysql.Server;
import com.aoindustries.aoserv.client.mysql.TableName;
import com.aoindustries.aoserv.client.pki.Certificate;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A <code>AoservDaemonConnector</code> provides the
 * connections between a client and a server-control daemon.
 *
 * @author  AO Industries, Inc.
 */
public final class AoservDaemonConnector {

  private static final Logger logger = Logger.getLogger(AoservDaemonConnector.class.getName());

  /**
   * Each unique connector is only created once.
   */
  private static final List<AoservDaemonConnector> connectors = new ArrayList<>();

  /**
   * The hostname to connect to.
   */
  final HostAddress hostname;

  /**
   * The local IP address to connect from.
   */
  final InetAddress localIp;

  /**
   * The port to connect to.
   */
  final Port port;

  /**
   * The protocol used.
   */
  final String protocol;

  /**
   * The key to connect with.
   */
  final UnprotectedKey key;

  final int poolSize;

  final long maxConnectionAge;

  final String trustStore;

  final String trustStorePassword;

  private final AoservDaemonConnectionPool pool;

  /**
   * Creates a new <code>AoservConnector</code>.
   */
  private AoservDaemonConnector(
      HostAddress hostname,
      InetAddress localIp,
      Port port,
      String protocol,
      UnprotectedKey key,
      int poolSize,
      long maxConnectionAge,
      String trustStore,
      String trustStorePassword
  ) {
    if (port.getProtocol() != com.aoapps.net.Protocol.TCP) {
      throw new IllegalArgumentException("Only TCP supported: " + port);
    }
    this.hostname = hostname;
    this.localIp = localIp;
    this.port = port;
    this.protocol = protocol;
    this.key = key;
    this.poolSize = poolSize;
    this.maxConnectionAge = maxConnectionAge;
    this.trustStore = trustStore;
    this.trustStorePassword = trustStorePassword;
    this.pool = new AoservDaemonConnectionPool(this, logger);
  }

  public int getConcurrency() {
    return pool.getConcurrency();
  }

  /**
   * Allocates a connection to the server.  These connections must later be
   * released with the <code>releaseConnection</code> method.  Connection
   * pooling is obtained this way.  These connections may be over any protocol,
   * so they may only be used for one client/server exchange at a time.
   */
  // TODO: Implement the last uses of this as Request/Response, then make this not public
  //       This might mean implementing direct socket I/O as some sort of wrapper that creates
  //       a bidirectional stream on top of Request/Response.  This would make everything
  //       request/response-based, which would mean we could then run on HTTP either directly
  //       or using ao-messaging.
  public AoservDaemonConnection getConnection() throws IOException {
    try {
      return pool.getConnection();
    } catch (IOException err) {
      logger.log(Level.INFO, "IOException while trying to get a connection to server from " + localIp + " to " + hostname + ":" + port, err);
      throw err;
    }
  }

  /**
   * Allocates a connection to the server.  These connections must later be
   * released with the <code>releaseConnection</code> method.  Connection
   * pooling is obtained this way.  These connections may be over any protocol,
   * so they may only be used for one client/server exchange at a time.
   */
  public AoservDaemonConnection getConnection(int maxConnections) throws IOException {
    try {
      return pool.getConnection(maxConnections);
    } catch (IOException err) {
      logger.log(Level.INFO, "IOException while trying to get a connection to server from " + localIp + " to " + hostname + ":" + port, err);
      throw err;
    }
  }

  public int getConnectionCount() {
    return pool.getConnectionCount();
  }

  /**
   * Gets the default <code>AoservConnector</code> as defined in the
   * <code>client.properties</code> resource.  Each possible
   * protocol is tried, in order, until a successful connection is
   * made.  If no connection is made, an <code>IOException</code>
   * is thrown.
   */
  public static synchronized AoservDaemonConnector getConnector(
      HostAddress hostname,
      InetAddress localIp,
      Port port,
      String protocol,
      UnprotectedKey key,
      int poolSize,
      long maxConnectionAge,
      String trustStore,
      String trustStorePassword
  ) {
    NullArgumentException.checkNotNull(hostname, "hostname");
    NullArgumentException.checkNotNull(localIp, "local_ip");
    NullArgumentException.checkNotNull(protocol, "protocol");

    int size = connectors.size();
    for (int c = 0; c < size; c++) {
      AoservDaemonConnector connector = connectors.get(c);
      if (
          connector.hostname.equals(hostname)
              && connector.localIp.equals(localIp)
              && connector.port == port
              && connector.protocol.equals(protocol)
              && (key == null ? connector.key == null : key.equals(connector.key))
              && connector.poolSize == poolSize
              && connector.maxConnectionAge == maxConnectionAge
      ) {
        return connector;
      }
    }
    AoservDaemonConnector connector = new AoservDaemonConnector(
        hostname,
        localIp,
        port,
        protocol,
        key,
        poolSize,
        maxConnectionAge,
        trustStore,
        trustStorePassword
    );
    connectors.add(connector);
    return connector;
  }

  public long getConnects() {
    return pool.getConnects();
  }

  /**
   * Gets the hostname that is connected to.
   */
  public HostAddress getHostname() {
    return hostname;
  }

  /**
   * Gets the local IP address that connections are established from.
   */
  public InetAddress getLocalIp() {
    return localIp;
  }

  public int getMaxConcurrency() {
    return pool.getMaxConcurrency();
  }

  public long getMaxConnectionAge() {
    return pool.getMaxConnectionAge();
  }

  public int getPoolSize() {
    return pool.getPoolSize();
  }

  /**
   * Gets the port that is connected to.
   */
  public Port getPort() {
    return port;
  }

  public long getTotalTime() {
    return pool.getTotalTime();
  }

  public long getTransactionCount() {
    return pool.getTransactionCount();
  }

  public void printConnectionStatsHtml(Appendable out, boolean isXhtml) throws IOException {
    pool.printStatisticsHtml(out, isXhtml);
  }

  /**
   * Releases a connection to the server.  This will either close the
   * connection or allow another thread to use the connection.
   * Connections may be of any protocol, so each connection must be
   * released after every transaction.
   *
   * @param  connection  the connection to close or release
   *
   * @throws  IOException  if an error occurred while closing or releasing the connection
   *
   * @see  #getConnection(int)
   * @see  AoservConnection#close()
   */
  void release(AoservDaemonConnection connection) throws IOException {
    pool.release(connection);
  }

  @Override
  public String toString() {
    return getClass().getName() + "?hostname=" + hostname + "&local_ip=" + localIp + "&port=" + port + "&protocol=" + protocol;
  }

  /**
   * Gets the error handler for this and its underlying connection pool.
   */
  Logger getLogger() {
    return logger;
  }

  /**
   * Determines the task once a connection is allocated and before anything is written.
   */
  // TODO: Move to protocol?
  @FunctionalInterface
  private static interface TaskCodeSupplier {
    /**
     * Gets the task code that will be used for the request.
     *
     * @throws  Error             any error, connection remains valid
     * @throws  RuntimeException  any unchecked exception, connection remains valid
     * @throws  IOException       any I/O error, connection remains valid
     * @throws  SQLException      qny SQL error, connection remains valid
     */
    int getTaskCode(AoservDaemonConnection conn) throws IOException, SQLException;
  }

  // TODO: Move to protocol?
  @FunctionalInterface
  private static interface Request  {

    /**
     * Performs a precheck on the request, before anything is written.
     *
     * @throws  Error             any error, connection remains valid
     * @throws  RuntimeException  any unchecked exception, connection remains valid
     * @throws  IOException       any I/O error, connection remains valid
     * @throws  SQLException      qny SQL error, connection remains valid
     */
    default void before(AoservDaemonConnection conn) throws IOException, SQLException {
      // Do nothing by default
    }

    /**
     * Writes the request to the server.
     * This does not need to flush the output stream.
     *
     * @throws  Error             any error will {@linkplain AoservDaemonConnection#abort(java.lang.Throwable) abort the connection}
     * @throws  RuntimeException  any unchecked exception will {@linkplain AoservDaemonConnection#abort(java.lang.Throwable) abort the connection}
     * @throws  IOException       any I/O error will {@linkplain AoservDaemonConnection#abort(java.lang.Throwable) abort the connection}
     */
    void write(AoservDaemonConnection conn, StreamableOutput out) throws IOException;
  }

  // TODO: Move to protocol?
  private static IOException newUnknownResult(int code) {
    return new IOException("Unknown result: " + code);
  }

  /**
   * Base implementation of reading request response.
   */
  // TODO: Move to protocol?
  private static class Response {

    protected IOException ioException;
    protected SQLException sqlException;

    /**
     * Performs a precheck on the response, before anything is written.
     *
     * @return  {@code true} if the request should continue, or {@code false} to skip the request.
     *
     * @throws  Error             any error, connection remains valid
     * @throws  RuntimeException  any unchecked exception, connection remains valid
     * @throws  IOException       any I/O error, connection remains valid
     * @throws  SQLException      qny SQL error, connection remains valid
     */
    protected boolean before(AoservDaemonConnection conn) throws IOException, SQLException {
      return true;
    }

    /**
     * Reads the response from the server if the request was successfully sent.
     *
     * @throws  Error             any error will {@linkplain AoservDaemonConnection#abort(java.lang.Throwable) abort the connection}
     * @throws  RuntimeException  any unchecked exception will {@linkplain AoservDaemonConnection#abort(java.lang.Throwable) abort the connection}
     * @throws  IOException       any I/O error will {@linkplain AoservDaemonConnection#abort(java.lang.Throwable) abort the connection}
     *
     * @see  #dispatch(com.aoindustries.aoserv.daemon.client.AoservDaemonConnection, com.aoapps.hodgepodge.io.stream.StreamableInput, int)
     */
    protected void read(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      dispatch(conn, in, in.read());
    }

    /**
     * Dispatches response block by result code.
     *
     * @see  #done(com.aoapps.hodgepodge.io.stream.StreamableInput)
     * @see  #next(com.aoapps.hodgepodge.io.stream.StreamableInput)
     * @see  #nextChunk(com.aoapps.hodgepodge.io.stream.StreamableInput)
     * @see  #ioException(com.aoapps.hodgepodge.io.stream.StreamableInput)
     * @see  #sqlException(com.aoapps.hodgepodge.io.stream.StreamableInput)
     */
    protected void dispatch(AoservDaemonConnection conn, StreamableInput in, int code) throws IOException {
      switch (code) {
        case AoservDaemonProtocol.DONE:
          done(conn, in);
          break;
        case AoservDaemonProtocol.NEXT:
          next(conn, in);
          break;
        case AoservDaemonProtocol.NEXT_CHUNK:
          nextChunk(conn, in);
          break;
        case AoservDaemonProtocol.IO_EXCEPTION:
          ioException(conn, in);
          break;
        case AoservDaemonProtocol.SQL_EXCEPTION:
          sqlException(conn, in);
          break;
        default:
          throw newUnknownResult(code);
      }
    }

    /**
     * Called for response code {@link AoservDaemonProtocol#DONE}.
     *
     * @see  #read(com.aoapps.hodgepodge.io.stream.StreamableInput)
     */
    protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      throw new IOException("Unknown result: " + AoservDaemonProtocol.DONE);
    }

    /**
     * Called for response code {@link AoservDaemonProtocol#NEXT}.
     *
     * @see  #read(com.aoapps.hodgepodge.io.stream.StreamableInput)
     */
    protected void next(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      throw new IOException("Unknown result: " + AoservDaemonProtocol.NEXT);
    }

    /**
     * Called for response code {@link AoservDaemonProtocol#NEXT_CHUNK}.
     *
     * @see  #read(com.aoapps.hodgepodge.io.stream.StreamableInput)
     */
    protected void nextChunk(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      throw new IOException("Unknown result: " + AoservDaemonProtocol.NEXT_CHUNK);
    }

    /**
     * Called for response code {@link AoservDaemonProtocol#IO_EXCEPTION}.
     *
     * @see  #read(com.aoapps.hodgepodge.io.stream.StreamableInput)
     */
    protected void ioException(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      ioException = new IOException(in.readUTF());
    }

    /**
     * Called for response code {@link AoservDaemonProtocol#SQL_EXCEPTION}.
     *
     * @see  #read(com.aoapps.hodgepodge.io.stream.StreamableInput)
     */
    protected void sqlException(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      sqlException = new SQLException(in.readUTF());
    }
  }

  /**
   * Handles a response with no return value.  Expects a single result code of
   * {@link AoservDaemonProtocol#DONE} as success confirmation.
   */
  private static class VoidResponse extends Response {

    /**
     * {@inheritDoc}
     *
     * @return  {@code true} if the request should continue, or {@code false} to skip the request and proceed to
     *          {@link #after()}
     */
    // Overriding for javadocs only
    @Override
    protected boolean before(AoservDaemonConnection conn) throws IOException, SQLException {
      return super.before(conn);
    }

    /**
     * If both the request and response were successful, this is called after the
     * connection to the server is released.
     *
     * @throws  Error             any error, connection remains valid
     * @throws  RuntimeException  any unchecked exception, connection remains valid
     * @throws  IOException       when read over protocol, connection remains valid
     * @throws  SQLException      when read over protocol, connection remains valid
     */
    protected void after() throws IOException, SQLException {
      if (ioException != null) {
        throw ioException;
      }
      if (sqlException != null) {
        throw sqlException;
      }
    }

    @Override
    protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      // Successful response
    }
  }

  /**
   * This is the preferred mechanism for providing custom requests that have a return value.
   *
   * @see  #requestResult(int, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.ResultResponse)
   */
  private abstract static class ResultResponse<T> extends Response {

    protected T result;

    /**
     * {@inheritDoc}
     *
     * @return  {@code true} if the request should continue, or {@code false} to skip the request and proceed to
     *          {@link #after()}
     */
    // Overriding for javadocs only
    @Override
    protected boolean before(AoservDaemonConnection conn) throws IOException, SQLException {
      return super.before(conn);
    }

    /**
     * If both the request and response were successful, this is called after the
     * connection to the server is released.  The result is returned here so
     * any additional processing in packaging the result may be performed
     * after the connection is released.
     *
     * @throws  Error             any error, connection remains valid
     * @throws  RuntimeException  any unchecked exception, connection remains valid
     * @throws  IOException       when read over protocol, connection remains valid
     * @throws  SQLException      when read over protocol, connection remains valid
     */
    protected T after() throws IOException, SQLException {
      if (ioException != null) {
        throw ioException;
      }
      if (sqlException != null) {
        throw sqlException;
      }
      return result;
    }
  }

  /**
   * A request that sends a single {@code boolean} following the task code.
   *
   * @see  StreamableOutput#writeBoolean(boolean)
   */
  private static class BooleanRequest implements Request {

    private final boolean value;

    private BooleanRequest(boolean value) {
      this.value = value;
    }

    @Override
    public void write(AoservDaemonConnection conn, StreamableOutput out) throws IOException {
      out.writeBoolean(value);
    }
  }

  /**
   * A response that expects a single {@code boolean} following {@link AoservDaemonProtocol#DONE}.
   *
   * @see  StreamableInput#readBoolean()
   */
  private static class BooleanResponse extends ResultResponse<Boolean> {
    @Override
    public void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      result = in.readBoolean();
    }
  }

  /**
   * A request that sends a single {@linkplain StreamableOutput#writeCompressedInt(int) compressed int} following the task code.
   *
   * @see  StreamableOutput#writeCompressedInt(int)
   */
  private static class CompressedIntRequest implements Request {

    private final int value;

    private CompressedIntRequest(int value) throws IOException {
      StreamableOutput.checkCompressedInt(value);
      this.value = value;
    }

    @Override
    public void write(AoservDaemonConnection conn, StreamableOutput out) throws IOException {
      out.writeCompressedInt(value);
    }
  }

  /**
   * A response that expects a single {@linkplain StreamableInput#readCompressedInt() compressed int} following {@link AoservDaemonProtocol#DONE}.
   *
   * @see  StreamableInput#readCompressedInt()
   */
  private static class CompressedIntResponse extends ResultResponse<Integer> {
    @Override
    public void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      result = in.readCompressedInt();
    }
  }

  private static class DumpRequest implements Request {

    private final int param1;
    private final boolean gzip;

    private DumpRequest(int param1, boolean gzip) {
      this.param1 = param1;
      this.gzip = gzip;
    }

    @Override
    public void before(AoservDaemonConnection conn) throws IOException, SQLException {
      if (gzip && conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_80_0) < 0) {
        throw new IOException(
            "Gzip compression requires AOServ Daemon version "
                + AoservDaemonProtocol.Version.VERSION_1_80_0
                + " or higher.  Current version is " + conn.getProtocolVersion() + '.');
      }
    }

    @Override
    public void write(AoservDaemonConnection conn, StreamableOutput out) throws IOException {
      out.writeCompressedInt(param1);
      if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_80_0) >= 0) {
        out.writeBoolean(gzip);
      }
    }
  }

  private static class DumpResponse extends StreamingResponse {

    private final DumpSizeCallback onDumpSize;
    private long dumpSize = -1;

    private DumpResponse(DumpSizeCallback onDumpSize, StreamableOutput out) {
      super(out);
      this.onDumpSize = onDumpSize;
    }

    @Override
    protected void read(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_80_0) >= 0) {
        dumpSize = in.readLong();
      } else {
        dumpSize = -1;
      }
      if (dumpSize < -1) {
        throw new IOException("dumpSize < -1: " + dumpSize);
      }
      if (onDumpSize != null) {
        onDumpSize.onDumpSize(dumpSize);
      }
      super.read(conn, in);
    }

    @Override
    public void addBytesRead(int blockLen) throws IOException {
      super.addBytesRead(blockLen);
      if (dumpSize != -1) {
        long bytesRead = getBytesRead();
        if (bytesRead > dumpSize) {
          throw new IOException("Too many bytes read: " + bytesRead + " > " + dumpSize);
        }
      }
    }

    @Override
    protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      if (dumpSize != -1) {
        long bytesRead = getBytesRead();
        if (bytesRead < dumpSize) {
          throw new IOException("Too few bytes read: " + bytesRead + " < " + dumpSize);
        }
      }
    }
  }

  /**
   * A response that expects a single {@code long} following {@link AoservDaemonProtocol#DONE}.
   *
   * @see  StreamableInput#readLong()
   */
  private static class LongResponse extends ResultResponse<Long> {
    @Override
    public void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      result = in.readLong();
    }
  }

  /**
   * A response that expects a single {@link String}, or {@code null}, following {@link AoservDaemonProtocol#DONE}.
   *
   * @see  StreamableInput#readNullUTF()
   */
  private static class NullUtfResponse extends ResultResponse<String> {
    @Override
    public void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      result = in.readNullUTF();
    }
  }

  /**
   * A response that streams blocks directly through to a {@link StreamableOutput}.
   */
  private static class StreamingResponse extends VoidResponse {

    private final StreamableOutput out;
    private long bytesRead;

    private StreamingResponse(StreamableOutput out) {
      this.out = out;
    }

    /**
     * Gets the number of bytes read.
     */
    public long getBytesRead() {
      return bytesRead;
    }

    /**
     * Adds to {@link #bytesRead}.
     *
     * @throws  IOException  any I/O error will {@linkplain AoservDaemonConnection#abort(java.lang.Throwable) abort the connection}
     */
    public void addBytesRead(int blockLen) throws IOException {
      bytesRead += blockLen;
    }

    @Override
    protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      // End of stream
    }

    @Override
    protected void next(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      int code;
      byte[] buff = BufferManager.getBytes();
      try {
        do {
          int blockLen = in.readShort();
          bytesRead += blockLen;
          in.readFully(buff, 0, blockLen);
          out.writeByte(AoservProtocol.NEXT);
          out.writeShort(blockLen);
          out.write(buff, 0, blockLen);
        } while ((code = in.read()) == AoservDaemonProtocol.NEXT);
      } finally {
        BufferManager.release(buff, false);
      }
      dispatch(conn, in, code);
    }
  }

  /**
   * A request that sends a single {@link String} following the task code.
   *
   * @see  StreamableOutput#writeUTF(java.lang.String)
   */
  private static class UtfRequest implements Request {

    private final String value;

    private UtfRequest(String value) {
      this.value = value;
    }

    @Override
    public void write(AoservDaemonConnection conn, StreamableOutput out) throws IOException {
      out.writeUTF(value);
    }
  }

  /**
   * A response that expects a single {@link String} following {@link AoservDaemonProtocol#DONE}.
   *
   * @see  StreamableInput#readUTF()
   */
  private static class UtfResponse extends ResultResponse<String> {
    @Override
    public void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
      result = in.readUTF();
    }
  }

  private void requestVoid(
      // TODO: boolean allowRetry,
      TaskCodeSupplier taskCodeSupplier,
      Request request,
      VoidResponse response
  ) throws IOException, SQLException {
    try (AoservDaemonConnection conn = getConnection()) {
      int taskCode = taskCodeSupplier.getTaskCode(conn);
      if (request != null) {
        request.before(conn);
      }
      if (response.before(conn)) {
        try {
          StreamableOutput out = conn.getRequestOut(taskCode);
          if (request != null) {
            request.write(conn, out);
          }
          out.flush();

          response.read(conn, conn.getResponseIn());
        } catch (Error | RuntimeException | IOException err) {
          throw Throwables.wrap(conn.abort(err), IOException.class, IOException::new);
        }
      }
    }
    response.after();
  }

  private void requestVoid(
      // TODO: boolean allowRetry,
      int taskCode,
      Request request,
      VoidResponse response
  ) throws IOException, SQLException {
    requestVoid(conn -> taskCode, request, response);
  }

  /**
   * Uses default {@link VoidResponse}.
   *
   * @see  #requestVoid(com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.TaskCodeSupplier, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.Request, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.VoidResponse)
   */
  private void requestVoid(
      // TODO: boolean allowRetry,
      TaskCodeSupplier taskCodeSupplier,
      Request request
  ) throws IOException, SQLException {
    requestVoid(taskCodeSupplier, request, new VoidResponse());
  }

  /**
   * Uses default {@link VoidResponse}.
   *
   * @see  #requestVoid(int, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.Request, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.VoidResponse)
   */
  private void requestVoid(
      // TODO: boolean allowRetry,
      int taskCode,
      Request request
  ) throws IOException, SQLException {
    requestVoid(taskCode, request, new VoidResponse());
  }

  /**
   * No writer and uses default {@link VoidResponse}.
   *
   * @see  #requestVoid(com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.TaskCodeSupplier, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.Request, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.VoidResponse)
   */
  private void requestVoid(
      // TODO: boolean allowRetry,
      TaskCodeSupplier taskCodeSupplier
  ) throws IOException, SQLException {
    requestVoid(taskCodeSupplier, null, new VoidResponse());
  }

  /**
   * No writer and uses default {@link VoidResponse}.
   *
   * @see  #requestVoid(int, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.Request, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.VoidResponse)
   */
  private void requestVoid(
      // TODO: boolean allowRetry,
      int taskCode
  ) throws IOException, SQLException {
    requestVoid(taskCode, null, new VoidResponse());
  }

  private <T> T requestResult(
      // TODO: boolean allowRetry,
      TaskCodeSupplier taskCodeSupplier,
      Request request,
      ResultResponse<T> response
  ) throws IOException, SQLException {
    try (AoservDaemonConnection conn = getConnection()) {
      int taskCode = taskCodeSupplier.getTaskCode(conn);
      if (request != null) {
        request.before(conn);
      }
      if (response.before(conn)) {
        try {
          StreamableOutput out = conn.getRequestOut(taskCode);
          if (request != null) {
            request.write(conn, out);
          }
          out.flush();

          response.read(conn, conn.getResponseIn());
        } catch (Error | RuntimeException | IOException err) {
          throw Throwables.wrap(conn.abort(err), IOException.class, IOException::new);
        }
      }
    }
    return response.after();
  }

  private <T> T requestResult(
      // TODO: boolean allowRetry,
      int taskCode,
      Request request,
      ResultResponse<T> response
  ) throws IOException, SQLException {
    return requestResult(conn -> taskCode, request, response);
  }

  /**
   * No writer.
   *
   * @see  #requestResult(com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.TaskCodeSupplier, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.ResultResponse)
   */
  private <T> T requestResult(
      // TODO: boolean allowRetry,
      TaskCodeSupplier taskCodeSupplier,
      ResultResponse<T> response
  ) throws IOException, SQLException {
    return requestResult(taskCodeSupplier, null, response);
  }

  /**
   * No writer.
   *
   * @see  #requestResult(int, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.Request, com.aoindustries.aoserv.daemon.client.AoservDaemonConnector.ResultResponse)
   */
  private <T> T requestResult(
      // TODO: boolean allowRetry,
      int taskCode,
      ResultResponse<T> response
  ) throws IOException, SQLException {
    return requestResult(taskCode, null, response);
  }

  /**
   * Copies a home directory.
   *
   * @param  username  the username to copy the home directory of
   * @param  toConnector  the connector to send the data to
   *
   * @return  the number of bytes transferred
   */
  public long copyHomeDirectory(com.aoindustries.aoserv.client.linux.User.Name username, AoservDaemonConnector toConnector) throws IOException, SQLException {
    // Establish the connection to the source
    try (AoservDaemonConnection sourceConn = getConnection()) {
      try {
        StreamableOutput sourceOut = sourceConn.getRequestOut(AoservDaemonProtocol.TAR_HOME_DIRECTORY);
        sourceOut.writeUTF(username.toString());
        sourceOut.flush();

        StreamableInput sourceIn = sourceConn.getResponseIn();

        // Establish the connection to the destination
        // TODO: Have a specialized version when both home directories are on the same server
        // TODO: Use direct daemon-to-daemon connections (when possible), like done for backups?
        try (AoservDaemonConnection destConn = toConnector.getConnection(toConnector == this ? 2 : 1)) {
          try {
            StreamableOutput destOut = destConn.getRequestOut(AoservDaemonProtocol.UNTAR_HOME_DIRECTORY);
            destOut.writeUTF(username.toString());

            long byteCount = 0;
            int sourceCode;
            byte[] buff = BufferManager.getBytes();
            try {
              while ((sourceCode = sourceIn.read()) == AoservDaemonProtocol.NEXT) {
                int len = sourceIn.readShort();
                byteCount += len;
                sourceIn.readFully(buff, 0, len);
                destOut.writeByte(AoservDaemonProtocol.NEXT);
                destOut.writeShort(len);
                destOut.write(buff, 0, len);
              }
            } finally {
              BufferManager.release(buff, false);
            }
            if (sourceCode != AoservDaemonProtocol.DONE) {
              if (sourceCode == AoservDaemonProtocol.IO_EXCEPTION) {
                String message = sourceIn.readUTF();
                destOut.writeByte(AoservDaemonProtocol.IO_EXCEPTION);
                destOut.writeUTF(message);
                destOut.flush();
                throw new IOException(message);
              } else if (sourceCode == AoservDaemonProtocol.SQL_EXCEPTION) {
                String message = sourceIn.readUTF();
                destOut.writeByte(AoservDaemonProtocol.SQL_EXCEPTION);
                destOut.writeUTF(message);
                destOut.flush();
                throw new SQLException(message);
              } else {
                throw new IOException("Unknown result: " + sourceCode);
              }
            }
            destOut.writeByte(AoservDaemonProtocol.DONE);
            destOut.flush();

            StreamableInput destIn = destConn.getResponseIn();
            int destResult = destIn.read();
            if (destResult != AoservDaemonProtocol.DONE) {
              if (destResult == AoservDaemonProtocol.IO_EXCEPTION) {
                throw new IOException(destIn.readUTF());
              } else if (destResult == AoservDaemonProtocol.SQL_EXCEPTION) {
                throw new SQLException(destIn.readUTF());
              } else {
                throw new IOException("Unknown result: " + destResult);
              }
            }

            return byteCount;
          } catch (Error | RuntimeException | IOException err) {
            throw Throwables.wrap(destConn.abort(err), IOException.class, IOException::new);
          }
        }
      } catch (Error | RuntimeException | IOException err) {
        throw Throwables.wrap(sourceConn.abort(err), IOException.class, IOException::new);
      }
    }
  }

  /**
   * Called once the dump size is known and before
   * the stream is written to.
   */
  @FunctionalInterface
  public static interface DumpSizeCallback {
    /**
     * Called once the dump size is known and before
     * the stream is written to.
     *
     * @param  dumpSize  The number of bytes that will be transferred or {@code -1} if unknown
     */
    void onDumpSize(long dumpSize) throws IOException;
  }

  public void dumpMysqlDatabase(
      int pkey,
      boolean gzip,
      DumpSizeCallback onDumpSize,
      StreamableOutput masterOut
  ) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.DUMP_MYSQL_DATABASE,
        new DumpRequest(pkey, gzip),
        new DumpResponse(onDumpSize, masterOut)
    );
  }

  public void dumpPostgresDatabase(
      int pkey,
      boolean gzip,
      DumpSizeCallback onDumpSize,
      StreamableOutput masterOut
  ) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.DUMP_POSTGRES_DATABASE,
        new DumpRequest(pkey, gzip),
        new DumpResponse(onDumpSize, masterOut)
    );
  }

  public String getAutoresponderContent(PosixPath path) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_AUTORESPONDER_CONTENT,
        new UtfRequest(path.toString()),
        new UtfResponse()
    );
  }

  /**
   * Gets a cron table.
   *
   * @param  username  the username to copy the home directory of
   *
   * @return  the cron table
   */
  public String getCronTable(com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_CRON_TABLE,
        new UtfRequest(username.toString()),
        new UtfResponse()
    );
  }

  /**
   * Gets a bonding report.
   *
   * @param  pkey  the unique ID of the net_device
   *
   * @return  the report
   */
  public String getNetDeviceBondingReport(int pkey) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_NET_DEVICE_BONDING_REPORT,
        new CompressedIntRequest(pkey),
        new UtfResponse()
    );
  }

  /**
   * Gets a statistics report.
   *
   * @param  pkey  the unique ID of the net_device
   *
   * @return  the report
   */
  public String getNetDeviceStatisticsReport(int pkey) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_NET_DEVICE_STATISTICS_REPORT,
        new CompressedIntRequest(pkey),
        new UtfResponse()
    );
  }

  /**
   * Determines if the inbox is in manual procmail mode.
   *
   * @param  lsa  the pkey of the LinuxServerAccount
   */
  public boolean isProcmailManual(int lsa) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.IS_PROCMAIL_MANUAL,
        new CompressedIntRequest(lsa),
        new BooleanResponse()
    );
  }

  /**
   * Gets the total size of a mounted filesystem in bytes.
   */
  public long getDiskDeviceTotalSize(PosixPath path) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_DISK_DEVICE_TOTAL_SIZE,
        new UtfRequest(path.toString()),
        new LongResponse()
    );
  }

  /**
   * Gets the used size of a mounted filesystem in bytes.
   */
  public long getDiskDeviceUsedSize(PosixPath path) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_DISK_DEVICE_USED_SIZE,
        new UtfRequest(path.toString()),
        new LongResponse()
    );
  }

  /**
   * Gets the file used by an email list.
   */
  public String getEmailListFile(PosixPath path) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_EMAIL_LIST_FILE,
        new UtfRequest(path.toString()),
        new UtfResponse()
    );
  }

  /**
   * Gets the encrypted password for a linux account as found in the /etc/shadow file.
   */
  public Tuple2<String, Integer> getEncryptedLinuxAccountPassword(com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD,
        new UtfRequest(username.toString()),
        new ResultResponse<>() {
          @Override
          protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            String encryptedPassword = in.readUTF();
            Integer changedDate;
            if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_80_1) >= 0) {
              int i = in.readCompressedInt();
              changedDate = i == -1 ? null : i;
            } else {
              changedDate = null;
            }
            result = new Tuple2<>(encryptedPassword, changedDate);
          }
        }
    );
  }

  public long[] getImapFolderSizes(com.aoindustries.aoserv.client.linux.User.Name username, String[] folderNames) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_IMAP_FOLDER_SIZES,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(username.toString());
          out.writeCompressedInt(folderNames.length);
          for (String folderName : folderNames) {
            out.writeUTF(folderName);
          }
        },
        new ResultResponse<>() {
          @Override
          protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            long[] sizes = new long[folderNames.length];
            for (int c = 0; c < folderNames.length; c++) {
              sizes[c] = in.readLong();
            }
            result = sizes;
          }
        }
    );
  }

  public InboxAttributes getInboxAttributes(com.aoindustries.aoserv.client.linux.User.Name username) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_INBOX_ATTRIBUTES,
        new UtfRequest(username.toString()),
        new ResultResponse<>() {
          @Override
          protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            result = new InboxAttributes(in.readLong(), in.readLong());
          }
        }
    );
  }

  public void getMrtgFile(String filename, StreamableOutput out) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.GET_MRTG_FILE,
        new UtfRequest(filename),
        new StreamingResponse(out)
    );
  }

  public Server.MasterStatus getMysqlMasterStatus(int mysqlServer) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_MYSQL_MASTER_STATUS,
        new CompressedIntRequest(mysqlServer),
        new ResultResponse<>() {
          @Override
          protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            result = null;
          }

          @Override
          protected void next(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            result = new Server.MasterStatus(
                in.readNullUTF(),
                in.readNullUTF()
            );
          }
        }
    );
  }

  public MysqlReplication.SlaveStatus getMysqlSlaveStatus(
      PosixPath failoverRoot,
      int nestedOperatingSystemVersion,
      Server.Name serverName,
      Port port
  ) throws IOException, SQLException {
    if (port.getProtocol() != com.aoapps.net.Protocol.TCP) {
      throw new IllegalArgumentException("Only TCP supported: " + port);
    }
    return requestResult(
        AoservDaemonProtocol.GET_MYSQL_SLAVE_STATUS,
        (conn, out) -> {
          out.writeUTF(failoverRoot == null ? "" : failoverRoot.toString());
          out.writeCompressedInt(nestedOperatingSystemVersion);
          if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_84_11) >= 0) {
            out.writeUTF(serverName.toString());
          }
          out.writeCompressedInt(port.getPort());
        },
        new ResultResponse<>() {
          @Override
          protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            result = null;
          }

          @Override
          protected void next(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            result = new MysqlReplication.SlaveStatus(
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF(),
                in.readNullUTF()
            );
          }
        }
    );
  }

  public List<TableStatus> getMysqlTableStatus(
      PosixPath failoverRoot,
      int nestedOperatingSystemVersion,
      Server.Name serverName,
      Port port,
      com.aoindustries.aoserv.client.mysql.Database.Name databaseName
  ) throws IOException, SQLException {
    if (port.getProtocol() != com.aoapps.net.Protocol.TCP) {
      throw new IllegalArgumentException("Only TCP supported: " + port);
    }
    return requestResult(
        AoservDaemonProtocol.GET_MYSQL_TABLE_STATUS,
        (conn, out) -> {
          out.writeUTF(failoverRoot == null ? "" : failoverRoot.toString());
          out.writeCompressedInt(nestedOperatingSystemVersion);
          if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_84_11) >= 0) {
            out.writeUTF(serverName.toString());
          }
          out.writeCompressedInt(port.getPort());
          out.writeUTF(databaseName.toString());
        },
        new ResultResponse<>() {
          @Override
          protected void next(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            try {
              int size = in.readCompressedInt();
              List<TableStatus> tableStatuses = new ArrayList<>(size);
              for (int c = 0; c < size; c++) {
                tableStatuses.add(new TableStatus(
                    TableName.valueOf(in.readUTF()), // name
                    in.readNullEnum(Engine.class), // engine
                    in.readNullInteger(), // version
                    in.readNullEnum(TableStatus.RowFormat.class), // rowFormat
                    in.readNullLong(), // rows
                    in.readNullLong(), // avgRowLength
                    in.readNullLong(), // dataLength
                    in.readNullLong(), // maxDataLength
                    in.readNullLong(), // indexLength
                    in.readNullLong(), // dataFree
                    in.readNullLong(), // autoIncrement
                    in.readNullUTF(), // createTime
                    in.readNullUTF(), // updateTime
                    in.readNullUTF(), // checkTime
                    in.readNullEnum(TableStatus.Collation.class), // collation
                    in.readNullUTF(), // checksum
                    in.readNullUTF(), // createOptions
                    in.readNullUTF() // comment
                )
                );
              }
              result = tableStatuses;
            } catch (ValidationException e) {
              throw new IOException(e);
            }
          }
        }
    );
  }

  public List<CheckTableResult> checkMysqlTables(
      PosixPath failoverRoot,
      int nestedOperatingSystemVersion,
      Server.Name serverName,
      Port port,
      com.aoindustries.aoserv.client.mysql.Database.Name databaseName,
      List<TableName> tableNames
  ) throws IOException, SQLException {
    if (port.getProtocol() != com.aoapps.net.Protocol.TCP) {
      throw new IllegalArgumentException("Only TCP supported: " + port);
    }
    return requestResult(
        AoservDaemonProtocol.CHECK_MYSQL_TABLES,
        (conn, out) -> {
          out.writeUTF(failoverRoot == null ? "" : failoverRoot.toString());
          out.writeCompressedInt(nestedOperatingSystemVersion);
          if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_84_11) >= 0) {
            out.writeUTF(serverName.toString());
          }
          out.writeCompressedInt(port.getPort());
          out.writeUTF(databaseName.toString());
          int numTables = tableNames.size();
          out.writeCompressedInt(numTables);
          for (int c = 0; c < numTables; c++) {
            out.writeUTF(tableNames.get(c).toString());
          }
        },
        new ResultResponse<>() {
          @Override
          protected void next(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            int size = in.readCompressedInt();
            List<CheckTableResult> checkTableResults = new ArrayList<>(size);
            for (int c = 0; c < size; c++) {
              try {
                checkTableResults.add(new CheckTableResult(
                    TableName.valueOf(in.readUTF()), // table
                    in.readLong(), // duration
                    in.readNullEnum(CheckTableResult.MsgType.class), // msgType
                    in.readNullUTF() // msgText
                )
                );
              } catch (ValidationException e) {
                throw new IOException(e);
              }
            }
            result = checkTableResults;
          }
        }
    );
  }

  public void getAwstatsFile(String siteName, String path, String queryString, StreamableOutput out) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.GET_AWSTATS_FILE,
        (AoservDaemonConnection conn, StreamableOutput daemonOut) -> {
          daemonOut.writeUTF(siteName);
          daemonOut.writeUTF(path);
          daemonOut.writeUTF(queryString);
        },
        new StreamingResponse(out)
    );
  }

  /**
   * Compares to the password list on the server.
   */
  public boolean compareLinuxAccountPassword(com.aoindustries.aoserv.client.linux.User.Name username, String password) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.COMPARE_LINUX_ACCOUNT_PASSWORD,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(username.toString());
          out.writeUTF(password);
        },
        new BooleanResponse()
    );
  }

  /**
   * Gets the encrypted password for a MySQL user as found in user table.
   */
  public String getEncryptedMysqlUserPassword(int mysqlServer, com.aoindustries.aoserv.client.mysql.User.Name username) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_ENCRYPTED_MYSQL_USER_PASSWORD,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeCompressedInt(mysqlServer);
          out.writeUTF(username.toString());
        },
        new UtfResponse()
    );
  }

  /**
   * Gets the password for a PostgreSQL user as found in pg_shadow or pg_authid table.
   */
  public String getPostgresUserPassword(int pkey) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_POSTGRES_PASSWORD,
        new CompressedIntRequest(pkey),
        new UtfResponse()
    );
  }

  public void grantDaemonAccess(
      long key,
      int command,
      String param1,
      String param2,
      String param3,
      String param4
  ) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.GRANT_DAEMON_ACCESS,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeLong(key);
          out.writeCompressedInt(command);
          out.writeBoolean(param1 != null);
          if (param1 != null) {
            out.writeUTF(param1);
          }
          out.writeBoolean(param2 != null);
          if (param2 != null) {
            out.writeUTF(param2);
          }
          out.writeBoolean(param3 != null);
          if (param3 != null) {
            out.writeUTF(param3);
          }
          out.writeBoolean(param4 != null);
          if (param4 != null) {
            out.writeUTF(param4);
          }
        }
    );
  }

  // public void initializeHttpdSitePasswdFile(int sitePKey, String username, String encPassword) throws IOException, SQLException {
  //   AoservDaemonConnection conn=getConnection();
  //   try {
  //     StreamableOutput out=conn.getOutputStream();
  //     out.writeCompressedInt(AoservDaemonProtocol.INITIALIZE_HTTPD_SITE_PASSWD_FILE);
  //     out.writeCompressedInt(sitePKey);
  //     out.writeUTF(username);
  //     out.writeUTF(encPassword);
  //     out.flush();
  //
  //     StreamableInput in=conn.getResponseIn();
  //     int result = in.read();
  //     if (result == AoservDaemonProtocol.DONE) {
  //       return;
  //     } else if (result == AoservDaemonProtocol.IO_EXCEPTION) {
  //       throw new IOException(in.readUTF());
  //     } else if (result == AoservDaemonProtocol.SQL_EXCEPTION) {
  //       throw new SQLException(in.readUTF());
  //     } else {
  //       throw new IOException("Unknown result: " + result);
  //     }
  //   } catch (IOException err) {
  //     conn.close();
  //     throw err;
  //   } finally {
  //     releaseConnection(conn);
  //   }
  // }

  /**
   * Deletes the contents of an email list.
   */
  public void removeEmailList(PosixPath listPath) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.REMOVE_EMAIL_LIST,
        new UtfRequest(listPath.toString())
    );
  }

  public void restartApache() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.RESTART_APACHE);
  }

  public void restartCron() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.RESTART_CRON);
  }

  public void restartMysql(int mysqlServer) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.RESTART_MYSQL,
        new CompressedIntRequest(mysqlServer)
    );
  }

  public void restartPostgres(int pkey) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.RESTART_POSTGRES,
        new CompressedIntRequest(pkey)
    );
  }

  public void restartXfs() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.RESTART_XFS);
  }

  public void restartXvfb() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.RESTART_XVFB);
  }

  public void setAutoresponderContent(PosixPath path, String content, int uid, int gid) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.SET_AUTORESPONDER_CONTENT,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(path.toString());
          out.writeBoolean(content != null);
          if (content != null) {
            out.writeUTF(content);
          }
          out.writeCompressedInt(uid);
          out.writeCompressedInt(gid);
        }
    );
  }

  /**
   * Sets a cron table.
   *
   * @param  username  the username to copy the home directory of
   * @param  cronTable  the new cron table
   */
  public void setCronTable(com.aoindustries.aoserv.client.linux.User.Name username, String cronTable) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.SET_CRON_TABLE,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(username.toString());
          out.writeUTF(cronTable);
        }
    );
  }

  /**
   * Sets the file used by an email list.
   */
  public void setEmailListFile(PosixPath path, String file, int uid, int gid, int mode) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.SET_EMAIL_LIST_FILE,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(path.toString());
          out.writeUTF(file);
          out.writeCompressedInt(uid);
          out.writeCompressedInt(gid);
          out.writeCompressedInt(mode);
        }
    );
  }

  /**
   * Sets the encrypted password for a Linux account.
   */
  public void setEncryptedLinuxAccountPassword(com.aoindustries.aoserv.client.linux.User.Name username, String encryptedPassword, Integer changedDate) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.SET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(username.toString());
          out.writeUTF(encryptedPassword);
          if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_80_1) >= 0) {
            out.writeCompressedInt(changedDate == null ? -1 : changedDate);
          }
        }
    );
  }

  /**
   * Sets the password for a <code>LinuxServerAccount</code>.
   */
  public void setLinuxServerAccountPassword(com.aoindustries.aoserv.client.linux.User.Name username, String plainPassword) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.SET_LINUX_SERVER_ACCOUNT_PASSWORD,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(username.toString());
          out.writeUTF(plainPassword);
        }
    );
  }

  /**
   * Sets the password for a <code>MysqlServerUser</code>.
   */
  public void setMysqlUserPassword(int mysqlServer, com.aoindustries.aoserv.client.mysql.User.Name username, String password) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.SET_MYSQL_USER_PASSWORD,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeCompressedInt(mysqlServer);
          out.writeUTF(username.toString());
          out.writeBoolean(password != null);
          if (password != null) {
            out.writeUTF(password);
          }
        }
    );
  }

  /**
   * Sets the password for a <code>PostgresServerUser</code>.
   */
  public void setPostgresUserPassword(int pkey, String password) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.SET_POSTGRES_USER_PASSWORD,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeCompressedInt(pkey);
          out.writeBoolean(password != null);
          if (password != null) {
            out.writeUTF(password);
          }
        }
    );
  }

  public void startApache() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.START_APACHE);
  }

  public void startCron() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.START_CRON);
  }

  /**
   * Starts a distribution verification.
   */
  public void startDistro(boolean includeUser) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.START_DISTRO,
        new BooleanRequest(includeUser)
    );
  }

  /**
   * Starts a Java VM.
   */
  public String startJvm(int httpdSite) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.START_JVM,
        new CompressedIntRequest(httpdSite),
        new NullUtfResponse()
    );
  }

  public void startMysql(int mysqlServer) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.START_MYSQL,
        new CompressedIntRequest(mysqlServer)
    );
  }

  public void startPostgresql(int pkey) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.START_POSTGRESQL,
        new CompressedIntRequest(pkey)
    );
  }

  public void startXfs() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.START_XFS);
  }

  public void startXvfb() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.START_XVFB);
  }

  public void stopApache() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.STOP_APACHE);
  }

  public void stopCron() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.STOP_CRON);
  }

  /**
   * Stops a Java VM.
   */
  public String stopJvm(int httpdSite) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.STOP_JVM,
        new CompressedIntRequest(httpdSite),
        new NullUtfResponse()
    );
  }

  public void stopMysql(int mysqlServer) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.STOP_MYSQL,
        new CompressedIntRequest(mysqlServer)
    );
  }

  public void stopPostgresql(int pkey) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.STOP_POSTGRESQL,
        new CompressedIntRequest(pkey)
    );
  }

  public void stopXfs() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.STOP_XFS);
  }

  public void stopXvfb() throws IOException, SQLException {
    requestVoid(AoservDaemonProtocol.STOP_XVFB);
  }

  private void waitFor(int taskCode) throws IOException, SQLException {
    requestVoid(
        conn -> (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_80_0) < 0)
            ? AoservDaemonProtocol.OLD_WAIT_FOR_REBUILD
            : taskCode,
        new Request() {
          private int tableId;

          @Override
          public void before(AoservDaemonConnection conn) throws IOException, SQLException {
            if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_80_0) < 0) {
              // Older protocol use a single WAIT_FOR_REBUILD with a follow-up table ID.
              // Table IDs can change over time, so the new protocol uses distinct task codes for each type of wait.
              // Find the table ID consistent with schema version 1.77
              switch (taskCode) {
                case AoservDaemonProtocol.WAIT_FOR_HTTPD_SITE_REBUILD:
                  tableId = AoservDaemonProtocol.OLD_HTTPD_SITES_TABLE_ID;
                  break;
                case AoservDaemonProtocol.WAIT_FOR_LINUX_ACCOUNT_REBUILD:
                  tableId = AoservDaemonProtocol.OLD_LINUX_ACCOUNTS_TABLE_ID;
                  break;
                case AoservDaemonProtocol.WAIT_FOR_MYSQL_DATABASE_REBUILD:
                  tableId = AoservDaemonProtocol.OLD_MYSQL_DATABASES_TABLE_ID;
                  break;
                case AoservDaemonProtocol.WAIT_FOR_MYSQL_DB_USER_REBUILD:
                  tableId = AoservDaemonProtocol.OLD_MYSQL_DB_USERS_TABLE_ID;
                  break;
                case AoservDaemonProtocol.WAIT_FOR_MYSQL_USER_REBUILD:
                  tableId = AoservDaemonProtocol.OLD_MYSQL_USERS_TABLE_ID;
                  break;
                case AoservDaemonProtocol.WAIT_FOR_POSTGRES_DATABASE_REBUILD:
                  tableId = AoservDaemonProtocol.OLD_POSTGRES_DATABASES_TABLE_ID;
                  break;
                case AoservDaemonProtocol.WAIT_FOR_POSTGRES_SERVER_REBUILD:
                  tableId = AoservDaemonProtocol.OLD_POSTGRES_SERVERS_TABLE_ID;
                  break;
                case AoservDaemonProtocol.WAIT_FOR_POSTGRES_USER_REBUILD:
                  tableId = AoservDaemonProtocol.OLD_POSTGRES_USERS_TABLE_ID;
                  break;
                default:
                  throw new IOException("Unexpected taskCode: " + taskCode);

              }
            }
          }

          @Override
          public void write(AoservDaemonConnection conn, StreamableOutput out) throws IOException {
            if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_80_0) < 0) {
              out.writeCompressedInt(tableId);
            }
          }
        }
    );
  }

  public void waitForHttpdSiteRebuild() throws IOException, SQLException {
    waitFor(AoservDaemonProtocol.WAIT_FOR_HTTPD_SITE_REBUILD);
  }

  public void waitForLinuxAccountRebuild() throws IOException, SQLException {
    waitFor(AoservDaemonProtocol.WAIT_FOR_LINUX_ACCOUNT_REBUILD);
  }

  public void waitForMysqlDatabaseRebuild() throws IOException, SQLException {
    waitFor(AoservDaemonProtocol.WAIT_FOR_MYSQL_DATABASE_REBUILD);
  }

  public void waitForMysqlDbUserRebuild() throws IOException, SQLException {
    waitFor(AoservDaemonProtocol.WAIT_FOR_MYSQL_DB_USER_REBUILD);
  }

  public void waitForMysqlServerRebuild() throws IOException, SQLException {
    waitFor(AoservDaemonProtocol.WAIT_FOR_MYSQL_SERVER_REBUILD);
  }

  public void waitForMysqlUserRebuild() throws IOException, SQLException {
    waitFor(AoservDaemonProtocol.WAIT_FOR_MYSQL_USER_REBUILD);
  }

  public void waitForPostgresDatabaseRebuild() throws IOException, SQLException {
    waitFor(AoservDaemonProtocol.WAIT_FOR_POSTGRES_DATABASE_REBUILD);
  }

  public void waitForPostgresServerRebuild() throws IOException, SQLException {
    waitFor(AoservDaemonProtocol.WAIT_FOR_POSTGRES_SERVER_REBUILD);
  }

  public void waitForPostgresUserRebuild() throws IOException, SQLException {
    waitFor(AoservDaemonProtocol.WAIT_FOR_POSTGRES_USER_REBUILD);
  }

  /**
   * Gets a 3ware RAID report.
   *
   * @return  the report
   */
  public String get3wareRaidReport() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_3WARE_RAID_REPORT, new UtfResponse());
  }

  /**
   * Gets the UPS status.
   *
   * @return  the report
   */
  public String getUpsStatus() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_UPS_STATUS, new UtfResponse());
  }

  /**
   * Gets a /proc/mdstat report.
   *
   * @return  the report
   */
  public String getMdStatReport() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_MD_STAT_REPORT, new UtfResponse());
  }

  /**
   * Gets a MD mismatch report.
   *
   * @return  the report
   */
  public String getMdMismatchReport() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_MD_MISMATCH_REPORT, new UtfResponse());
  }

  /**
   * Gets a DRBD report.
   *
   * @return  the report
   */
  public String getDrbdReport() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_DRBD_REPORT, new UtfResponse());
  }

  public Tuple2<Long, String> getFailoverFileReplicationActivity(int replication) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_FAILOVER_FILE_REPLICATION_ACTIVITY,
        new CompressedIntRequest(replication),
        new ResultResponse<>() {
          @Override
          protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            result = new Tuple2<>(in.readLong(), in.readUTF());
          }
        }
    );
  }

  /**
   * Gets a LVM report.
   *
   * @return  the report
   */
  public String[] getLvmReport() throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_LVM_REPORT,
        new ResultResponse<>() {
          @Override
          protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            result = new String[]{
                in.readUTF(),
                in.readUTF(),
                in.readUTF()
            };
          }
        }
    );
  }

  /**
   * Gets a hard drive temperature report.
   *
   * @return  the report
   */
  public String getHddTempReport() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_HDD_TEMP_REPORT, new UtfResponse());
  }

  /**
   * Gets a hard drive model report.
   *
   * @return  the report
   */
  public String getHddModelReport() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_HDD_MODEL_REPORT, new UtfResponse());
  }

  /**
   * Gets a filesystems CSV report.
   *
   * @return  the report
   */
  public String getFilesystemsCsvReport() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_FILESYSTEMS_CSV_REPORT, new UtfResponse());
  }

  /**
   * Gets a load average report.
   *
   * @return  the report
   */
  public String getLoadAvgReport() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_AO_SERVER_LOADAVG_REPORT, new UtfResponse());
  }

  /**
   * Gets a meminfo report.
   *
   * @return  the report
   */
  public String getMemInfoReport() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_AO_SERVER_MEMINFO_REPORT, new UtfResponse());
  }

  /**
   * Checks a port from the server point of view.
   *
   * @return  the result
   */
  public String checkPort(
      InetAddress ipAddress,
      Port port,
      String appProtocol,
      String monitoringParameters
  ) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.CHECK_PORT,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(ipAddress.toString());
          out.writeCompressedInt(port.getPort());
          if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_80_0) < 0) {
            // Old protocol transferred lowercase
            out.writeUTF(port.getProtocol().name().toLowerCase(Locale.ROOT));
          } else {
            out.writeEnum(port.getProtocol());
          }
          out.writeUTF(appProtocol);
          out.writeUTF(monitoringParameters);
        },
        new UtfResponse()
    );
  }

  /**
   * Checks for a SMTP blacklist from the server point of view.
   *
   * @return  the status line
   */
  public String checkSmtpBlacklist(InetAddress sourceIp, InetAddress connectIp) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.CHECK_SMTP_BLACKLIST,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(sourceIp.toString());
          out.writeUTF(connectIp.toString());
        },
        new UtfResponse()
    );
  }

  public List<Certificate.Check> checkSslCertificate(int sslCertificate, boolean allowCached) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.CHECK_SSL_CERTIFICATE,
        (conn, out) -> {
          out.writeCompressedInt(sslCertificate);
          if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_83_0) >= 0) {
            out.writeBoolean(allowCached);
          }
        },
        new ResultResponse<>() {
          @Override
          protected boolean before(AoservDaemonConnection conn) throws IOException, SQLException {
            if (conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_81_10) < 0) {
              result = Collections.singletonList(new Certificate.Check(
                  "Daemon Protocol",
                  conn.getProtocolVersion().toString(),
                  AlertLevel.UNKNOWN,
                  "Protocol version does not support checking SSL certificates, please installed AOServ Daemon >= " + AoservDaemonProtocol.Version.VERSION_1_81_10
              )
              );
              return false;
            } else {
              return true;
            }
          }

          @Override
          protected void next(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            assert conn.getProtocolVersion().compareTo(AoservDaemonProtocol.Version.VERSION_1_81_10) >= 0;
            int size = in.readCompressedInt();
            List<Certificate.Check> results = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
              results.add(
                  new Certificate.Check(
                      in.readUTF(),
                      in.readUTF(),
                      AlertLevel.valueOf(in.readUTF()),
                      in.readNullUTF()
                  )
              );
            }
            result = results;
          }
        }
    );
  }

  /**
   * Gets the current system time.
   *
   * @return  the report
   */
  public long getSystemTimeMillis() throws IOException, SQLException {
    return requestResult(AoservDaemonProtocol.GET_AO_SERVER_SYSTEM_TIME_MILLIS, new LongResponse());
  }

  /**
   * Gets the list of servers configured to auto-start in /etc/xen/auto.
   */
  public Set<String> getXenAutoStartLinks() throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_XEN_AUTO_START_LINKS,
        new ResultResponse<>() {
          @Override
          protected void done(AoservDaemonConnection conn, StreamableInput in) throws IOException {
            int numLinks = in.readCompressedInt();
            Set<String> links = AoCollections.newLinkedHashSet(numLinks);
            for (int i = 0; i < numLinks; i++) {
              links.add(in.readUTF());
            }
            result = Collections.unmodifiableSet(links);
          }
        }
    );
  }

  /**
   * See {@link VirtualServer#create()}.
   */
  public String createVirtualServer(String virtualServer) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.CREATE_VIRTUAL_SERVER,
        new UtfRequest(virtualServer),
        new UtfResponse()
    );
  }

  /**
   * See {@link VirtualServer#reboot()}.
   */
  public String rebootVirtualServer(String virtualServer) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.REBOOT_VIRTUAL_SERVER,
        new UtfRequest(virtualServer),
        new UtfResponse()
    );
  }

  /**
   * See {@link VirtualServer#shutdown()}.
   */
  public String shutdownVirtualServer(String virtualServer) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.SHUTDOWN_VIRTUAL_SERVER,
        new UtfRequest(virtualServer),
        new UtfResponse()
    );
  }

  /**
   * See {@link VirtualServer#destroy()}.
   */
  public String destroyVirtualServer(String virtualServer) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.DESTROY_VIRTUAL_SERVER,
        new UtfRequest(virtualServer),
        new UtfResponse()
    );
  }

  /**
   * See {@link VirtualServer#pause()}.
   */
  public String pauseVirtualServer(String virtualServer) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.PAUSE_VIRTUAL_SERVER,
        new UtfRequest(virtualServer),
        new UtfResponse()
    );
  }

  /**
   * See {@link VirtualServer#unpause()}.
   */
  public String unpauseVirtualServer(String virtualServer) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.UNPAUSE_VIRTUAL_SERVER,
        new UtfRequest(virtualServer),
        new UtfResponse()
    );
  }

  /**
   * See {@link VirtualServer#getStatus()}.
   */
  public int getVirtualServerStatus(String virtualServer) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_VIRTUAL_SERVER_STATUS,
        new UtfRequest(virtualServer),
        new CompressedIntResponse()
    );
  }

  /**
   * Begins verification of a virtual disk, returns the Unix time in seconds since Epoch.
   */
  public long verifyVirtualDisk(String virtualServerName, String device) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.VERIFY_VIRTUAL_DISK,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(virtualServerName);
          out.writeUTF(device);
        },
        new LongResponse()
    );
  }

  /**
   * Updates the record of when a virtual disk was last verified.
   */
  public void updateVirtualDiskLastVerified(String virtualServerName, String device, long lastVerified) throws IOException, SQLException {
    requestVoid(
        AoservDaemonProtocol.UPDATE_VIRTUAL_DISK_LAST_UPDATED,
        (AoservDaemonConnection conn, StreamableOutput out) -> {
          out.writeUTF(virtualServerName);
          out.writeUTF(device);
          out.writeLong(lastVerified);
        }
    );
  }

  /**
   * Gets the current concurrency for a HTTP server.
   *
   * @return  the concurrency
   */
  public int getHttpdServerConcurrency(int httpdServer) throws IOException, SQLException {
    return requestResult(
        AoservDaemonProtocol.GET_HTTPD_SERVER_CONCURRENCY,
        new CompressedIntRequest(httpdServer),
        new CompressedIntResponse()
    );
  }
}
