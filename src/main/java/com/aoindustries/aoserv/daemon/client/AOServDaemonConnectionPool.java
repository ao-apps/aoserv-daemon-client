/*
 * aoserv-daemon-client - Java client for the AOServ Daemon.
 * Copyright (C) 2001-2009, 2016, 2017, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.hodgepodge.io.AOPool;
import com.aoapps.security.Password;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.logging.Logger;

/**
 * Connections made by <code>TCPConnector</code> or any
 * of its derivatives are pooled and reused.
 *
 * @author  AO Industries, Inc.
 */
final class AOServDaemonConnectionPool extends AOPool<AOServDaemonConnection, IOException, InterruptedIOException> {

	private final AOServDaemonConnector connector;

	AOServDaemonConnectionPool(AOServDaemonConnector connector, Logger logger) {
		super(
			AOServDaemonConnectionPool.class.getName()+"?hostname=" + connector.hostname+"&local_ip="+connector.local_ip+"&port="+connector.port+"&protocol="+connector.protocol,
			connector.poolSize,
			connector.maxConnectionAge,
			logger
		);
		this.connector=connector;
	}

	@Override
	protected void close(AOServDaemonConnection conn) throws IOException {
		conn.abort();
	}

	// Expose to package
	@Override
	protected void release(AOServDaemonConnection conn) throws IOException {
		super.release(conn);
	}

	@Override
	protected AOServDaemonConnection getConnectionObject() throws InterruptedIOException, IOException {
		return new AOServDaemonConnection(connector);
	}

	@Override
	protected boolean isClosed(AOServDaemonConnection conn) {
		return conn.isClosed();
	}

	@Override
	@SuppressWarnings("deprecation")
	protected void printConnectionStats(Appendable out, boolean isXhtml) throws IOException {
		out.append("  <thead>\n"
				+ "    <tr><th colspan=\"2\"><span style=\"font-size:large\">AOServ Daemon Connection Pool</span></th></tr>\n"
				+ "  </thead>\n");
		super.printConnectionStats(out, isXhtml);
		out.append("    <tr><td>Local IP:</td><td>");
		com.aoapps.hodgepodge.util.EncodingUtils.encodeHtml(connector.local_ip.toString(), out, isXhtml);
		out.append("</td></tr>\n"
				+ "    <tr><td>Host:</td><td>");
		com.aoapps.hodgepodge.util.EncodingUtils.encodeHtml(connector.hostname.toString(), out, isXhtml);
		out.append("</td></tr>\n"
				+ "    <tr><td>Port:</td><td>").append(Integer.toString(connector.port.getPort())).append("</td></tr>\n"
				+ "    <tr><td>Protocol:</td><td>");
		com.aoapps.hodgepodge.util.EncodingUtils.encodeHtml(connector.protocol, out, isXhtml);
		out.append("</td></tr>\n"
				+ "    <tr><td>Key:</td><td>");
		if(connector.key != null) out.append(Password.MASKED_PASSWORD);
		out.append("</td></tr>\n");
	}

	@Override
	protected void resetConnection(AOServDaemonConnection conn) {
	}

	@Override
	protected IOException newException(String message, Throwable cause) {
		if(cause instanceof IOException) return (IOException)cause;
		if(cause instanceof InterruptedException) return newInterruptedException(message, cause);
		if(message == null) {
			if(cause == null) {
				return new IOException();
			} else {
				return new IOException(cause);
			}
		} else {
			if(cause == null) {
				return new IOException(message);
			} else {
				return new IOException(message, cause);
			}
		}
	}

	@Override
	protected InterruptedIOException newInterruptedException(String message, Throwable cause) {
		// Restore the interrupted status
		Thread.currentThread().interrupt();
		if(cause instanceof InterruptedIOException) return (InterruptedIOException)cause;
		if(message == null) {
			if(cause == null) {
				return new InterruptedIOException();
			} else {
				InterruptedIOException err = new InterruptedIOException(cause.toString());
				err.initCause(cause);
				return err;
			}
		} else {
			if(cause == null) {
				return new InterruptedIOException(message);
			} else {
				InterruptedIOException err = new InterruptedIOException(message);
				err.initCause(cause);
				return err;
			}
		}
	}
}
