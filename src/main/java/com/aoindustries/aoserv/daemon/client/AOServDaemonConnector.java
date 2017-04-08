/*
 * aoserv-daemon-client - Java client for the AOServ Daemon.
 * Copyright (C) 2001-2013, 2014, 2015, 2016, 2017  AO Industries, Inc.
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

import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.FailoverMySQLReplication;
import com.aoindustries.aoserv.client.InboxAttributes;
import com.aoindustries.aoserv.client.MySQLDatabase.CheckTableResult;
import com.aoindustries.aoserv.client.MySQLDatabase.Engine;
import com.aoindustries.aoserv.client.MySQLDatabase.TableStatus;
import com.aoindustries.aoserv.client.MySQLServer;
import com.aoindustries.aoserv.client.validator.MySQLDatabaseName;
import com.aoindustries.aoserv.client.validator.MySQLTableName;
import com.aoindustries.aoserv.client.validator.MySQLUserId;
import com.aoindustries.aoserv.client.validator.UnixPath;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.lang.NullArgumentException;
import com.aoindustries.net.HostAddress;
import com.aoindustries.net.InetAddress;
import com.aoindustries.net.Port;
import com.aoindustries.util.BufferManager;
import com.aoindustries.util.Tuple2;
import com.aoindustries.validation.ValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A <code>AOServDaemonConnector</code> provides the
 * connections between a client and a server-control daemon.
 *
 * @author  AO Industries, Inc.
 */
final public class AOServDaemonConnector {

	/**
	 * Each unique connector is only created once.
	 */
	private static final List<AOServDaemonConnector> connectors=new ArrayList<AOServDaemonConnector>();

	/**
	 * The hostname to connect to.
	 */
	final HostAddress hostname;

	/**
	 * The local IP address to connect from.
	 */
	final InetAddress local_ip;

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
	final String key;

	final int poolSize;

	final long maxConnectionAge;

	final String trustStore;

	final String trustStorePassword;

	private final AOServDaemonConnectionPool pool;

	/**
	 * Creates a new <code>AOServConnector</code>.
	 */
	private AOServDaemonConnector(
		HostAddress hostname,
		InetAddress local_ip,
		Port port,
		String protocol,
		String key,
		int poolSize,
		long maxConnectionAge,
		String trustStore,
		String trustStorePassword,
		Logger logger
	) throws IOException {
		if(port.getProtocol() != com.aoindustries.net.Protocol.TCP) throw new IllegalArgumentException("Only TCP supported: " + port);
		this.hostname=hostname;
		this.local_ip=local_ip;
		this.port=port;
		this.protocol=protocol;
		this.key=key;
		this.poolSize=poolSize;
		this.maxConnectionAge=maxConnectionAge;
		this.trustStore=trustStore;
		this.trustStorePassword=trustStorePassword;
		this.pool=new AOServDaemonConnectionPool(this, logger);
	}

	/**
	 * Copies a home directory.
	 *
	 * @param  username  the username to copy the home directory of
	 * @param  to_connector  the connector to send the data to
	 *
	 * @return  the number of bytes transferred
	 */
	public long copyHomeDirectory(UserId username, AOServDaemonConnector to_connector) throws IOException, SQLException {
		// Establish the connection to the source
		AOServDaemonConnection sourceConn=getConnection();
		try {
			CompressedDataOutputStream sourceOut = sourceConn.getOutputStream(AOServDaemonProtocol.TAR_HOME_DIRECTORY);
			sourceOut.writeUTF(username.toString());
			sourceOut.flush();

			CompressedDataInputStream sourceIn=sourceConn.getInputStream();

			// Establish the connection to the destination
			AOServDaemonConnection destConn=to_connector.getConnection();
			try {
				CompressedDataOutputStream destOut = destConn.getOutputStream(AOServDaemonProtocol.UNTAR_HOME_DIRECTORY);
				destOut.writeUTF(username.toString());

				long byteCount=0;
				int sourceCode;
				byte[] buff=BufferManager.getBytes();
				try {
					while((sourceCode=sourceIn.read())==AOServDaemonProtocol.NEXT) {
						int len=sourceIn.readShort();
						byteCount+=len;
						sourceIn.readFully(buff, 0, len);
						destOut.writeByte(AOServDaemonProtocol.NEXT);
						destOut.writeShort(len);
						destOut.write(buff, 0, len);
					}
				} finally {
					BufferManager.release(buff, false);
				}
				if (sourceCode != AOServDaemonProtocol.DONE) {
					if (sourceCode == AOServDaemonProtocol.IO_EXCEPTION) {
						String message=sourceIn.readUTF();
						destOut.writeByte(AOServDaemonProtocol.IO_EXCEPTION);
						destOut.writeUTF(message);
						destOut.flush();
						throw new IOException(message);
					} else if (sourceCode == AOServDaemonProtocol.SQL_EXCEPTION) {
						String message=sourceIn.readUTF();
						destOut.writeByte(AOServDaemonProtocol.SQL_EXCEPTION);
						destOut.writeUTF(message);
						destOut.flush();
						throw new SQLException(message);
					} else throw new IOException("Unknown result: " + sourceCode);
				}
				destOut.writeByte(AOServDaemonProtocol.DONE);
				destOut.flush();

				CompressedDataInputStream destIn=destConn.getInputStream();
				int destResult=destIn.read();
				if(destResult!=AOServDaemonProtocol.DONE) {
					if (destResult == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(destIn.readUTF());
					else if (destResult == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(destIn.readUTF());
					else throw new IOException("Unknown result: " + destResult);
				}

				return byteCount;
			} catch(IOException err) {
				destConn.close();
				throw err;
			} finally {
				to_connector.releaseConnection(destConn);
			}
		} catch(IOException err) {
			sourceConn.close();
			throw err;
		} finally {
			releaseConnection(sourceConn);
		}
	}

	public static interface DumpSizeCallback {
		/**
		 * Called once the dump size is known and before
		 * the stream is written to.
		 *
		 * @param  dumpSize  The number of bytes that will be transferred or {@code -1} if unknown
		 */
		void onDumpSize(long dumpSize) throws IOException;
	}

	public void dumpMySQLDatabase(
		int pkey,
		boolean gzip,
		DumpSizeCallback onDumpSize,
		CompressedDataOutputStream masterOut
	) throws IOException, SQLException {
		transferStream(AOServDaemonProtocol.DUMP_MYSQL_DATABASE, pkey, gzip, onDumpSize, masterOut);
	}

	public void dumpPostgresDatabase(
		int pkey,
		boolean gzip,
		DumpSizeCallback onDumpSize,
		CompressedDataOutputStream masterOut
	) throws IOException, SQLException {
		transferStream(AOServDaemonProtocol.DUMP_POSTGRES_DATABASE, pkey, gzip, onDumpSize, masterOut);
	}

	public String getAutoresponderContent(UnixPath path) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_AUTORESPONDER_CONTENT);
			out.writeUTF(path.toString());
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
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
	public AOServDaemonConnection getConnection() throws IOException {
		try {
			return pool.getConnection();
		} catch(IOException err) {
			pool.getLogger().log(Level.INFO, "IOException while trying to get a connection to server from "+local_ip+" to "+hostname+":"+port, err);
			throw err;
		}
	}

	/**
	 * Allocates a connection to the server.  These connections must later be
	 * released with the <code>releaseConnection</code> method.  Connection
	 * pooling is obtained this way.  These connections may be over any protocol,
	 * so they may only be used for one client/server exchange at a time.
	 */
	public AOServDaemonConnection getConnection(int maxConnections) throws IOException {
		return pool.getConnection(maxConnections);
	}

	public int getConnectionCount() {
		return pool.getConnectionCount();
	}

	/**
	 * Gets the default <code>AOServConnector</code> as defined in the
	 * <code>client.properties</code> resource.  Each possible
	 * protocol is tried, in order, until a successful connection is
	 * made.  If no connection is made, an <code>IOException</code>
	 * is thrown.
	 */
	public synchronized static AOServDaemonConnector getConnector(
		HostAddress hostname,
		InetAddress local_ip,
		Port port,
		String protocol,
		String key,
		int poolSize,
		long maxConnectionAge,
		String trustStore,
		String trustStorePassword,
		Logger logger
	) throws IOException {
		NullArgumentException.checkNotNull(hostname, "hostname");
		NullArgumentException.checkNotNull(local_ip, "local_ip");
		NullArgumentException.checkNotNull(protocol, "protocol");

		int size=connectors.size();
		for(int c=0;c<size;c++) {
			AOServDaemonConnector connector=connectors.get(c);
			if(
				connector.hostname.equals(hostname)
				&& connector.local_ip.equals(local_ip)
				&& connector.port==port
				&& connector.protocol.equals(protocol)
				&& (key==null?connector.key==null:key.equals(connector.key))
				&& connector.poolSize==poolSize
				&& connector.maxConnectionAge==maxConnectionAge
			) return connector;
		}
		AOServDaemonConnector connector=new AOServDaemonConnector(
			hostname,
			local_ip,
			port,
			protocol,
			key,
			poolSize,
			maxConnectionAge,
			trustStore,
			trustStorePassword,
			logger
		);
		connectors.add(connector);
		return connector;
	}

	public long getConnects() {
		return pool.getConnects();
	}

	/**
	 * Gets a cron table.
	 *
	 * @param  username  the username to copy the home directory of
	 *
	 * @return  the cron table
	 */
	public String getCronTable(UserId username) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_CRON_TABLE);
			out.writeUTF(username.toString());
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a bonding report.
	 *
	 * @param  pkey  the unique ID of the net_device
	 *
	 * @return  the report
	 */
	public String getNetDeviceBondingReport(int pkey) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_NET_DEVICE_BONDING_REPORT);
			out.writeCompressedInt(pkey);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a statistics report.
	 *
	 * @param  pkey  the unique ID of the net_device
	 *
	 * @return  the report
	 */
	public String getNetDeviceStatisticsReport(int pkey) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_NET_DEVICE_STATISTICS_REPORT);
			out.writeCompressedInt(pkey);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Determines if the inbox is in manual procmail mode.
	 *
	 * @param  lsa  the pkey of the LinuxServerAccount
	 */
	public boolean isProcmailManual(int lsa) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.IS_PROCMAIL_MANUAL);
			out.writeCompressedInt(lsa);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readBoolean();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets the total size of a mounted filesystem in bytes.
	 */
	public long getDiskDeviceTotalSize(UnixPath path) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection(2);
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_DISK_DEVICE_TOTAL_SIZE);
			out.writeUTF(path.toString());
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) return in.readLong();
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets the used size of a mounted filesystem in bytes.
	 */
	public long getDiskDeviceUsedSize(UnixPath path) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection(2);
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_DISK_DEVICE_USED_SIZE);
			out.writeUTF(path.toString());
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) return in.readLong();
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets the file used by an email list.
	 */
	public String getEmailListFile(UnixPath path) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_EMAIL_LIST_FILE);
			out.writeUTF(path.toString());
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) return in.readUTF();
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets the encrypted password for a linux account as found in the /etc/shadow file.
	 */
	public Tuple2<String,Integer> getEncryptedLinuxAccountPassword(UserId username) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD);
			out.writeUTF(username.toString());
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) {
				String encryptedPassword = in.readUTF();
				Integer changedDate;
				if(conn.protocolVersion.compareTo(AOServDaemonProtocol.Version.VERSION_1_80_1_SNAPSHOT) >= 0) {
					int i = in.readCompressedInt();
					changedDate = i==-1 ? null : i;
				} else {
					changedDate = null;
				}
				return new Tuple2<String,Integer>(encryptedPassword, changedDate);
			}
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public long[] getImapFolderSizes(UserId username, String[] folderNames) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_IMAP_FOLDER_SIZES);
			out.writeUTF(username.toString());
			out.writeCompressedInt(folderNames.length);
			for(String folderName : folderNames) {
				out.writeUTF(folderName);
			}
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) {
				long[] sizes=new long[folderNames.length];
				for(int c=0;c<folderNames.length;c++) {
					sizes[c]=in.readLong();
				}
				return sizes;
			}
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public InboxAttributes getInboxAttributes(UserId username) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_INBOX_ATTRIBUTES);
			out.writeUTF(username.toString());
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) {
				return new InboxAttributes(in.readLong(), in.readLong());
			}
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public void getMrtgFile(String filename, CompressedDataOutputStream out) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream daemonOut = conn.getOutputStream(AOServDaemonProtocol.GET_MRTG_FILE);
			daemonOut.writeUTF(filename);
			daemonOut.flush();

			byte[] buff=BufferManager.getBytes();
			try {
				CompressedDataInputStream in=conn.getInputStream();
				int code;
				while((code=in.read())==AOServDaemonProtocol.NEXT) {
					int len=in.readShort();
					in.readFully(buff, 0, len);
					out.writeByte(AOServProtocol.NEXT);
					out.writeShort(len);
					out.write(buff, 0, len);
				}
				if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				if (code != AOServDaemonProtocol.DONE) throw new IOException("Unknown result: " + code);
			} finally {
				BufferManager.release(buff, false);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public MySQLServer.MasterStatus getMySQLMasterStatus(int mysqlServer) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream daemonOut = conn.getOutputStream(AOServDaemonProtocol.GET_MYSQL_MASTER_STATUS);
			daemonOut.writeCompressedInt(mysqlServer);
			daemonOut.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.NEXT) {
				return new MySQLServer.MasterStatus(
					in.readNullUTF(),
					in.readNullUTF()
				);
			} else if(code==AOServDaemonProtocol.DONE) {
				return null;
			} else if(code == AOServDaemonProtocol.IO_EXCEPTION) {
				throw new IOException(in.readUTF());
			} else if (code == AOServDaemonProtocol.SQL_EXCEPTION) {
				throw new SQLException(in.readUTF());
			} else {
				throw new IOException("Unknown result: " + code);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public FailoverMySQLReplication.SlaveStatus getMySQLSlaveStatus(
		UnixPath failoverRoot,
		int nestedOperatingSystemVersion,
		Port port
	) throws IOException, SQLException {
		if(port.getProtocol() != com.aoindustries.net.Protocol.TCP) throw new IllegalArgumentException("Only TCP supported: " + port);
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream daemonOut = conn.getOutputStream(AOServDaemonProtocol.GET_MYSQL_SLAVE_STATUS);
			daemonOut.writeUTF(failoverRoot==null ? "" : failoverRoot.toString());
			daemonOut.writeCompressedInt(nestedOperatingSystemVersion);
			daemonOut.writeCompressedInt(port.getPort());
			daemonOut.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.NEXT) {
				return new FailoverMySQLReplication.SlaveStatus(
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
			} else if(code==AOServDaemonProtocol.DONE) {
				return null;
			} else if(code == AOServDaemonProtocol.IO_EXCEPTION) {
				throw new IOException(in.readUTF());
			} else if (code == AOServDaemonProtocol.SQL_EXCEPTION) {
				throw new SQLException(in.readUTF());
			} else {
				throw new IOException("Unknown result: " + code);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public List<TableStatus> getMySQLTableStatus(
		UnixPath failoverRoot,
		int nestedOperatingSystemVersion,
		Port port,
		MySQLDatabaseName databaseName
	) throws IOException, SQLException {
		if(port.getProtocol() != com.aoindustries.net.Protocol.TCP) throw new IllegalArgumentException("Only TCP supported: " + port);
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream daemonOut = conn.getOutputStream(AOServDaemonProtocol.GET_MYSQL_TABLE_STATUS);
			daemonOut.writeUTF(failoverRoot==null ? "" : failoverRoot.toString());
			daemonOut.writeCompressedInt(nestedOperatingSystemVersion);
			daemonOut.writeCompressedInt(port.getPort());
			daemonOut.writeUTF(databaseName.toString());
			daemonOut.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.NEXT) {
				try {
					int size = in.readCompressedInt();
					List<TableStatus> tableStatuses = new ArrayList<TableStatus>(size);
					for(int c=0;c<size;c++) {
						tableStatuses.add(
							new TableStatus(
								MySQLTableName.valueOf(in.readUTF()), // name
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
					return tableStatuses;
				} catch(ValidationException e) {
					throw new IOException(e);
				}
			} else if(code == AOServDaemonProtocol.IO_EXCEPTION) {
				throw new IOException(in.readUTF());
			} else if (code == AOServDaemonProtocol.SQL_EXCEPTION) {
				throw new SQLException(in.readUTF());
			} else {
				throw new IOException("Unknown result: " + code);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public List<CheckTableResult> checkMySQLTables(
		UnixPath failoverRoot,
		int nestedOperatingSystemVersion,
		Port port,
		MySQLDatabaseName databaseName,
		List<? extends MySQLTableName> tableNames
	) throws IOException, SQLException {
		if(port.getProtocol() != com.aoindustries.net.Protocol.TCP) throw new IllegalArgumentException("Only TCP supported: " + port);
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream daemonOut = conn.getOutputStream(AOServDaemonProtocol.CHECK_MYSQL_TABLES);
			daemonOut.writeUTF(failoverRoot==null ? "" : failoverRoot.toString());
			daemonOut.writeCompressedInt(nestedOperatingSystemVersion);
			daemonOut.writeCompressedInt(port.getPort());
			daemonOut.writeUTF(databaseName.toString());
			int numTables = tableNames.size();
			daemonOut.writeCompressedInt(numTables);
			for(int c=0;c<numTables;c++) daemonOut.writeUTF(tableNames.get(c).toString());
			daemonOut.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.NEXT) {
				int size = in.readCompressedInt();
				List<CheckTableResult> checkTableResults = new ArrayList<CheckTableResult>(size);
				for(int c=0;c<size;c++) {
					try {
						checkTableResults.add(
							new CheckTableResult(
								MySQLTableName.valueOf(in.readUTF()), // table
								in.readLong(), // duration
								in.readNullEnum(CheckTableResult.MsgType.class), // msgType
								in.readNullUTF() // msgText
							)
						);
					} catch(ValidationException e) {
						throw new IOException(e);
					}
				}
				return checkTableResults;
			} else if(code == AOServDaemonProtocol.IO_EXCEPTION) {
				throw new IOException(in.readUTF());
			} else if (code == AOServDaemonProtocol.SQL_EXCEPTION) {
				throw new SQLException(in.readUTF());
			} else {
				throw new IOException("Unknown result: " + code);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public void getAWStatsFile(String siteName, String path, String queryString, CompressedDataOutputStream out) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream daemonOut = conn.getOutputStream(AOServDaemonProtocol.GET_AWSTATS_FILE);
			daemonOut.writeUTF(siteName);
			daemonOut.writeUTF(path);
			daemonOut.writeUTF(queryString);
			daemonOut.flush();

			byte[] buff=BufferManager.getBytes();
			try {
				CompressedDataInputStream in=conn.getInputStream();
				int code;
				while((code=in.read())==AOServDaemonProtocol.NEXT) {
					int len=in.readShort();
					in.readFully(buff, 0, len);
					out.writeByte(AOServProtocol.NEXT);
					out.writeShort(len);
					out.write(buff, 0, len);
				}
				if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				if (code != AOServDaemonProtocol.DONE) throw new IOException("Unknown result: " + code);
			} finally {
				BufferManager.release(buff, false);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Compares to the password list on the server.
	 */
	public boolean compareLinuxAccountPassword(UserId username, String password) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.COMPARE_LINUX_ACCOUNT_PASSWORD);
			out.writeUTF(username.toString());
			out.writeUTF(password);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) return in.readBoolean();
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets the encrypted password for a MySQL user as found in user table.
	 */
	public String getEncryptedMySQLUserPassword(int mysqlServer, MySQLUserId username) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_ENCRYPTED_MYSQL_USER_PASSWORD);
			out.writeCompressedInt(mysqlServer);
			out.writeUTF(username.toString());
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) return in.readUTF();
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
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
		return local_ip;
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

	/**
	 * Gets the password for a PostgreSQL user as found in pg_shadow or pg_authid table.
	 */
	public String getPostgresUserPassword(int pkey) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_POSTGRES_PASSWORD);
			out.writeCompressedInt(pkey);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) return in.readUTF();
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public long getTotalTime() {
		return pool.getTotalTime();
	}

	public long getTransactionCount() {
		return pool.getTransactionCount();
	}

	public void grantDaemonAccess(
		long key,
		int command,
		String param1,
		String param2,
		String param3,
		String param4
	) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GRANT_DAEMON_ACCESS);
			out.writeLong(key);
			out.writeCompressedInt(command);
			out.writeBoolean(param1!=null); if(param1!=null) out.writeUTF(param1);
			out.writeBoolean(param2!=null); if(param2!=null) out.writeUTF(param2);
			out.writeBoolean(param3!=null); if(param3!=null) out.writeUTF(param3);
			out.writeBoolean(param4!=null); if(param4!=null) out.writeUTF(param4);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code!=AOServDaemonProtocol.DONE) {
				if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				throw new IOException("Unknown result: " + code);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/*public void initializeHttpdSitePasswdFile(int sitePKey, String username, String encPassword) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out=conn.getOutputStream();
			out.writeCompressedInt(AOServDaemonProtocol.INITIALIZE_HTTPD_SITE_PASSWD_FILE);
			out.writeCompressedInt(sitePKey);
			out.writeUTF(username);
			out.writeUTF(encPassword);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) return;
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}*/

	public void printConnectionStatsHTML(Appendable out) throws IOException {
		pool.printConnectionStats(out);
	}

	/**
	 * Releases a connection to the server.  This will allow another thread
	 * to use the connection.  Connections may be of any protocol, so each
	 * connection must be released after every transaction.
	 */
	public void releaseConnection(AOServDaemonConnection connection) throws IOException {
		pool.releaseConnection(connection);
	}

	/**
	 * Deletes the contents of an email list
	 */
	public void removeEmailList(UnixPath listPath) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.REMOVE_EMAIL_LIST);
			out.writeUTF(listPath.toString());
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Controls a process.
	 */
	private void controlProcess(int command) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(command);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Controls a process.
	 */
	private void controlProcess(int command, int param1) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(command);
			out.writeCompressedInt(param1);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public void restartApache() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.RESTART_APACHE);
	}

	public void restartCron() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.RESTART_CRON);
	}

	public void restartMySQL(int mysqlServer) throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.RESTART_MYSQL, mysqlServer);
	}

	public void restartPostgres(int pkey) throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.RESTART_POSTGRES, pkey);
	}

	public void restartXfs() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.RESTART_XFS);
	}

	public void restartXvfb() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.RESTART_XVFB);
	}

	public void setAutoresponderContent(UnixPath path, String content, int uid, int gid) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.SET_AUTORESPONDER_CONTENT);
			out.writeUTF(path.toString());
			out.writeBoolean(content!=null);
			if(content!=null) out.writeUTF(content);
			out.writeCompressedInt(uid);
			out.writeCompressedInt(gid);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return;
			if(code==AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if(code==AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Sets a cron table.
	 *
	 * @param  username  the username to copy the home directory of
	 * @param  cronTable  the new cron table
	 */
	public void setCronTable(UserId username, String cronTable) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.SET_CRON_TABLE);
			out.writeUTF(username.toString());
			out.writeUTF(cronTable);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return;
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Sets the file used by an email list.
	 */
	public void setEmailListFile(UnixPath path, String file, int uid, int gid, int mode) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.SET_EMAIL_LIST_FILE);
			out.writeUTF(path.toString());
			out.writeUTF(file);
			out.writeCompressedInt(uid);
			out.writeCompressedInt(gid);
			out.writeCompressedInt(mode);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Sets the encrypted password for a Linux account.
	 */
	public void setEncryptedLinuxAccountPassword(UserId username, String encryptedPassword, Integer changedDate) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.SET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD);
			out.writeUTF(username.toString());
			out.writeUTF(encryptedPassword);
			if(conn.protocolVersion.compareTo(AOServDaemonProtocol.Version.VERSION_1_80_1_SNAPSHOT) >= 0) {
				out.writeCompressedInt(changedDate==null ? -1 : changedDate);
			}
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Sets the password for a <code>LinuxServerAccount</code>.
	 */
	public void setLinuxServerAccountPassword(UserId username, String plain_password) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.SET_LINUX_SERVER_ACCOUNT_PASSWORD);
			out.writeUTF(username.toString());
			out.writeUTF(plain_password);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Subscribes/unsubscribes to an IMAP folder.
	 *
	 * @param  username  the username to copy the home directory of
	 * @param  folderName  the folderName, should include a trailing / for a folder that holds folders
	 * @param  subscribed  the new subscribes state
	 */
	public void setImapFolderSubscribed(UserId username, String folderName, boolean subscribed) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.SET_IMAP_FOLDER_SUBSCRIBED);
			out.writeUTF(username.toString());
			out.writeUTF(folderName);
			out.writeBoolean(subscribed);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return;
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Sets the password for a <code>MySQLServerUser</code>.
	 */
	public void setMySQLUserPassword(int mysqlServer, MySQLUserId username, String password) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.SET_MYSQL_USER_PASSWORD);
			out.writeCompressedInt(mysqlServer);
			out.writeUTF(username.toString());
			out.writeBoolean(password!=null); if(password!=null) out.writeUTF(password);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Sets the password for a <code>PostgresServerUser</code>.
	 */
	public void setPostgresUserPassword(int pkey, String password) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.SET_POSTGRES_USER_PASSWORD);
			out.writeCompressedInt(pkey);
			out.writeBoolean(password!=null); if(password!=null) out.writeUTF(password);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public void startApache() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.START_APACHE);
	}

	public void startCron() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.START_CRON);
	}

	/**
	 * Starts a distribution verification.
	 */
	public void startDistro(boolean includeUser) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.START_DISTRO);
			out.writeBoolean(includeUser);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Starts a Java VM.
	 */
	public String startJVM(int httpdSite) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.START_JVM);
			out.writeCompressedInt(httpdSite);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) return in.readBoolean()?in.readUTF():null;
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public void startMySQL() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.START_MYSQL);
	}

	public void startPostgreSQL(int pkey) throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.START_POSTGRESQL, pkey);
	}

	public void startXfs() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.START_XFS);
	}

	public void startXvfb() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.START_XVFB);
	}

	public void stopApache() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.STOP_APACHE);
	}

	public void stopCron() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.STOP_CRON);
	}

	/**
	 * Stops a Java VM.
	 */
	public String stopJVM(int httpdSite) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.STOP_JVM);
			out.writeCompressedInt(httpdSite);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) return in.readBoolean()?in.readUTF():null;
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public void stopMySQL() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.STOP_MYSQL);
	}

	public void stopPostgreSQL(int pkey) throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.STOP_POSTGRESQL, pkey);
	}

	public void stopXfs() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.STOP_XFS);
	}

	public void stopXvfb() throws IOException, SQLException {
		controlProcess(AOServDaemonProtocol.STOP_XVFB);
	}

	@Override
	public String toString() {
		return getClass().getName()+"?hostname="+hostname+"&local_ip="+local_ip+"&port="+port+"&protocol="+protocol;
	}

	private void transferStream(
		int command,
		int param1,
		boolean gzip,
		DumpSizeCallback onDumpSize,
		CompressedDataOutputStream masterOut
	) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			if(gzip && conn.protocolVersion.compareTo(AOServDaemonProtocol.Version.VERSION_1_80_0) < 0) {
				throw new IOException(
					"Gzip compression requires AOServ Daemon version "
						+ AOServDaemonProtocol.Version.VERSION_1_80_0
						+ " or higher.  Current version is " + conn.protocolVersion + '.');
			}
			CompressedDataOutputStream out = conn.getOutputStream(command);
			out.writeCompressedInt(param1);
			if(conn.protocolVersion.compareTo(AOServDaemonProtocol.Version.VERSION_1_80_0) >= 0) {
				out.writeBoolean(gzip);
			}
			out.flush();

			transferStream0(conn, onDumpSize, masterOut);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/* Unused 2017-03-20
	private void transferStream(
		int command,
		String param1,
		CompressedDataOutputStream masterOut
	) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out=conn.getOutputStream();
			out.writeCompressedInt(command);
			out.writeUTF(param1);
			out.flush();

			transferStream0(conn, masterOut);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}
	 */

	/* Unused 2017-03-20
	private void transferStream(
		int command,
		String param1,
		CompressedDataOutputStream masterOut,
		long skipBytes
	) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out=conn.getOutputStream();
			out.writeCompressedInt(command);
			out.writeUTF(param1);
			out.writeLong(skipBytes);
			out.flush();

			/*if(reporter!=null) {
				long fileSize=conn.getInputStream().readLong();
				reporter.setTotalSize(fileSize);
				reporter.setFinishedSize(skipBytes);
			}* /
			transferStream0(conn, masterOut);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}
	 */

	private void transferStream0(
		AOServDaemonConnection conn,
		DumpSizeCallback onDumpSize,
		CompressedDataOutputStream masterOut
	) throws IOException, SQLException {
		CompressedDataInputStream in=conn.getInputStream();
		long dumpSize;
		if(conn.protocolVersion.compareTo(AOServDaemonProtocol.Version.VERSION_1_80_0) >= 0) {
			dumpSize = in.readLong();
		} else {
			dumpSize = -1;
		}
		if(dumpSize < -1) throw new IOException("dumpSize < -1: " + dumpSize);
		if(onDumpSize != null) onDumpSize.onDumpSize(dumpSize);
		long bytesRead = 0;
		{
			int code;
			byte[] buff=BufferManager.getBytes();
			try {
				while((code=in.read())==AOServDaemonProtocol.NEXT) {
					int len=in.readShort();
					bytesRead += len;
					if(dumpSize != -1 && bytesRead > dumpSize) throw new IOException("Too many bytes read: " + bytesRead + " > " + dumpSize);
					in.readFully(buff, 0, len);
					masterOut.writeByte(AOServProtocol.NEXT);
					masterOut.writeShort(len);
					masterOut.write(buff, 0, len);
					//if(reporter!=null) reporter.addFinishedSize(len);
				}
			} finally {
				BufferManager.release(buff, false);
			}
			if (code != AOServDaemonProtocol.DONE) {
				if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + code);
			}
		}
		if(dumpSize != -1 && bytesRead < dumpSize) throw new IOException("Too few bytes read: " + bytesRead + " < " + dumpSize);
	}

	private void waitFor(int taskCode) throws IOException, SQLException {
		AOServDaemonConnection conn = getConnection();
		try {
			CompressedDataOutputStream out;
			if(conn.protocolVersion.compareTo(AOServDaemonProtocol.Version.VERSION_1_80_0) < 0) {
				// Older protocol use a single WAIT_FOR_REBUILD with a follow-up table ID.
				// Table IDs can change over time, so the new protocol uses distinct task codes for each type of wait.
				// Find the table ID consistent with schema version 1.77
				final int tableId;
				switch(taskCode) {
					case AOServDaemonProtocol.WAIT_FOR_HTTPD_SITE_REBUILD :
						tableId = AOServDaemonProtocol.OLD_HTTPD_SITES_TABLE_ID;
						break;
					case AOServDaemonProtocol.WAIT_FOR_LINUX_ACCOUNT_REBUILD :
						tableId = AOServDaemonProtocol.OLD_LINUX_ACCOUNTS_TABLE_ID;
						break;
					case AOServDaemonProtocol.WAIT_FOR_MYSQL_DATABASE_REBUILD :
						tableId = AOServDaemonProtocol.OLD_MYSQL_DATABASES_TABLE_ID;
						break;
					case AOServDaemonProtocol.WAIT_FOR_MYSQL_DB_USER_REBUILD :
						tableId = AOServDaemonProtocol.OLD_MYSQL_DB_USERS_TABLE_ID;
						break;
					case AOServDaemonProtocol.WAIT_FOR_MYSQL_USER_REBUILD :
						tableId = AOServDaemonProtocol.OLD_MYSQL_USERS_TABLE_ID;
						break;
					case AOServDaemonProtocol.WAIT_FOR_POSTGRES_DATABASE_REBUILD :
						tableId = AOServDaemonProtocol.OLD_POSTGRES_DATABASES_TABLE_ID;
						break;
					case AOServDaemonProtocol.WAIT_FOR_POSTGRES_SERVER_REBUILD :
						tableId = AOServDaemonProtocol.OLD_POSTGRES_SERVERS_TABLE_ID;
						break;
					case AOServDaemonProtocol.WAIT_FOR_POSTGRES_USER_REBUILD :
						tableId = AOServDaemonProtocol.OLD_POSTGRES_USERS_TABLE_ID;
						break;
					default :
						throw new IOException("Unexpected taskCode: " + taskCode);

				}
				out = conn.getOutputStream(AOServDaemonProtocol.OLD_WAIT_FOR_REBUILD);
				out.writeCompressedInt(tableId);
			} else {
				out = conn.getOutputStream(taskCode);
			}
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public void waitForHttpdSiteRebuild() throws IOException, SQLException {
		waitFor(AOServDaemonProtocol.WAIT_FOR_HTTPD_SITE_REBUILD);
	}

	public void waitForLinuxAccountRebuild() throws IOException, SQLException {
		waitFor(AOServDaemonProtocol.WAIT_FOR_LINUX_ACCOUNT_REBUILD);
	}

	public void waitForMySQLDatabaseRebuild() throws IOException, SQLException {
		waitFor(AOServDaemonProtocol.WAIT_FOR_MYSQL_DATABASE_REBUILD);
	}

	public void waitForMySQLDBUserRebuild() throws IOException, SQLException {
		waitFor(AOServDaemonProtocol.WAIT_FOR_MYSQL_DB_USER_REBUILD);
	}

	public void waitForMySQLServerRebuild() throws IOException, SQLException {
		waitFor(AOServDaemonProtocol.WAIT_FOR_MYSQL_SERVER_REBUILD);
	}

	public void waitForMySQLUserRebuild() throws IOException, SQLException {
		waitFor(AOServDaemonProtocol.WAIT_FOR_MYSQL_USER_REBUILD);
	}

	public void waitForPostgresDatabaseRebuild() throws IOException, SQLException {
		waitFor(AOServDaemonProtocol.WAIT_FOR_POSTGRES_DATABASE_REBUILD);
	}

	public void waitForPostgresServerRebuild() throws IOException, SQLException {
		waitFor(AOServDaemonProtocol.WAIT_FOR_POSTGRES_SERVER_REBUILD);
	}

	public void waitForPostgresUserRebuild() throws IOException, SQLException {
		waitFor(AOServDaemonProtocol.WAIT_FOR_POSTGRES_USER_REBUILD);
	}

	/**
	 * Gets the error handler for this and its underlying connection pool.
	 */
	Logger getLogger() {
		return pool.getLogger();
	}

	/**
	 * Gets a 3ware RAID report.
	 *
	 * @return  the report
	 */
	public String get3wareRaidReport() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_3WARE_RAID_REPORT);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets the UPS status.
	 *
	 * @return  the report
	 */
	public String getUpsStatus() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_UPS_STATUS);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a /proc/mdstat report.
	 *
	 * @return  the report
	 */
	public String getMdStatReport() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_MD_STAT_REPORT);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a MD mismatch report.
	 *
	 * @return  the report
	 */
	public String getMdMismatchReport() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_MD_MISMATCH_REPORT);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a DRBD report.
	 *
	 * @return  the report
	 */
	public String getDrbdReport() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_DRBD_REPORT);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	public Tuple2<Long,String> getFailoverFileReplicationActivity(int replication) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_FAILOVER_FILE_REPLICATION_ACTIVITY);
			out.writeCompressedInt(replication);
			out.flush();

			CompressedDataInputStream in = conn.getInputStream();
			int code=in.read();
			if(code == AOServDaemonProtocol.DONE)          return new Tuple2<Long,String>(in.readLong(), in.readUTF());
			if(code == AOServDaemonProtocol.IO_EXCEPTION)  throw new IOException(in.readUTF());
			if(code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a LVM report.
	 *
	 * @return  the report
	 */
	public String[] getLvmReport() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_LVM_REPORT);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) {
				return new String[] {
					in.readUTF(),
					in.readUTF(),
					in.readUTF()
				};
			}
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a hard drive temperature report.
	 *
	 * @return  the report
	 */
	public String getHddTempReport() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_HDD_TEMP_REPORT);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a hard drive model report.
	 *
	 * @return  the report
	 */
	public String getHddModelReport() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_HDD_MODEL_REPORT);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a filesystems CSV report.
	 *
	 * @return  the report
	 */
	public String getFilesystemsCsvReport() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_FILESYSTEMS_CSV_REPORT);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a load average report.
	 *
	 * @return  the report
	 */
	public String getLoadAvgReport() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_AO_SERVER_LOADAVG_REPORT);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets a meminfo report.
	 *
	 * @return  the report
	 */
	public String getMemInfoReport() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_AO_SERVER_MEMINFO_REPORT);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
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
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.CHECK_PORT);
			out.writeUTF(ipAddress.toString());
			out.writeCompressedInt(port.getPort());
			if(conn.protocolVersion.compareTo(AOServDaemonProtocol.Version.VERSION_1_80_0) < 0) {
				// Old protocol transferred lowercase
				out.writeUTF(port.getProtocol().name().toLowerCase(Locale.ROOT));
			} else {
				out.writeEnum(port.getProtocol());
			}
			out.writeUTF(appProtocol);
			out.writeUTF(monitoringParameters);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Checks for a SMTP blacklist from the server point of view.
	 *
	 * @return  the status line
	 */
	public String checkSmtpBlacklist(InetAddress sourceIp, InetAddress connectIp) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.CHECK_SMTP_BLACKLIST);
			out.writeUTF(sourceIp.toString());
			out.writeUTF(connectIp.toString());
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets the current system time.
	 *
	 * @return  the report
	 */
	public long getSystemTimeMillis() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_AO_SERVER_SYSTEM_TIME_MILLIS);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readLong();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Gets the list of servers configured to auto-start in /etc/xen/auto.
	 */
	public Set<String> getXenAutoStartLinks() throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_XEN_AUTO_START_LINKS);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) {
				int numLinks = in.readCompressedInt();
				Set<String> links = new LinkedHashSet<String>(numLinks*4/3+1);
				for(int i=0; i<numLinks; i++) {
					links.add(in.readUTF());
				}
				return Collections.unmodifiableSet(links);
			}
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * @see  VirtualServer#create()
	 */
	public String createVirtualServer(String virtualServer) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.CREATE_VIRTUAL_SERVER);
			out.writeUTF(virtualServer);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * @see  VirtualServer#reboot()
	 */
	public String rebootVirtualServer(String virtualServer) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.REBOOT_VIRTUAL_SERVER);
			out.writeUTF(virtualServer);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * @see  VirtualServer#shutdown()
	 */
	public String shutdownVirtualServer(String virtualServer) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.SHUTDOWN_VIRTUAL_SERVER);
			out.writeUTF(virtualServer);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * @see  VirtualServer#destroy()
	 */
	public String destroyVirtualServer(String virtualServer) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.DESTROY_VIRTUAL_SERVER);
			out.writeUTF(virtualServer);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * @see  VirtualServer#pause()
	 */
	public String pauseVirtualServer(String virtualServer) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.PAUSE_VIRTUAL_SERVER);
			out.writeUTF(virtualServer);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * @see  VirtualServer#unpause()
	 */
	public String unpauseVirtualServer(String virtualServer) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.UNPAUSE_VIRTUAL_SERVER);
			out.writeUTF(virtualServer);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readUTF();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * @see  VirtualServer#getStatus()
	 */
	public int getVirtualServerStatus(String virtualServer) throws IOException, SQLException {
		// Establish the connection to the server
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.GET_VIRTUAL_SERVER_STATUS);
			out.writeUTF(virtualServer);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int code=in.read();
			if(code==AOServDaemonProtocol.DONE) return in.readCompressedInt();
			if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			throw new IOException("Unknown result: " + code);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Begins verification of a virtual disk, returns the Unix time in seconds since Epoch.
	 */
	public long verifyVirtualDisk(String virtualServerName, String device) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.VERIFY_VIRTUAL_DISK);
			out.writeUTF(virtualServerName);
			out.writeUTF(device);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result == AOServDaemonProtocol.DONE) return in.readLong();
			else if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
			else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
			else throw new IOException("Unknown result: " + result);
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}

	/**
	 * Updates the record of when a virtual disk was last verified
	 */
	public void updateVirtualDiskLastVerified(String virtualServerName, String device, long lastVerified) throws IOException, SQLException {
		AOServDaemonConnection conn=getConnection();
		try {
			CompressedDataOutputStream out = conn.getOutputStream(AOServDaemonProtocol.UPDATE_VIRTUAL_DISK_LAST_UPDATED);
			out.writeUTF(virtualServerName);
			out.writeUTF(device);
			out.writeLong(lastVerified);
			out.flush();

			CompressedDataInputStream in=conn.getInputStream();
			int result = in.read();
			if (result != AOServDaemonProtocol.DONE) {
				if (result == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
				else if (result == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
				else throw new IOException("Unknown result: " + result);
			}
		} catch(IOException err) {
			conn.close();
			throw err;
		} finally {
			releaseConnection(conn);
		}
	}
}
