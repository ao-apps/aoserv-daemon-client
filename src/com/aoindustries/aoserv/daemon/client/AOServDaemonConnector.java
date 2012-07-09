package com.aoindustries.aoserv.daemon.client;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServProtocol;
import com.aoindustries.aoserv.client.DaemonProfile;
import com.aoindustries.aoserv.client.FailoverMySQLReplication;
import com.aoindustries.aoserv.client.InboxAttributes;
import com.aoindustries.aoserv.client.MySQLDatabase.CheckTableResult;
import com.aoindustries.aoserv.client.MySQLDatabase.Engine;
import com.aoindustries.aoserv.client.MySQLDatabase.TableStatus;
import com.aoindustries.aoserv.client.MySQLServer;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.VirtualServer;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.util.BufferManager;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A <code>AOServConnector</code> provides the connection
 * between the object layer and the data.  This connection
 * may be persistant over TCP sockets, or it may be request
 * based like HTTP.  The implementing class is responsible
 * for getting the cache updates to <code>cacheInvalidated</code>
 * by any means possible.
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
    final String hostname;
    
    /**
     * The local IP address to connect from.
     */
    final String local_ip;

    /**
     * The port to connect to.
     */
    final int port;

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
        String hostname,
        String local_ip,
        int port,
        String protocol,
        String key,
        int poolSize,
        long maxConnectionAge,
        String trustStore,
        String trustStorePassword,
        Logger logger
    ) throws IOException {
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
    public long copyHomeDirectory(String username, AOServDaemonConnector to_connector) throws IOException, SQLException {
        // Establish the connection to the source
        AOServDaemonConnection sourceConn=getConnection();
        try {
            CompressedDataOutputStream sourceOut=sourceConn.getOutputStream();
            sourceOut.writeCompressedInt(AOServDaemonProtocol.TAR_HOME_DIRECTORY);
            sourceOut.writeUTF(username);
            sourceOut.flush();

            CompressedDataInputStream sourceIn=sourceConn.getInputStream();

            // Establish the connection to the destination
            AOServDaemonConnection destConn=to_connector.getConnection();
            try {
                CompressedDataOutputStream destOut=destConn.getOutputStream();
                destOut.writeCompressedInt(AOServDaemonProtocol.UNTAR_HOME_DIRECTORY);
                destOut.writeUTF(username);

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
                    BufferManager.release(buff);
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

    public void dumpMySQLDatabase(int pkey, CompressedDataOutputStream masterOut) throws IOException, SQLException {
        transferStream(AOServDaemonProtocol.DUMP_MYSQL_DATABASE, pkey, masterOut);
    }

    public void dumpPostgresDatabase(int pkey, CompressedDataOutputStream masterOut) throws IOException, SQLException {
        transferStream(AOServDaemonProtocol.DUMP_POSTGRES_DATABASE, pkey, masterOut);
    }

    public String getAutoresponderContent(String path) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_AUTORESPONDER_CONTENT);
            out.writeUTF(path);
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
        String hostname,
        String local_ip,
        int port,
        String protocol,
        String key,
        int poolSize,
        long maxConnectionAge,
        String trustStore,
        String trustStorePassword,
        Logger logger
    ) throws IOException {
        if(hostname==null) throw new NullPointerException("hostname is null");
        if(local_ip==null) throw new NullPointerException("local_ip is null");
        if(protocol==null) throw new NullPointerException("protocol is null");

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
    public String getCronTable(String username) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_CRON_TABLE);
            out.writeUTF(username);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_NET_DEVICE_BONDING_REPORT);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_NET_DEVICE_STATISTICS_REPORT);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.IS_PROCMAIL_MANUAL);
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
     * Gets the profiling information for the daemon
     */
    public void getDaemonProfile(List<DaemonProfile> objs) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_DAEMON_PROFILE);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result;
            while((result=in.read())==AOServDaemonProtocol.NEXT) {
                DaemonProfile dp=new DaemonProfile();
                dp.read(in);
                objs.add(dp);
            }
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
    }

    /**
     * Gets the total size of a mounted filesystem in bytes.
     */
    public long getDiskDeviceTotalSize(String path) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection(2);
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_DISK_DEVICE_TOTAL_SIZE);
            out.writeUTF(path);
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
    public long getDiskDeviceUsedSize(String path) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection(2);
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_DISK_DEVICE_USED_SIZE);
            out.writeUTF(path);
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
    public String getEmailListFile(String path) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_EMAIL_LIST_FILE);
            out.writeUTF(path);
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
    public String getEncryptedLinuxAccountPassword(String username) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD);
            out.writeUTF(username);
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

    public long[] getImapFolderSizes(String username, String[] folderNames) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_IMAP_FOLDER_SIZES);
            out.writeUTF(username);
            out.writeCompressedInt(folderNames.length);
            for(int c=0;c<folderNames.length;c++) {
                out.writeUTF(folderNames[c]);
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

    public InboxAttributes getInboxAttributes(String username) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_INBOX_ATTRIBUTES);
            out.writeUTF(username);
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
            CompressedDataOutputStream daemonOut=conn.getOutputStream();
            daemonOut.writeCompressedInt(AOServDaemonProtocol.GET_MRTG_FILE);
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
                BufferManager.release(buff);
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
            CompressedDataOutputStream daemonOut=conn.getOutputStream();
            daemonOut.writeCompressedInt(AOServDaemonProtocol.GET_MYSQL_MASTER_STATUS);
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
        String failoverRoot,
        int nestedOperatingSystemVersion,
        int port
    ) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream daemonOut=conn.getOutputStream();
            daemonOut.writeCompressedInt(AOServDaemonProtocol.GET_MYSQL_SLAVE_STATUS);
            daemonOut.writeUTF(failoverRoot);
            daemonOut.writeCompressedInt(nestedOperatingSystemVersion);
            daemonOut.writeCompressedInt(port);
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
        String failoverRoot,
        int nestedOperatingSystemVersion,
        int port,
        String databaseName
    ) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream daemonOut=conn.getOutputStream();
            daemonOut.writeCompressedInt(AOServDaemonProtocol.GET_MYSQL_TABLE_STATUS);
            daemonOut.writeUTF(failoverRoot);
            daemonOut.writeCompressedInt(nestedOperatingSystemVersion);
            daemonOut.writeCompressedInt(port);
            daemonOut.writeUTF(databaseName);
            daemonOut.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.NEXT) {
                int size = in.readCompressedInt();
                List<TableStatus> tableStatuses = new ArrayList<TableStatus>(size);
                for(int c=0;c<size;c++) {
                    tableStatuses.add(
                        new TableStatus(
                            in.readUTF(), // name
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
        String failoverRoot,
        int nestedOperatingSystemVersion,
        int port,
        String databaseName,
        List<String> tableNames
    ) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream daemonOut=conn.getOutputStream();
            daemonOut.writeCompressedInt(AOServDaemonProtocol.CHECK_MYSQL_TABLES);
            daemonOut.writeUTF(failoverRoot);
            daemonOut.writeCompressedInt(nestedOperatingSystemVersion);
            daemonOut.writeCompressedInt(port);
            daemonOut.writeUTF(databaseName);
            int numTables = tableNames.size();
            daemonOut.writeCompressedInt(numTables);
            for(int c=0;c<numTables;c++) daemonOut.writeUTF(tableNames.get(c));
            daemonOut.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.NEXT) {
                int size = in.readCompressedInt();
                List<CheckTableResult> checkTableResults = new ArrayList<CheckTableResult>(size);
                for(int c=0;c<size;c++) {
                    checkTableResults.add(
                        new CheckTableResult(
                            in.readUTF(), // table
                            in.readLong(), // duration
                            in.readNullEnum(CheckTableResult.MsgType.class), // msgType
                            in.readNullUTF() // msgText
                        )
                    );
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
            CompressedDataOutputStream daemonOut=conn.getOutputStream();
            daemonOut.writeCompressedInt(AOServDaemonProtocol.GET_AWSTATS_FILE);
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
                BufferManager.release(buff);
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
    public boolean compareLinuxAccountPassword(String username, String password) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.COMPARE_LINUX_ACCOUNT_PASSWORD);
            out.writeUTF(username);
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
    public String getEncryptedMySQLUserPassword(int mysqlServer, String username) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_ENCRYPTED_MYSQL_USER_PASSWORD);
            out.writeCompressedInt(mysqlServer);
            out.writeUTF(username);
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
    public String getHostname() {
        return hostname;
    }

    /**
     * Gets the local IP address that connections are established from.
     */
    public String getLocalIp() {
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
    public int getPort() {
        return port;
    }

    /**
     * Gets the password for a PostgreSQL user as found in pg_shadow or pg_authid table.
     */
    public String getPostgresUserPassword(int pkey) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_POSTGRES_PASSWORD);
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
        String param3
    ) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GRANT_DAEMON_ACCESS);
            out.writeLong(key);
            out.writeCompressedInt(command);
            out.writeBoolean(param1!=null); if(param1!=null) out.writeUTF(param1);
            out.writeBoolean(param2!=null); if(param2!=null) out.writeUTF(param2);
            out.writeBoolean(param3!=null); if(param3!=null) out.writeUTF(param3);
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
    public void removeEmailList(String listPath) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.REMOVE_EMAIL_LIST);
            out.writeUTF(listPath);
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
    }

    /**
     * Controls a process.
     */
    private void controlProcess(int command) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(command);
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
    }

    /**
     * Controls a process.
     */
    private void controlProcess(int command, int param1) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(command);
            out.writeCompressedInt(param1);
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

    public void setAutoresponderContent(String path, String content, int uid, int gid) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.SET_AUTORESPONDER_CONTENT);
            out.writeUTF(path);
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
    public void setCronTable(String username, String cronTable) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.SET_CRON_TABLE);
            out.writeUTF(username);
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
    public void setEmailListFile(String path, String file, int uid, int gid, int mode) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.SET_EMAIL_LIST_FILE);
            out.writeUTF(path);
            out.writeUTF(file);
            out.writeCompressedInt(uid);
            out.writeCompressedInt(gid);
            out.writeCompressedInt(mode);
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
    }

    /**
     * Sets the encrypted password for a Linux account.
     */
    public void setEncryptedLinuxAccountPassword(String username, String encryptedPassword) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.SET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD);
            out.writeUTF(username);
            out.writeUTF(encryptedPassword);
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
    }

    /**
     * Sets the password for a <code>LinuxServerAccount</code>.
     */
    public void setLinuxServerAccountPassword(String username, String plain_password) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.SET_LINUX_SERVER_ACCOUNT_PASSWORD);
            out.writeUTF(username);
            out.writeUTF(plain_password);
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
    }

    /**
     * Subscribes/unsubscribes to an IMAP folder.
     *
     * @param  username  the username to copy the home directory of
     * @param  folderName  the folderName, should include a trailing / for a folder that holds folders
     * @param  subscribed  the new subscribes state
     */
    public void setImapFolderSubscribed(String username, String folderName, boolean subscribed) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.SET_IMAP_FOLDER_SUBSCRIBED);
            out.writeUTF(username);
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
    public void setMySQLUserPassword(int mysqlServer, String username, String password) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.SET_MYSQL_USER_PASSWORD);
            out.writeCompressedInt(mysqlServer);
            out.writeUTF(username);
            out.writeBoolean(password!=null); if(password!=null) out.writeUTF(password);
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
    }

    /**
     * Sets the password for a <code>PostgresServerUser</code>.
     */
    public void setPostgresUserPassword(int pkey, String password) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.SET_POSTGRES_USER_PASSWORD);
            out.writeCompressedInt(pkey);
            out.writeBoolean(password!=null); if(password!=null) out.writeUTF(password);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.START_DISTRO);
            out.writeBoolean(includeUser);
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
    }

    /**
     * Starts a Java VM.
     */
    public String startJVM(int httpdSite) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.START_JVM);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.STOP_JVM);
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
        CompressedDataOutputStream masterOut
    ) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(command);
            out.writeCompressedInt(param1);
            out.flush();

            transferStream0(conn, masterOut);
        } catch(IOException err) {
            conn.close();
            throw err;
        } finally {
            releaseConnection(conn);
        }
    }

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
            }*/
            transferStream0(conn, masterOut);
        } catch(IOException err) {
            conn.close();
            throw err;
        } finally {
            releaseConnection(conn);
        }
    }

    private void transferStream0(
        AOServDaemonConnection conn,
        CompressedDataOutputStream masterOut
    ) throws IOException, SQLException {
        CompressedDataInputStream in=conn.getInputStream();
        int code;
        byte[] buff=BufferManager.getBytes();
        try {
            while((code=in.read())==AOServDaemonProtocol.NEXT) {
                int len=in.readShort();
                in.readFully(buff, 0, len);
                masterOut.writeByte(AOServProtocol.NEXT);
                masterOut.writeShort(len);
                masterOut.write(buff, 0, len);
                //if(reporter!=null) reporter.addFinishedSize(len);
            }
        } finally {
            BufferManager.release(buff);
        }
        if (code == AOServDaemonProtocol.DONE) return;
        else if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
        else if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
        else throw new IOException("Unknown result: " + code);
    }

    private void waitFor(SchemaTable.TableID tableID) throws IOException, SQLException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.WAIT_FOR_REBUILD);
            out.writeCompressedInt(tableID.ordinal());
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
    }

    public void waitForHttpdSiteRebuild() throws IOException, SQLException {
        waitFor(SchemaTable.TableID.HTTPD_SITES);
    }

    public void waitForLinuxAccountRebuild() throws IOException, SQLException {
        waitFor(SchemaTable.TableID.LINUX_ACCOUNTS);
    }

    public void waitForMySQLDatabaseRebuild() throws IOException, SQLException {
        waitFor(SchemaTable.TableID.MYSQL_DATABASES);
    }

    public void waitForMySQLDBUserRebuild() throws IOException, SQLException {
        waitFor(SchemaTable.TableID.MYSQL_DB_USERS);
    }

    public void waitForMySQLUserRebuild() throws IOException, SQLException {
        waitFor(SchemaTable.TableID.MYSQL_USERS);
    }

    public void waitForPostgresDatabaseRebuild() throws IOException, SQLException {
        waitFor(SchemaTable.TableID.POSTGRES_DATABASES);
    }

    public void waitForPostgresServerRebuild() throws IOException, SQLException {
        waitFor(SchemaTable.TableID.POSTGRES_SERVERS);
    }

    public void waitForPostgresUserRebuild() throws IOException, SQLException {
        waitFor(SchemaTable.TableID.POSTGRES_USERS);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_3WARE_RAID_REPORT);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_UPS_STATUS);
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
     * Gets a MD RAID report.
     *
     * @return  the report
     */
    public String getMdRaidReport() throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_MD_RAID_REPORT);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_DRBD_REPORT);
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
     * Gets a LVM report.
     *
     * @return  the report
     */
    public String[] getLvmReport() throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_LVM_REPORT);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_HDD_TEMP_REPORT);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_HDD_MODEL_REPORT);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_FILESYSTEMS_CSV_REPORT);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_AO_SERVER_LOADAVG_REPORT);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_AO_SERVER_MEMINFO_REPORT);
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
    public String checkPort(String ipAddress, int port, String netProtocol, String appProtocol, String monitoringParameters) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.CHECK_PORT);
            out.writeUTF(ipAddress);
            out.writeCompressedInt(port);
            out.writeUTF(netProtocol);
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
    public String checkSmtpBlacklist(String sourceIp, String connectIp) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.CHECK_SMTP_BLACKLIST);
            out.writeUTF(sourceIp);
            out.writeUTF(connectIp);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_AO_SERVER_SYSTEM_TIME_MILLIS);
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
     * @see  VirtualServer#create()
     */
    public String createVirtualServer(String virtualServer) throws IOException, SQLException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.CREATE_VIRTUAL_SERVER);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.REBOOT_VIRTUAL_SERVER);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.SHUTDOWN_VIRTUAL_SERVER);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.DESTROY_VIRTUAL_SERVER);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.PAUSE_VIRTUAL_SERVER);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.UNPAUSE_VIRTUAL_SERVER);
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
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.CREATE_VIRTUAL_SERVER);
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
}
