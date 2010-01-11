package com.aoindustries.aoserv.daemon.client;

/*
 * Copyright 2001-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.FailoverMySQLReplication;
import com.aoindustries.aoserv.client.InboxAttributes;
import com.aoindustries.aoserv.client.MySQLDatabase;
import com.aoindustries.aoserv.client.MySQLServer;
import com.aoindustries.aoserv.client.ServiceName;
import com.aoindustries.aoserv.client.validator.Hostname;
import com.aoindustries.aoserv.client.validator.InetAddress;
import com.aoindustries.aoserv.client.validator.MySQLTableName;
import com.aoindustries.aoserv.client.validator.NetPort;
import com.aoindustries.aoserv.client.validator.ValidationException;
import com.aoindustries.io.CompressedDataInputStream;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.util.BufferManager;
import java.io.IOException;
import java.rmi.RemoteException;
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
    final Hostname hostname;
    
    /**
     * The local IP address to connect from.
     */
    final InetAddress local_ip;

    /**
     * The port to connect to.
     */
    final NetPort port;

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
        Hostname hostname,
        InetAddress local_ip,
        NetPort port,
        String protocol,
        String key,
        int poolSize,
        long maxConnectionAge,
        String trustStore,
        String trustStorePassword,
        Logger logger
    ) {
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
    public long copyHomeDirectory(String username, AOServDaemonConnector to_connector) throws RemoteException {
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
                    if (sourceCode == AOServDaemonProtocol.REMOTE_EXCEPTION) {
                        String message=sourceIn.readUTF();
                        destOut.writeByte(AOServDaemonProtocol.REMOTE_EXCEPTION);
                        destOut.writeUTF(message);
                        destOut.flush();
                        throw new RemoteException(message);
                    } else throw new RemoteException("Unknown result: " + sourceCode);
                }
                destOut.writeByte(AOServDaemonProtocol.DONE);
                destOut.flush();

                CompressedDataInputStream destIn=destConn.getInputStream();
                int destResult=destIn.read();
                if(destResult!=AOServDaemonProtocol.DONE) {
                    if (destResult == AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(destIn.readUTF());
                    else throw new RemoteException("Unknown result: " + destResult);
                }

                return byteCount;
            } catch(RuntimeException err) {
                destConn.close();
                throw err;
            } catch(RemoteException err) {
                destConn.close();
                throw err;
            } finally {
                to_connector.releaseConnection(destConn);
            }
        } catch(RuntimeException err) {
            sourceConn.close();
            throw err;
        } catch(RemoteException err) {
            sourceConn.close();
            throw err;
        } catch(IOException err) {
            sourceConn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(sourceConn);
        }
    }

    public void dumpMySQLDatabase(int pkey, CompressedDataOutputStream masterOut) throws RemoteException {
        transferStream(AOServDaemonProtocol.DUMP_MYSQL_DATABASE, pkey, masterOut);
    }

    public void dumpPostgresDatabase(int pkey, CompressedDataOutputStream masterOut) throws RemoteException {
        transferStream(AOServDaemonProtocol.DUMP_POSTGRES_DATABASE, pkey, masterOut);
    }

    public String getAutoresponderContent(String path) throws RemoteException {
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
            if (code == AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
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
    public AOServDaemonConnection getConnection() throws RemoteException {
        try {
            return pool.getConnection();
        } catch(RuntimeException err) {
            pool.getLogger().log(Level.INFO, "RuntimeException while trying to get a connection to server from "+local_ip+" to "+hostname+":"+port, err);
            throw err;
        } catch(RemoteException err) {
            pool.getLogger().log(Level.INFO, "RemoteException while trying to get a connection to server from "+local_ip+" to "+hostname+":"+port, err);
            throw err;
        } catch(IOException err) {
            pool.getLogger().log(Level.INFO, "IOExceptoin while trying to get a connection to server from "+local_ip+" to "+hostname+":"+port, err);
            throw new RemoteException(err.getMessage(), err);
        }
    }

    /**
     * Allocates a connection to the server.  These connections must later be
     * released with the <code>releaseConnection</code> method.  Connection
     * pooling is obtained this way.  These connections may be over any protocol,
     * so they may only be used for one client/server exchange at a time.
     */
    public AOServDaemonConnection getConnection(int maxConnections) throws RemoteException {
        try {
            return pool.getConnection(maxConnections);
        } catch(RuntimeException err) {
            pool.getLogger().log(Level.INFO, "RuntimeException while trying to get a connection to server from "+local_ip+" to "+hostname+":"+port, err);
            throw err;
        } catch(RemoteException err) {
            pool.getLogger().log(Level.INFO, "RemoteException while trying to get a connection to server from "+local_ip+" to "+hostname+":"+port, err);
            throw err;
        } catch(IOException err) {
            pool.getLogger().log(Level.INFO, "IOExceptoin while trying to get a connection to server from "+local_ip+" to "+hostname+":"+port, err);
            throw new RemoteException(err.getMessage(), err);
        }
    }

    public int getConnectionCount() {
        return pool.getConnectionCount();
    }

    /**
     * Gets the default <code>AOServConnector</code> as defined in the
     * <code>client.properties</code> resource.  Each possible
     * protocol is tried, in order, until a successful connection is
     * made.  If no connection is made, an <code>RemoteException</code>
     * is thrown.
     */
    public synchronized static AOServDaemonConnector getConnector(
        Hostname hostname,
        InetAddress local_ip,
        NetPort port,
        String protocol,
        String key,
        int poolSize,
        long maxConnectionAge,
        String trustStore,
        String trustStorePassword,
        Logger logger
    ) {
        if(hostname==null) throw new NullPointerException("hostname is null");
        if(local_ip==null) throw new NullPointerException("local_ip is null");
        if(protocol==null) throw new NullPointerException("protocol is null");

        int size=connectors.size();
        for(int c=0;c<size;c++) {
            AOServDaemonConnector connector=connectors.get(c);
            if(
                connector.hostname.equals(hostname)
                && connector.local_ip.equals(local_ip)
                && connector.port.equals(port)
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
    public String getCronTable(String username) throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
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
    public String getNetDeviceBondingReport(int pkey) throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
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
    public String getNetDeviceStatisticsReport(int pkey) throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Determines if the inbox is in manual procmail mode.
     *
     * @param  lsa  the pkey of the LinuxServerAccount
     */
    public boolean isProcmailManual(int lsa) throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets the total size of a mounted filesystem in bytes.
     */
    public long getDiskDeviceTotalSize(String path) throws RemoteException {
        AOServDaemonConnection conn=getConnection(2);
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_DISK_DEVICE_TOTAL_SIZE);
            out.writeUTF(path);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if(result==AOServDaemonProtocol.DONE) return in.readLong();
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets the used size of a mounted filesystem in bytes.
     */
    public long getDiskDeviceUsedSize(String path) throws RemoteException {
        AOServDaemonConnection conn=getConnection(2);
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_DISK_DEVICE_USED_SIZE);
            out.writeUTF(path);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if(result==AOServDaemonProtocol.DONE) return in.readLong();
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets the file used by an email list.
     */
    public String getEmailListFile(String path) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_EMAIL_LIST_FILE);
            out.writeUTF(path);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if(result==AOServDaemonProtocol.DONE) return in.readUTF();
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets the encrypted password for a linux account as found in the /etc/shadow file.
     */
    public String getEncryptedLinuxAccountPassword(String username) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD);
            out.writeUTF(username);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if(result==AOServDaemonProtocol.DONE) return in.readUTF();
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public long[] getImapFolderSizes(String username, String[] folderNames) throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public InboxAttributes getInboxAttributes(String username) throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public void getMrtgFile(String filename, CompressedDataOutputStream out) throws RemoteException {
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
                    out.writeByte(AOServDaemonProtocol.NEXT);
                    out.writeShort(len);
                    out.write(buff, 0, len);
                }
                if(code!=AOServDaemonProtocol.DONE) {
                    if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
                    throw new RemoteException("Unknown result: " + code);
                }
            } finally {
                BufferManager.release(buff);
            }
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public MySQLServer.MasterStatus getMySQLMasterStatus(int mysqlServer) throws RemoteException {
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
            }
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public FailoverMySQLReplication.SlaveStatus getMySQLSlaveStatus(
        String failoverRoot,
        int nestedOperatingSystemVersion,
        NetPort port
    ) throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream daemonOut=conn.getOutputStream();
            daemonOut.writeCompressedInt(AOServDaemonProtocol.GET_MYSQL_SLAVE_STATUS);
            daemonOut.writeUTF(failoverRoot);
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
            }
            if(code==AOServDaemonProtocol.DONE) return null;
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public List<MySQLDatabase.TableStatus> getMySQLTableStatus(
        String failoverRoot,
        int nestedOperatingSystemVersion,
        NetPort port,
        String databaseName
    ) throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream daemonOut=conn.getOutputStream();
            daemonOut.writeCompressedInt(AOServDaemonProtocol.GET_MYSQL_TABLE_STATUS);
            daemonOut.writeUTF(failoverRoot);
            daemonOut.writeCompressedInt(nestedOperatingSystemVersion);
            daemonOut.writeCompressedInt(port.getPort());
            daemonOut.writeUTF(databaseName);
            daemonOut.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.NEXT) {
                int size = in.readCompressedInt();
                List<MySQLDatabase.TableStatus> tableStatuses = new ArrayList<MySQLDatabase.TableStatus>(size);
                for(int c=0;c<size;c++) {
                    tableStatuses.add(
                        new MySQLDatabase.TableStatus(
                            in.readUTF(), // name
                            in.readNullEnum(MySQLDatabase.Engine.class), // engine
                            in.readNullInteger(), // version
                            in.readNullEnum(MySQLDatabase.TableStatus.RowFormat.class), // rowFormat
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
                            in.readNullEnum(MySQLDatabase.TableStatus.Collation.class), // collation
                            in.readNullUTF(), // checksum
                            in.readNullUTF(), // createOptions
                            in.readNullUTF() // comment
                        )
                    );
                }
                return tableStatuses;
            }
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public List<MySQLDatabase.CheckTableResult> checkMySQLTables(
        String failoverRoot,
        int nestedOperatingSystemVersion,
        NetPort port,
        String databaseName,
        List<String> tableNames
    ) throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream daemonOut=conn.getOutputStream();
            daemonOut.writeCompressedInt(AOServDaemonProtocol.CHECK_MYSQL_TABLES);
            daemonOut.writeUTF(failoverRoot);
            daemonOut.writeCompressedInt(nestedOperatingSystemVersion);
            daemonOut.writeCompressedInt(port.getPort());
            daemonOut.writeUTF(databaseName);
            int numTables = tableNames.size();
            daemonOut.writeCompressedInt(numTables);
            for(int c=0;c<numTables;c++) daemonOut.writeUTF(tableNames.get(c));
            daemonOut.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.NEXT) {
                int size = in.readCompressedInt();
                List<MySQLDatabase.CheckTableResult> checkTableResults = new ArrayList<MySQLDatabase.CheckTableResult>(size);
                for(int c=0;c<size;c++) {
                    checkTableResults.add(
                        new MySQLDatabase.CheckTableResult(
                            MySQLTableName.valueOf(in.readUTF()), // table
                            in.readLong(), // duration
                            in.readNullEnum(MySQLDatabase.CheckTableResult.MsgType.class), // msgType
                            in.readNullUTF() // msgText
                        )
                    );
                }
                return checkTableResults;
            }
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } catch(ValidationException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public void getAWStatsFile(String siteName, String path, String queryString, CompressedDataOutputStream out) throws RemoteException {
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
                    out.writeByte(AOServDaemonProtocol.NEXT);
                    out.writeShort(len);
                    out.write(buff, 0, len);
                }
                if(code!=AOServDaemonProtocol.DONE) {
                    if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
                    throw new RemoteException("Unknown result: " + code);
                }
            } finally {
                BufferManager.release(buff);
            }
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Compares to the password list on the server.
     */
    public boolean compareLinuxAccountPassword(String username, String password) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.COMPARE_LINUX_ACCOUNT_PASSWORD);
            out.writeUTF(username);
            out.writeUTF(password);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if(result==AOServDaemonProtocol.DONE) return in.readBoolean();
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets the encrypted password for a MySQL user as found in user table.
     */
    public String getEncryptedMySQLUserPassword(int mysqlServer, String username) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_ENCRYPTED_MYSQL_USER_PASSWORD);
            out.writeCompressedInt(mysqlServer);
            out.writeUTF(username);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if(result==AOServDaemonProtocol.DONE) return in.readUTF();
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets the hostname that is connected to.
     */
    public Hostname getHostname() {
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
    public NetPort getPort() {
        return port;
    }

    /**
     * Gets the password for a PostgreSQL user as found in pg_shadow or pg_authid table.
     */
    public String getPostgresUserPassword(int pkey) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_POSTGRES_PASSWORD);
            out.writeCompressedInt(pkey);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if(result==AOServDaemonProtocol.DONE) return in.readUTF();
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
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
    ) throws RemoteException {
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
                if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
                throw new RemoteException("Unknown result: " + code);
            }
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /*public void initializeHttpdSitePasswdFile(int sitePKey, String username, String encPassword) {
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
            if(result==AOServDaemonProtocol.DONE) return;
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } finally {
            releaseConnection(conn);
        }
    }*/

    public void printConnectionStatsHTML(Appendable out) throws RemoteException {
        try {
            pool.printConnectionStats(out);
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    /**
     * Releases a connection to the server.  This will allow another thread
     * to use the connection.  Connections may be of any protocol, so each
     * connection must be released after every transaction.
     */
    public void releaseConnection(AOServDaemonConnection connection) throws RemoteException {
        try {
            pool.releaseConnection(connection);
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    /**
     * Deletes the contents of an email list
     */
    public void removeEmailList(String listPath) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.REMOVE_EMAIL_LIST);
            out.writeUTF(listPath);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if(result==AOServDaemonProtocol.DONE) return;
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Controls a process.
     */
    private void controlProcess(int command) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(command);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if(result==AOServDaemonProtocol.DONE) return;
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Controls a process.
     */
    private void controlProcess(int command, int param1) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(command);
            out.writeCompressedInt(param1);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if(result==AOServDaemonProtocol.DONE) return;
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public void restartApache() throws RemoteException {
        controlProcess(AOServDaemonProtocol.RESTART_APACHE);
    }

    public void restartCron() throws RemoteException {
        controlProcess(AOServDaemonProtocol.RESTART_CRON);
    }

    public void restartMySQL(int mysqlServer) throws RemoteException {
        controlProcess(AOServDaemonProtocol.RESTART_MYSQL, mysqlServer);
    }

    public void restartPostgres(int pkey) throws RemoteException {
        controlProcess(AOServDaemonProtocol.RESTART_POSTGRES, pkey);
    }

    public void restartXfs() throws RemoteException {
        controlProcess(AOServDaemonProtocol.RESTART_XFS);
    }

    public void restartXvfb() throws RemoteException {
        controlProcess(AOServDaemonProtocol.RESTART_XVFB);
    }

    public void setAutoresponderContent(String path, String content, int uid, int gid) throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
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
    public void setCronTable(String username, String cronTable) throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Sets the file used by an email list.
     */
    public void setEmailListFile(String path, String file, int uid, int gid, int mode) throws RemoteException {
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
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Sets the encrypted password for a Linux account.
     */
    public void setEncryptedLinuxAccountPassword(String username, String encryptedPassword) throws RemoteException {
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
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Sets the password for a <code>LinuxServerAccount</code>.
     */
    public void setLinuxServerAccountPassword(String username, String plain_password) throws RemoteException {
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
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
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
    public void setImapFolderSubscribed(String username, String folderName, boolean subscribed) throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Sets the password for a <code>MySQLUser</code>.
     */
    public void setMySQLUserPassword(int mysqlServer, String username, String password) throws RemoteException {
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
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Sets the password for a <code>PostgresServerUser</code>.
     */
    public void setPostgresUserPassword(int pkey, String password) throws RemoteException {
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
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public void startApache() throws RemoteException {
        controlProcess(AOServDaemonProtocol.START_APACHE);
    }

    public void startCron() throws RemoteException {
        controlProcess(AOServDaemonProtocol.START_CRON);
    }

    /**
     * Starts a distribution verification.
     */
    public void startDistro(boolean includeUser) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.START_DISTRO);
            out.writeBoolean(includeUser);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if (result == AOServDaemonProtocol.DONE) return;
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Starts a Java VM.
     */
    public String startJVM(int httpdSite) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.START_JVM);
            out.writeCompressedInt(httpdSite);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if (result == AOServDaemonProtocol.DONE) return in.readBoolean()?in.readUTF():null;
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public void startMySQL() throws RemoteException {
        controlProcess(AOServDaemonProtocol.START_MYSQL);
    }

    public void startPostgreSQL(int pkey) throws RemoteException {
        controlProcess(AOServDaemonProtocol.START_POSTGRESQL, pkey);
    }

    public void startXfs() throws RemoteException {
        controlProcess(AOServDaemonProtocol.START_XFS);
    }

    public void startXvfb() throws RemoteException {
        controlProcess(AOServDaemonProtocol.START_XVFB);
    }

    public void stopApache() throws RemoteException {
        controlProcess(AOServDaemonProtocol.STOP_APACHE);
    }

    public void stopCron() throws RemoteException {
        controlProcess(AOServDaemonProtocol.STOP_CRON);
    }

    /**
     * Stops a Java VM.
     */
    public String stopJVM(int httpdSite) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.STOP_JVM);
            out.writeCompressedInt(httpdSite);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if (result == AOServDaemonProtocol.DONE) return in.readBoolean()?in.readUTF():null;
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public void stopMySQL() throws RemoteException {
        controlProcess(AOServDaemonProtocol.STOP_MYSQL);
    }

    public void stopPostgreSQL(int pkey) throws RemoteException {
        controlProcess(AOServDaemonProtocol.STOP_POSTGRESQL, pkey);
    }

    public void stopXfs() throws RemoteException {
        controlProcess(AOServDaemonProtocol.STOP_XFS);
    }

    public void stopXvfb() throws RemoteException {
        controlProcess(AOServDaemonProtocol.STOP_XVFB);
    }

    @Override
    final public String toString() {
        return getClass().getName()+"?hostname="+hostname+"&local_ip="+local_ip+"&port="+port+"&protocol="+protocol;
    }

    private void transferStream(
        int command,
        int param1,
        CompressedDataOutputStream masterOut
    ) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(command);
            out.writeCompressedInt(param1);
            out.flush();

            transferStream0(conn, masterOut);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /*
    private void transferStream(
        int command,
        String param1,
        CompressedDataOutputStream masterOut
    ) {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(command);
            out.writeUTF(param1);
            out.flush();

            transferStream0(conn, masterOut);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }
    */
    private void transferStream(
        int command,
        String param1,
        CompressedDataOutputStream masterOut,
        long skipBytes
    ) throws RemoteException {
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
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    private void transferStream0(
        AOServDaemonConnection conn,
        CompressedDataOutputStream masterOut
    ) throws RemoteException {
        try {
            CompressedDataInputStream in=conn.getInputStream();
            int code;
            byte[] buff=BufferManager.getBytes();
            try {
                while((code=in.read())==AOServDaemonProtocol.NEXT) {
                    int len=in.readShort();
                    in.readFully(buff, 0, len);
                    masterOut.writeByte(AOServDaemonProtocol.NEXT);
                    masterOut.writeShort(len);
                    masterOut.write(buff, 0, len);
                    //if(reporter!=null) reporter.addFinishedSize(len);
                }
            } finally {
                BufferManager.release(buff);
            }
            if (code == AOServDaemonProtocol.DONE) return;
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }

    private void waitFor(ServiceName serviceName) throws RemoteException {
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.WAIT_FOR_REBUILD);
            out.writeUTF(serviceName.name());
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int result = in.read();
            if (result == AOServDaemonProtocol.DONE) return;
            if(result==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + result);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    public void waitForHttpdSiteRebuild() throws RemoteException {
        waitFor(ServiceName.httpd_sites);
    }

    public void waitForLinuxAccountRebuild() throws RemoteException {
        waitFor(ServiceName.linux_accounts);
    }

    public void waitForMySQLDatabaseRebuild() throws RemoteException {
        waitFor(ServiceName.mysql_databases);
    }

    public void waitForMySQLDBUserRebuild() throws RemoteException {
        waitFor(ServiceName.mysql_db_users);
    }

    public void waitForMySQLUserRebuild() throws RemoteException {
        waitFor(ServiceName.mysql_users);
    }

    public void waitForPostgresDatabaseRebuild() throws RemoteException {
        waitFor(ServiceName.postgres_databases);
    }

    public void waitForPostgresServerRebuild() throws RemoteException {
        waitFor(ServiceName.postgres_servers);
    }

    public void waitForPostgresUserRebuild() throws RemoteException {
        waitFor(ServiceName.postgres_users);
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
    public String get3wareRaidReport() throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_3WARE_RAID_REPORT);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.DONE) return in.readUTF();
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets a MD RAID report.
     *
     * @return  the report
     */
    public String getMdRaidReport() throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_MD_RAID_REPORT);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.DONE) return in.readUTF();
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets a DRBD report.
     *
     * @return  the report
     */
    public String getDrbdReport() throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_DRBD_REPORT);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.DONE) return in.readUTF();
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets a LVM report.
     *
     * @return  the report
     */
    public String[] getLvmReport() throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets a hard drive temperature report.
     *
     * @return  the report
     */
    public String getHddTempReport() throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_HDD_TEMP_REPORT);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.DONE) return in.readUTF();
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets a hard drive model report.
     *
     * @return  the report
     */
    public String getHddModelReport() throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_HDD_MODEL_REPORT);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.DONE) return in.readUTF();
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets a filesystems CSV report.
     *
     * @return  the report
     */
    public String getFilesystemsCsvReport() throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_FILESYSTEMS_CSV_REPORT);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.DONE) return in.readUTF();
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }
 
    /**
     * Gets a load average report.
     *
     * @return  the report
     */
    public String getLoadAvgReport() throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_AO_SERVER_LOADAVG_REPORT);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.DONE) return in.readUTF();
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets a meminfo report.
     *
     * @return  the report
     */
    public String getMemInfoReport() throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_AO_SERVER_MEMINFO_REPORT);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.DONE) return in.readUTF();
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Checks a port from the server point of view.
     *
     * @return  the result
     */
    public String checkPort(InetAddress ipAddress, NetPort port, String netProtocol, String appProtocol, String monitoringParameters) throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.CHECK_PORT);
            out.writeLong(ipAddress.getIp().getHigh());
            out.writeLong(ipAddress.getIp().getLow());
            out.writeCompressedInt(port.getPort());
            out.writeUTF(netProtocol);
            out.writeUTF(appProtocol);
            out.writeUTF(monitoringParameters);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.DONE) return in.readUTF();
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Checks for a SMTP blacklist from the server point of view.
     *
     * @return  the status line
     */
    public String checkSmtpBlacklist(String sourceIp, String connectIp) throws RemoteException {
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
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }

    /**
     * Gets the current system time.
     *
     * @return  the report
     */
    public long getSystemTimeMillis() throws RemoteException {
        // Establish the connection to the server
        AOServDaemonConnection conn=getConnection();
        try {
            CompressedDataOutputStream out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.GET_AO_SERVER_SYSTEM_TIME_MILLIS);
            out.flush();

            CompressedDataInputStream in=conn.getInputStream();
            int code=in.read();
            if(code==AOServDaemonProtocol.DONE) return in.readLong();
            if(code==AOServDaemonProtocol.REMOTE_EXCEPTION) throw new RemoteException(in.readUTF());
            throw new RemoteException("Unknown result: " + code);
        } catch(RuntimeException err) {
            conn.close();
            throw err;
        } catch(RemoteException err) {
            conn.close();
            throw err;
        } catch(IOException err) {
            conn.close();
            throw new RemoteException(err.getMessage(), err);
        } finally {
            releaseConnection(conn);
        }
    }
}
