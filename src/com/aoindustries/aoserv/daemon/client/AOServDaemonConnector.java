package com.aoindustries.aoserv.daemon.client;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 2200 Dogwood Ct N, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.daemon.client.AOServDaemonProtocol;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

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
     * The server this connection is to.
     */
    final int aoServer;

    /**
     * The hostname to connect to.
     */
    final String hostname;
    
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

    final Object sslLock;
    
    final boolean[] sslProviderLoaded;
    
    final String trustStore;
    
    final String trustStorePassword;

    private final AOServDaemonConnectionPool pool;
    
    /**
     * Creates a new <code>AOServConnector</code>.
     */
    private AOServDaemonConnector(
        int aoServer,
        String hostname,
        int port,
        String protocol,
        String key,
        int poolSize,
        long maxConnectionAge,
        Object sslLock,
        boolean[] sslProviderLoaded,
        String trustStore,
        String trustStorePassword,
        ErrorHandler errorHandler
    ) throws IOException {
        Profiler.startProfile(Profiler.FAST, AOServDaemonConnector.class, "<init>(int,String,int,String,String,int,long,Object,boolean[],String,String,ErrorHandler)", null);
        try {
            this.aoServer=aoServer;
            this.hostname=hostname;
            this.port=port;
            this.protocol=protocol;
            this.key=key;
            this.poolSize=poolSize;
            this.maxConnectionAge=maxConnectionAge;
            this.sslLock=sslLock;
            this.sslProviderLoaded=sslProviderLoaded;
            this.trustStore=trustStore;
            this.trustStorePassword=trustStorePassword;
            this.pool=new AOServDaemonConnectionPool(this, errorHandler);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
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
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "copyHomeDirectory(String,AOServDaemonConnector)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public void dumpInterBaseDatabase(int pkey, CompressedDataOutputStream masterOut) throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "dumpInterBaseDatabase(int,CompressedDataOutputStream)", null);
        try {
            transferStream(AOServDaemonProtocol.DUMP_INTERBASE_DATABASE, pkey, masterOut, null);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void dumpMySQLDatabase(int pkey, CompressedDataOutputStream masterOut) throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "dumpMySQLDatabase(int,CompressedDataOutputStream)", null);
        try {
            transferStream(AOServDaemonProtocol.DUMP_MYSQL_DATABASE, pkey, masterOut, null);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void dumpPostgresDatabase(int pkey, CompressedDataOutputStream masterOut) throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "dumpPostgresDatabase(int,CompressedDataOutputStream)", null);
        try {
            transferStream(AOServDaemonProtocol.DUMP_POSTGRES_DATABASE, pkey, masterOut, null);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public String getAutoresponderContent(String path) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getAutoresponderContent(String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public void getBackupData(
        String path,
        CompressedDataOutputStream masterOut,
        long skipBytes,
        GetBackupDataReporter reporter) throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getBackupData(String,CompressedDataOutputStream,long,GetBackupDataReporter)", null);
        try {
            transferStream(AOServDaemonProtocol.GET_BACKUP_DATA, path, masterOut, skipBytes, reporter);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public int getConcurrency() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getConcurrency()", null);
        try {
            return pool.getConcurrency();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Allocates a connection to the server.  These connections must later be
     * released with the <code>releaseConnection</code> method.  Connection
     * pooling is obtained this way.  These connections may be over any protocol,
     * so they may only be used for one client/server exchange at a time.
     */
    public AOServDaemonConnection getConnection() throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getConnection()", null);
        try {
            try {
                return pool.getConnection();
            } catch(IOException err) {
                IOException newErr = new IOException("IOException while trying to get a connection to server #"+aoServer+": "+hostname+":"+port);
                newErr.initCause(err);
                throw newErr;
            }
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Allocates a connection to the server.  These connections must later be
     * released with the <code>releaseConnection</code> method.  Connection
     * pooling is obtained this way.  These connections may be over any protocol,
     * so they may only be used for one client/server exchange at a time.
     */
    public AOServDaemonConnection getConnection(int maxConnections) throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getConnection(int)", null);
        try {
            return pool.getConnection(maxConnections);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public int getConnectionCount() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getConnectionCount()", null);
        try {
            return pool.getConnectionCount();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Gets the default <code>AOServConnector</code> as defined in the
     * <code>client.properties</code> resource.  Each possible
     * protocol is tried, in order, until a successful connection is
     * made.  If no connection is made, an <code>IOException</code>
     * is thrown.
     */
    public synchronized static AOServDaemonConnector getConnector(
        int aoServer,
        String hostname,
        int port,
        String protocol,
        String key,
        int poolSize,
        long maxConnectionAge,
        Object sslLock,
        boolean[] sslProviderLoaded,
        String trustStore,
        String trustStorePassword,
        ErrorHandler errorHandler
    ) throws IOException {
        Profiler.startProfile(Profiler.FAST, AOServDaemonConnector.class, "getConnector(int,String,int,String,String,int,long,Object,boolean[],String,String,ErrorHandler)", null);
        try {
            if(hostname==null) throw new NullPointerException("hostname is null");
            if(protocol==null) throw new NullPointerException("protocol is null");

            int size=connectors.size();
            for(int c=0;c<size;c++) {
                AOServDaemonConnector connector=connectors.get(c);
                if(
                    connector.aoServer==aoServer
                    && connector.hostname.equals(hostname)
                    && connector.port==port
                    && connector.protocol.equals(protocol)
                    && (key==null?connector.key==null:key.equals(connector.key))
                    && connector.poolSize==poolSize
                    && connector.maxConnectionAge==maxConnectionAge
                ) return connector;
            }
            AOServDaemonConnector connector=new AOServDaemonConnector(
                aoServer,
                hostname,
                port,
                protocol,
                key,
                poolSize,
                maxConnectionAge,
                sslLock,
                sslProviderLoaded,
                trustStore,
                trustStorePassword,
                errorHandler
            );
            connectors.add(connector);
            return connector;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public long getConnects() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getConnects()", null);
        try {
            return pool.getConnects();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Gets a cron table.
     *
     * @param  username  the username to copy the home directory of
     *
     * @return  the cron table
     */
    public String getCronTable(String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getCronTable(String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Determines if the inbox is in manual procmail mode.
     *
     * @param  lsa  the pkey of the LinuxServerAccount
     */
    public boolean isProcmailManual(int lsa) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "isProcmailManual(int)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Gets the profiling information for the daemon
     */
    public void getDaemonProfile(List<DaemonProfile> objs) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getDaemonProfile(List<DaemonProfile>)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Gets the total size of a mounted filesystem in bytes.
     */
    public long getDiskDeviceTotalSize(String device) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getDiskDeviceTotalSize(String)", null);
        try {
            AOServDaemonConnection conn=getConnection(2);
            try {
                CompressedDataOutputStream out=conn.getOutputStream();
                out.writeCompressedInt(AOServDaemonProtocol.GET_DISK_DEVICE_TOTAL_SIZE);
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Gets the used size of a mounted filesystem in bytes.
     */
    public long getDiskDeviceUsedSize(String device) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getDiskDeviceUsedSize(String)", null);
        try {
            AOServDaemonConnection conn=getConnection(2);
            try {
                CompressedDataOutputStream out=conn.getOutputStream();
                out.writeCompressedInt(AOServDaemonProtocol.GET_DISK_DEVICE_USED_SIZE);
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Gets the file used by an email list.
     */
    public String getEmailListFile(String path) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getEmailListFile(String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Gets the encrypted password for a linux account as found in the /etc/shadow file.
     */
    public String getEncryptedLinuxAccountPassword(String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getEncryptedLinuxAccountPassword(String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public long[] getImapFolderSizes(String username, String[] folderNames) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getImapFolderSizes(String,String[])", null);
        try {
            // Establish the connection to the server
            AOServDaemonConnection conn=getConnection();
            try {
                CompressedDataOutputStream out=conn.getOutputStream();
                out.writeCompressedInt(AOServDaemonProtocol.GET_IMAP_FOLDER_SIZES);
                out.writeUTF(username);
                out.writeCompressedInt(folderNames.length);
                for(int c=0;c<folderNames.length;c++) out.writeUTF(folderNames[c]);
                out.flush();
                
                CompressedDataInputStream in=conn.getInputStream();
                int code=in.read();
                if(code==AOServDaemonProtocol.DONE) {
                    long[] sizes=new long[folderNames.length];
                    for(int c=0;c<folderNames.length;c++) sizes[c]=in.readLong();
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public InboxAttributes getInboxAttributes(String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getInboxAttributes(String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public void getMrtgFile(String filename, CompressedDataOutputStream out) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getMrtgFile(String,CompressedDataOutputStream)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public void getAWStatsFile(String siteName, String path, String queryString, CompressedDataOutputStream out) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getAWStatsFile(String,String,String,CompressedDataOutputStream)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Compares to the password list on the server.
     */
    public boolean compareLinuxAccountPassword(String username, String password) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "compareLinuxAccountPassword(String,String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Gets the encrypted password for an InterBase user as found in user table.
     */
    public String getEncryptedInterBaseUserPassword(String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getEncryptedInterBaseUserPassword(String)", null);
        try {
            AOServDaemonConnection conn=getConnection();
            try {
                CompressedDataOutputStream out=conn.getOutputStream();
                out.writeCompressedInt(AOServDaemonProtocol.GET_ENCRYPTED_INTERBASE_USER_PASSWORD);
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Gets the encrypted password for a MySQL user as found in user table.
     */
    public String getEncryptedMySQLUserPassword(int mysqlServer, String username) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getEncryptedMySQLUserPassword(int,String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Gets the hostname that is connected to.
     */
    public String getHostname() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getHostname()", null);
        try {
            return hostname;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public int getMaxConcurrency() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getMaxConcurrency()", null);
        try {
            return pool.getMaxConcurrency();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public long getMaxConnectionAge() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getMaxConnectionAge()", null);
        try {
            return pool.getMaxConnectionAge();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public int getPoolSize() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getPoolSize()", null);
        try {
            return pool.getPoolSize();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Gets the port that is connected to.
     */
    public int getPort() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getPort()", null);
        try {
            return port;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Gets the password for a PostgreSQL user as found in pg_shadow or pg_authid table.
     */
    public String getPostgresUserPassword(int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "getPostgresUserPassword(int)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public long getTotalTime() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getTotalTime()", null);
        try {
            return pool.getTotalTime();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public long getTransactionCount() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getTransactionCount()", null);
        try {
            return pool.getTransactionCount();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void grantDaemonAccess(
        long key,
        int command,
        String param1,
        String param2,
        String param3
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "grantDaemonAccess(long,int,String,String,String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /*public void initializeHttpdSitePasswdFile(int sitePKey, String username, String encPassword) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "initializeHttpdSitePasswdFile(int,String,String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }*/

    public void printConnectionStatsHTML(ChainWriter out) throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "printConnectionStatsHTML(ChainWriter)", null);
        try {
            pool.printConnectionStats(out);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Releases a connection to the server.  This will allow another thread
     * to use the connection.  Connections may be of any protocol, so each
     * connection must be released after every transaction.
     */
    public void releaseConnection(AOServDaemonConnection connection) throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "releaseConnection(AOServDaemonConnection)", null);
        try {
            pool.releaseConnection(connection);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void removeBackupData(int backupPartition, String relativePath) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "removeBackupData(int,String)", null);
        try {
            AOServDaemonConnection conn=getConnection();
            try {
                CompressedDataOutputStream out=conn.getOutputStream();
                out.writeCompressedInt(AOServDaemonProtocol.REMOVE_BACKUP_DATA);
                out.writeCompressedInt(backupPartition);
                out.writeUTF(relativePath);
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Deletes the contents of an email list
     */
    public void removeEmailList(String listPath) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "removeEmailList(String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Controls a process.
     */
    private void controlProcess(int command) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "controlProcess(int)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Controls a process.
     */
    private void controlProcess(int command, int param1) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "controlProcess(int,int)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public void restartApache() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "restartApache()", null);
        try {
            controlProcess(AOServDaemonProtocol.RESTART_APACHE);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void restartCron() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "restartCron()", null);
        try {
            controlProcess(AOServDaemonProtocol.RESTART_CRON);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void restartInterBase() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "restartInterBase()", null);
        try {
            controlProcess(AOServDaemonProtocol.RESTART_INTERBASE);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void restartMySQL(int mysqlServer) throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "restartMySQL(int)", null);
        try {
            controlProcess(AOServDaemonProtocol.RESTART_MYSQL, mysqlServer);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void restartPostgres(int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "restartPostgres(int)", null);
        try {
            controlProcess(AOServDaemonProtocol.RESTART_POSTGRES, pkey);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void restartXfs() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "restartXfs()", null);
        try {
            controlProcess(AOServDaemonProtocol.RESTART_XFS);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void restartXvfb() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "restartXvfb()", null);
        try {
            controlProcess(AOServDaemonProtocol.RESTART_XVFB);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void setAutoresponderContent(String path, String content, int uid, int gid) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "setAutoresponderContent(String,String,int,int)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Sets a cron table.
     *
     * @param  username  the username to copy the home directory of
     * @param  cronTable  the new cron table
     */
    public void setCronTable(String username, String cronTable) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "setCronTable(String,String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Sets the file used by an email list.
     */
    public void setEmailListFile(String path, String file, int uid, int gid, int mode) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "setEmailListFile(String,String,int,int,int)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Sets the encrypted password for a Linux account.
     */
    public void setEncryptedLinuxAccountPassword(String username, String encryptedPassword) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "setEncryptedLinuxAccountPassword(String,String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Sets the password for a <code>LinuxServerAccount</code>.
     */
    public void setLinuxServerAccountPassword(String username, String plain_password) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "setLinuxServerAccountPassword(String,String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
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
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "setImapFolderSubscribed(String,String,boolean)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Sets the password for an <code>InterBaseServerUser</code>.
     */
    public void setInterBaseUserPassword(String username, String password) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "setInterBaseUserPassword(String,String)", null);
        try {
            AOServDaemonConnection conn=getConnection();
            try {
                CompressedDataOutputStream out=conn.getOutputStream();
                out.writeCompressedInt(AOServDaemonProtocol.SET_INTERBASE_USER_PASSWORD);
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Sets the password for a <code>MySQLServerUser</code>.
     */
    public void setMySQLUserPassword(int mysqlServer, String username, String password) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "setMySQLUserPassword(int,String,String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    /**
     * Sets the password for a <code>PostgresServerUser</code>.
     */
    public void setPostgresUserPassword(int pkey, String password) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "setPostgresUserPassword(int,String)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public void startApache() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "startApache()", null);
        try {
            controlProcess(AOServDaemonProtocol.START_APACHE);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void startCron() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "startCron()", null);
        try {
            controlProcess(AOServDaemonProtocol.START_CRON);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Starts a distribution verification.
     */
    public void startDistro(boolean includeUser) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "startDistro(boolean)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public void startInterBase() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "startInterBase()", null);
        try {
            controlProcess(AOServDaemonProtocol.START_INTERBASE);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Starts a Java VM.
     */
    public String startJVM(int httpdSite) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "startJVM(int)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public void startMySQL() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "startMySQL()", null);
        try {
            controlProcess(AOServDaemonProtocol.START_MYSQL);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void startPostgreSQL(int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "startPostgres(pkey)", null);
        try {
            controlProcess(AOServDaemonProtocol.START_POSTGRESQL, pkey);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void startXfs() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "startXfs()", null);
        try {
            controlProcess(AOServDaemonProtocol.START_XFS);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void startXvfb() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "startXvfb()", null);
        try {
            controlProcess(AOServDaemonProtocol.START_XVFB);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void stopApache() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "stopApache()", null);
        try {
            controlProcess(AOServDaemonProtocol.STOP_APACHE);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void stopCron() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "stopCron()", null);
        try {
            controlProcess(AOServDaemonProtocol.STOP_CRON);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void stopInterBase() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "stopInterBase()", null);
        try {
            controlProcess(AOServDaemonProtocol.STOP_INTERBASE);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Stops a Java VM.
     */
    public String stopJVM(int httpdSite) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "stopJVM(int)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public void stopMySQL() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "stopMySQL()", null);
        try {
            controlProcess(AOServDaemonProtocol.STOP_MYSQL);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void stopPostgreSQL(int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "stopPostgreSQL(int)", null);
        try {
            controlProcess(AOServDaemonProtocol.STOP_POSTGRESQL, pkey);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void stopXfs() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "stopXfs()", null);
        try {
            controlProcess(AOServDaemonProtocol.STOP_XFS);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void stopXvfb() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "stopXvfb()", null);
        try {
            controlProcess(AOServDaemonProtocol.STOP_XVFB);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    final public String toString() {
        Profiler.startProfile(Profiler.FAST, AOServDaemonConnector.class, "toString()", null);
        try {
            return getClass().getName()+"?hostname="+hostname+"&port="+port+"&protocol="+protocol;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private void transferStream(
        int command,
        int param1,
        CompressedDataOutputStream masterOut,
        GetBackupDataReporter reporter
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "transferStream(int,int,CompressedDataOutputStream,GetBackupDataReporter)", null);
        try {
            AOServDaemonConnection conn=getConnection();
            try {
                CompressedDataOutputStream out=conn.getOutputStream();
                out.writeCompressedInt(command);
                out.writeCompressedInt(param1);
                out.flush();
                
                transferStream0(conn, masterOut, reporter);
            } catch(IOException err) {
                conn.close();
                throw err;
            } finally {
                releaseConnection(conn);
            }
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    private void transferStream(
        int command,
        String param1,
        CompressedDataOutputStream masterOut,
        GetBackupDataReporter reporter
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "transferStream(int,String,CompressedDataOutputStream,GetBackupDataReporter)", null);
        try {
            AOServDaemonConnection conn=getConnection();
            try {
                CompressedDataOutputStream out=conn.getOutputStream();
                out.writeCompressedInt(command);
                out.writeUTF(param1);
                out.flush();
                
                transferStream0(conn, masterOut, reporter);
            } catch(IOException err) {
                conn.close();
                throw err;
            } finally {
                releaseConnection(conn);
            }
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    private void transferStream(
        int command,
        String param1,
        CompressedDataOutputStream masterOut,
        long skipBytes,
        GetBackupDataReporter reporter
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "transferStream(int,String,CompressedDataOutputStream,long,GetBackupDataReporter)", null);
        try {
            AOServDaemonConnection conn=getConnection();
            try {
                CompressedDataOutputStream out=conn.getOutputStream();
                out.writeCompressedInt(command);
                out.writeUTF(param1);
                out.writeLong(skipBytes);
                out.flush();
                
                if(reporter!=null) {
                    long fileSize=conn.getInputStream().readLong();
                    reporter.setTotalSize(fileSize);
                    reporter.setFinishedSize(skipBytes);
                }
                transferStream0(conn, masterOut, reporter);
            } catch(IOException err) {
                conn.close();
                throw err;
            } finally {
                releaseConnection(conn);
            }
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    private void transferStream0(
        AOServDaemonConnection conn,
        CompressedDataOutputStream masterOut,
        GetBackupDataReporter reporter
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "transferStream0(AOServDaemonConnection,CompressedDataOutputStream,GetBackupDataReporter)", null);
        try {
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
                    if(reporter!=null) reporter.addFinishedSize(len);
                }
            } finally {
                BufferManager.release(buff);
            }
            if (code == AOServDaemonProtocol.DONE) return;
            else if (code == AOServDaemonProtocol.IO_EXCEPTION) throw new IOException(in.readUTF());
            else if (code == AOServDaemonProtocol.SQL_EXCEPTION) throw new SQLException(in.readUTF());
            else throw new IOException("Unknown result: " + code);
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    private void waitFor(int tableID) throws IOException, SQLException {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnector.class, "waitFor(int)", null);
        try {
            AOServDaemonConnection conn=getConnection();
            try {
                CompressedDataOutputStream out=conn.getOutputStream();
                out.writeCompressedInt(AOServDaemonProtocol.WAIT_FOR_REBUILD);
                out.writeCompressedInt(tableID);
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
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    public void waitForHttpdSiteRebuild() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "waitForHttpdSiteRebuild()", null);
        try {
            waitFor(SchemaTable.HTTPD_SITES);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void waitForInterBaseRebuild() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "waitForInterBaseRebuild()", null);
        try {
            waitFor(SchemaTable.INTERBASE_USERS);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void waitForLinuxAccountRebuild() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "waitForLinuxAccountRebuild()", null);
        try {
            waitFor(SchemaTable.LINUX_ACCOUNTS);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void waitForMySQLDatabaseRebuild() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "waitForMySQLDatabaseRebuild()", null);
        try {
            waitFor(SchemaTable.MYSQL_DATABASES);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void waitForMySQLDBUserRebuild() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "waitForMySQLDBUserRebuild()", null);
        try {
            waitFor(SchemaTable.MYSQL_DB_USERS);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void waitForMySQLUserRebuild() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "waitForMySQLUserRebuild()", null);
        try {
            waitFor(SchemaTable.MYSQL_USERS);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void waitForPostgresDatabaseRebuild() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "waitForPostgresDatabaseRebuild()", null);
        try {
            waitFor(SchemaTable.POSTGRES_DATABASES);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void waitForPostgresServerRebuild() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "waitForPostgresServerRebuild()", null);
        try {
            waitFor(SchemaTable.POSTGRES_SERVERS);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void waitForPostgresUserRebuild() throws IOException, SQLException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "waitForPostgresUserRebuild()", null);
        try {
            waitFor(SchemaTable.POSTGRES_USERS);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    /**
     * Gets the error handler for this and its underlying connection pool.
     */
    public ErrorHandler getErrorHandler() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnector.class, "getErrorHandler()", null);
        try {
            return pool.getErrorHandler();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    public void sendData(
        long daemonKey,
        InputStream in,
        String filename,
        long length,
        boolean fileIsCompressed,
        boolean shouldBeCompressed,
        BitRateProvider bitRateProvider
    ) throws IOException, SQLException {
        long bytesRead=0;
	OutputStream sendOut=null;
        try {
            DaemonBackupDataOutputStream backupOut=new DaemonBackupDataOutputStream(
                this,
                daemonKey,
                filename,
                fileIsCompressed || shouldBeCompressed
            );
            if(bitRateProvider==null) {
                sendOut=
                    !fileIsCompressed && shouldBeCompressed
                    ?(OutputStream)new GZIPOutputStream(new BufferedOutputStream(backupOut))
                    :backupOut
                ;
            } else {
                sendOut=
                    !fileIsCompressed && shouldBeCompressed
                    ?(OutputStream)new GZIPOutputStream(new BufferedOutputStream(new BitRateOutputStream(backupOut, bitRateProvider)))
                    :new BitRateOutputStream(backupOut, bitRateProvider)
                ;
            }
	    try {
		// Should only read up to a maximum length of length, because log files grow between initial processing
		// and here.
		byte[] buff=BufferManager.getBytes();
		try {
		    while(bytesRead<length) {
			long bytesLeft=length-bytesRead;
			if(bytesLeft>BufferManager.BUFFER_SIZE) bytesLeft=BufferManager.BUFFER_SIZE;
			int ret=in.read(buff, 0, (int)bytesLeft);

                        // Unexpected end of file
			if(ret==-1) break;
			
			sendOut.write(buff, 0, ret);
			bytesRead+=ret;
		    }
		} finally {
		    BufferManager.release(buff);
		}
	    } finally {
		in.close();
	    }

	    // The length and MD5 are verified by the daemon as it receives the data.
	} finally {
	    if(sendOut!=null) {
		sendOut.flush();
		sendOut.close();
	    }
	}
    }
}