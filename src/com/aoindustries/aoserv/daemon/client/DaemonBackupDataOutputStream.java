package com.aoindustries.aoserv.daemon.client;

/*
 * Copyright 2002-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.io.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * @see  BackupDataTable#sendData
 *
 * @version  1.0a
 *
 * @author  AO Industries, Inc.
 */
final public class DaemonBackupDataOutputStream extends OutputStream {

    private AOServDaemonConnector connector;
    private AOServDaemonConnection conn;
    private boolean closed=false;
    private CompressedDataOutputStream out;

    public DaemonBackupDataOutputStream(
        AOServDaemonConnector connector,
        long daemonKey,
        String filename,
        boolean isCompressed
    ) throws IOException {
        this.connector=connector;
        conn=connector.getConnection();
        boolean initDone=false;
        try {
            out=conn.getOutputStream();
            out.writeCompressedInt(AOServDaemonProtocol.STORE_BACKUP_DATA_DIRECT_ACCESS);
            out.writeLong(daemonKey);
            out.writeUTF(filename);
            out.writeBoolean(isCompressed);
            initDone=true;
        } catch(IOException err) {
            conn.close();
            throw err;
        } finally {
            if(!initDone) {
                connector.releaseConnection(conn);
                closed=true;
            }
        }
    }
    
    synchronized public void close() throws IOException {
        if(!closed) {
            try {
                out.write(AOServDaemonProtocol.DONE);
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
            } catch(SQLException err) {
                conn.close();
                IOException ioErr=new IOException("Error closing DaemonBackupDataOutputStream");
                ioErr.initCause(err);
                throw ioErr;
            } finally {
                connector.releaseConnection(conn);
                closed=true;
            }
        }
    }

    synchronized public void flush() throws IOException {
        try {
            out.flush();
        } catch(IOException err) {
            throw err;
        }
    }

    synchronized public void write(byte[] buff, int off, int len) throws IOException {
        try {
            while(len>0) {
                int blockLen=len;
                if(blockLen>BufferManager.BUFFER_SIZE) blockLen=BufferManager.BUFFER_SIZE;
                out.write(AOServDaemonProtocol.NEXT);
                out.writeShort(blockLen);
                out.write(buff, off, blockLen);
                off+=blockLen;
                len-=blockLen;
            }
        } catch(IOException err) {
            throw err;
        }
    }
    
    synchronized public void write(int b) throws IOException {
        try {
            out.write(AOServDaemonProtocol.NEXT);
            out.writeShort(1);
            out.write(b);
        } catch(IOException err) {
            throw err;
        }
    }
}
