package com.aoindustries.aoserv.daemon.client;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
 * 2200 Dogwood Ct N, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.util.*;
import java.io.*;

/**
 * Connections made by <code>TCPConnector</code> or any
 * of its derivatives are pooled and reused.
 *
 * @author  AO Industries, Inc.
 */
final public class AOServDaemonConnectionPool extends AOPool {

    private final AOServDaemonConnector connector;

    /**
     * @deprecated
     */
    AOServDaemonConnectionPool(AOServDaemonConnector connector) {
        super(AOServDaemonConnectionPool.class.getName()+"?hostname=" + connector.hostname+"&port="+connector.port+"&protocol="+connector.protocol, connector.poolSize, connector.maxConnectionAge);
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnectionPool.class, "<init>(AOServDaemonConnector)", Integer.valueOf(connector.aoServer));
        try {
            this.connector=connector;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    AOServDaemonConnectionPool(AOServDaemonConnector connector, ErrorHandler errorHandler) {
        super(AOServDaemonConnectionPool.class.getName()+"?hostname=" + connector.hostname+"&port="+connector.port+"&protocol="+connector.protocol, connector.poolSize, connector.maxConnectionAge, errorHandler);
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnectionPool.class, "<init>(AOServDaemonConnector,ErrorHandler)", Integer.valueOf(connector.aoServer));
        try {
            this.connector=connector;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    void close() throws IOException {
        Profiler.startProfile(Profiler.FAST, AOServDaemonConnectionPool.class, "close()", null);
        try {
            try {
                closeImp();
            } catch(Exception err) {
                if(err instanceof IOException) throw (IOException)err;
                IOException ioErr=new IOException();
                ioErr.initCause(err);
                throw ioErr;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    protected void close(Object O) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnectionPool.class, "close(Object)", null);
        try {
            ((AOServDaemonConnection)O).close();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    AOServDaemonConnection getConnection() throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnectionPool.class, "getConnection()", null);
        try {
            return getConnection(1);
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    AOServDaemonConnection getConnection(int maxConnections) throws IOException {
        Profiler.startProfile(Profiler.FAST, AOServDaemonConnectionPool.class, "getConnection(int)", null);
        try {
            try {
                return (AOServDaemonConnection)getConnectionImp(maxConnections);
            } catch(Exception err) {
                if(err instanceof IOException) throw (IOException)err;
                IOException ioErr=new IOException();
                ioErr.initCause(err);
                throw ioErr;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    protected Object getConnectionObject() throws IOException {
        Profiler.startProfile(Profiler.FAST, AOServDaemonConnectionPool.class, "getConnectionObject()", null);
        try {
            return new AOServDaemonConnection(connector);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    protected boolean isClosed(Object O) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnectionPool.class, "isClosed(Object)", null);
        try {
            return ((AOServDaemonConnection)O).isClosed();
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    protected void printConnectionStats(ChainWriter out) {
        Profiler.startProfile(Profiler.IO, AOServDaemonConnectionPool.class, "printConnectionStats(ChainWriter)", null);
        try {
            out.print("<TABLE>\n"
                    + "  <TR><TH colspan=2><FONT size=+1>AOServ Daemon Connection Pool</FONT></TH></TR>\n"
                    + "  <TR><TD>Host:</TD><TD>").print(connector.hostname).print("</TD></TR>\n"
                    + "  <TR><TD>Port:</TD><TD>").print(connector.port).print("</TD></TR>\n"
                    + "  <TR><TD>Protocol:</TD><TD>").print(connector.protocol).print("</TD></TR>\n"
                    + "  <TR><TD>Key:</TD><TD>");
            String key=connector.key;
            int len=key.length();
            for(int c=0;c<len;c++) out.print('*');
            out.print("</TD></TR>\n");
        } finally {
            Profiler.endProfile(Profiler.IO);
        }
    }

    void printStatisticsHTML(ChainWriter out) throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnectionPool.class, "printStatisticsHTML(ChainWriter)", null);
        try {
            try {
                printStatisticsHTMLImp(out);
            } catch(Exception err) {
                if(err instanceof IOException) throw (IOException)err;
                IOException ioErr=new IOException();
                ioErr.initCause(err);
                throw ioErr;
            }
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    void releaseConnection(AOServDaemonConnection connection) throws IOException {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnectionPool.class, "releaseConnection(AOServDaemonConnection)", null);
        try {
            try {
                releaseConnectionImp(connection);
            } catch(Exception err) {
                if(err instanceof IOException) throw (IOException)err;
                IOException ioErr=new IOException();
                ioErr.initCause(err);
                throw ioErr;
            }
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }

    protected void resetConnection(Object O) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, AOServDaemonConnectionPool.class, "resetConnection(Object)", null);
        Profiler.endProfile(Profiler.INSTANTANEOUS);
    }

    protected void throwException(String message, Throwable allocateStackTrace) throws Exception {
        Profiler.startProfile(Profiler.FAST, AOServDaemonConnectionPool.class, "throwException(String,Throwable)", null);
        try {
            IOException err=new IOException(message);
            err.initCause(allocateStackTrace);
            throw err;
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }
}