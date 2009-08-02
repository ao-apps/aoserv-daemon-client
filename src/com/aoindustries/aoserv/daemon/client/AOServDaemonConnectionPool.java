package com.aoindustries.aoserv.daemon.client;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.io.AOPool;
import com.aoindustries.util.EncodingUtils;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Connections made by <code>TCPConnector</code> or any
 * of its derivatives are pooled and reused.
 *
 * @author  AO Industries, Inc.
 */
final class AOServDaemonConnectionPool extends AOPool<AOServDaemonConnection,IOException> {

    private final AOServDaemonConnector connector;

    AOServDaemonConnectionPool(AOServDaemonConnector connector, Logger logger) {
        super(
            AOServDaemonConnection.class,
            AOServDaemonConnectionPool.class.getName()+"?hostname=" + connector.hostname+"&local_ip="+connector.local_ip+"&port="+connector.port+"&protocol="+connector.protocol,
            connector.poolSize,
            connector.maxConnectionAge,
            logger
        );
        this.connector=connector;
    }

    protected void close(AOServDaemonConnection conn) {
        conn.close();
    }

    protected AOServDaemonConnection getConnectionObject() throws IOException {
        return new AOServDaemonConnection(connector);
    }

    protected boolean isClosed(AOServDaemonConnection conn) {
        return conn.isClosed();
    }

    protected void printConnectionStats(Appendable out) throws IOException {
        out.append("<table>\n"
                + "  <tr><th colspan='2'><span style='font-size:large;'>AOServ Daemon Connection Pool</span></th></tr>\n"
                + "  <tr><td>Local IP:</td><td>");
        EncodingUtils.encodeHtml(connector.local_ip, out);
        out.append("</td></tr>\n"
                + "  <tr><td>Host:</td><td>");
        EncodingUtils.encodeHtml(connector.hostname, out);
        out.append("</td></tr>\n"
                + "  <tr><td>Port:</td><td>").append(Integer.toString(connector.port)).append("</td></tr>\n"
                + "  <tr><td>Protocol:</td><td>");
        EncodingUtils.encodeHtml(connector.protocol, out);
        out.append("</td></tr>\n"
                + "  <tr><td>Key:</td><td>");
        String key=connector.key;
        int len=key.length();
        for(int c=0;c<len;c++) {
            out.append('*');
        }
        out.append("</td></tr>\n");
    }

    protected void resetConnection(AOServDaemonConnection conn) {
    }

    protected void throwException(String message, Throwable allocateStackTrace) throws IOException {
        IOException err=new IOException(message);
        err.initCause(allocateStackTrace);
        throw err;
    }
}