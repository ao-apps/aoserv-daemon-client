package com.aoindustries.aoserv.daemon.client;

/*
 * Copyright 2003-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */

/**
 * Reports the amount of data backed over time.
 *
 * @author  AO Industries, Inc.
 */
public class GetBackupDataReporter {

    private long totalSize=-1;
    private long finishedSize=-1;
    
    public GetBackupDataReporter() {
    }
    
    public synchronized void setTotalSize(long totalSize) {
        this.totalSize=totalSize;
    }
    
    public synchronized void addFinishedSize(long moreFinished) {
        this.finishedSize+=moreFinished;
    }

    public synchronized void setFinishedSize(long finishedSize) {
        this.finishedSize=finishedSize;
    }

    public synchronized String toString() {
        int tenthPercent;
        if(finishedSize==-1 || totalSize==-1) tenthPercent=-1;
        else tenthPercent=(int)(1000*finishedSize/totalSize);
        StringBuilder SB=
            new StringBuilder()
            .append('(')
        ;
        if(tenthPercent==-1) SB.append("Unknown% - ");
        else SB
            .append(tenthPercent/10)
            .append('.')
            .append(tenthPercent%10)
            .append("% - ")
        ;
        return SB
            .append(finishedSize==-1?"Unknown":Long.toString(finishedSize))
            .append(" of ")
            .append(totalSize==-1?"Unknown":Long.toString(totalSize))
            .append(')')
            .toString()
        ;
    }
}