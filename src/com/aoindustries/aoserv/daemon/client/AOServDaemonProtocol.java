package com.aoindustries.aoserv.daemon.client;

/*
 * Copyright 2000-2008 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
/**
 * Codes used in communication between the <code>AOServServer</code> and
 * the <code>SimpleAOClient</code>.
 *
 * @author  AO Industries, Inc.
 */
final public class AOServDaemonProtocol {

    /**
     * Each time the protocols are changed in a way that is not backwards-compatible, please increase this number
     * so that older clients will not be allowed to connect, rather than get undefined erroneous behavior.
     * These versions roughly match the version of AOServProtocol at the time they were first used.
     */
    public static final String VERSION_1_1="1.1";
    public static final String VERSION_1_2="1.2";
    public static final String VERSION_1_6="1.6";
    public static final String VERSION_1_6_1="1.6.1";
    public static final String VERSION_1_6_2="1.6.2";
    public static final String VERSION_1_8="1.8";
    public static final String VERSION_1_9="1.9";
    public static final String VERSION_1_13="1.13";
    public static final String VERSION_1_18="1.18";
    public static final String VERSION_1_18_1="1.18.1";
    public static final String VERSION_1_28="1.28";
    public static final String VERSION_1_28_1="1.28.1";
    public static final String VERSION_1_29="1.29";
    public static final String VERSION_1_30="1.30";
    public static final String VERSION_1_31="1.31";
    public static final String VERSION_1_35="1.35";
    public static final String CURRENT_VERSION=VERSION_1_35;

    /**
     * The protocol codes used between the AOServ Master and the AOServ Daemons
     */
    public static final int
        COMPARE_LINUX_ACCOUNT_PASSWORD=0,
        DUMP_MYSQL_DATABASE=COMPARE_LINUX_ACCOUNT_PASSWORD+1,
        DUMP_POSTGRES_DATABASE=DUMP_MYSQL_DATABASE+1,
        FAILOVER_FILE_REPLICATION=DUMP_POSTGRES_DATABASE+1,
        GET_AUTORESPONDER_CONTENT=FAILOVER_FILE_REPLICATION+1,
        GET_CRON_TABLE=GET_AUTORESPONDER_CONTENT+1,
        GET_DAEMON_PROFILE=GET_CRON_TABLE+1,
        GET_DISK_DEVICE_TOTAL_SIZE=GET_DAEMON_PROFILE+1,
        GET_DISK_DEVICE_USED_SIZE=GET_DISK_DEVICE_TOTAL_SIZE+1,
        GET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD=GET_DISK_DEVICE_USED_SIZE+1,
        GET_ENCRYPTED_MYSQL_USER_PASSWORD=GET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD+1,
        GET_EMAIL_LIST_FILE=GET_ENCRYPTED_MYSQL_USER_PASSWORD+1,
        GET_IMAP_FOLDER_SIZES=GET_EMAIL_LIST_FILE+1,
        GET_INBOX_ATTRIBUTES=GET_IMAP_FOLDER_SIZES+1,
        GET_MRTG_FILE=GET_INBOX_ATTRIBUTES+1,
        GET_POSTGRES_PASSWORD=GET_MRTG_FILE+1,
        GRANT_DAEMON_ACCESS=GET_POSTGRES_PASSWORD+1,
        //INITIALIZE_HTTPD_SITE_PASSWD_FILE=GRANT_DAEMON_ACCESS+1,
        IS_PROCMAIL_MANUAL=GRANT_DAEMON_ACCESS+1,
        QUIT=IS_PROCMAIL_MANUAL+1,
        REMOVE_EMAIL_LIST=QUIT+1,
        RESTART_APACHE=REMOVE_EMAIL_LIST+1,
        RESTART_CRON=RESTART_APACHE+1,
        RESTART_MYSQL=RESTART_CRON+1,
        RESTART_POSTGRES=RESTART_MYSQL+1,
        RESTART_XFS=RESTART_POSTGRES+1,
        RESTART_XVFB=RESTART_XFS+1,
        SET_AUTORESPONDER_CONTENT=RESTART_XVFB+1,
        SET_CRON_TABLE=SET_AUTORESPONDER_CONTENT+1,
        SET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD=SET_CRON_TABLE+1,
        SET_EMAIL_LIST_FILE=SET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD+1,
        SET_IMAP_FOLDER_SUBSCRIBED=SET_EMAIL_LIST_FILE+1,
        SET_LINUX_SERVER_ACCOUNT_PASSWORD=SET_IMAP_FOLDER_SUBSCRIBED+1,
        SET_MYSQL_USER_PASSWORD=SET_LINUX_SERVER_ACCOUNT_PASSWORD+1,
        SET_POSTGRES_USER_PASSWORD=SET_MYSQL_USER_PASSWORD+1,
        START_APACHE=SET_POSTGRES_USER_PASSWORD+1,
        START_CRON=START_APACHE+1,
        START_DISTRO=START_CRON+1,
        START_JVM=START_DISTRO+1,
        START_MYSQL=START_JVM+1,
        START_POSTGRESQL=START_MYSQL+1,
        START_XFS=START_POSTGRESQL+1,
        START_XVFB=START_XFS+1,
        STOP_APACHE=START_XVFB+1,
        STOP_CRON=STOP_APACHE+1,
        STOP_JVM=STOP_CRON+1,
        STOP_MYSQL=STOP_JVM+1,
        STOP_POSTGRESQL=STOP_MYSQL+1,
        STOP_XFS=STOP_POSTGRESQL+1,
        STOP_XVFB=STOP_XFS+1,
        TAR_HOME_DIRECTORY=STOP_XVFB+1,
        UNTAR_HOME_DIRECTORY=TAR_HOME_DIRECTORY+1,
        WAIT_FOR_REBUILD=UNTAR_HOME_DIRECTORY+1,
        GET_AWSTATS_FILE=WAIT_FOR_REBUILD+1,
        GET_MYSQL_SLAVE_STATUS=GET_AWSTATS_FILE+1,
        GET_MYSQL_MASTER_STATUS = GET_MYSQL_SLAVE_STATUS+1,
        GET_NET_DEVICE_BONDING_REPORT = GET_MYSQL_MASTER_STATUS+1,
        GET_3WARE_RAID_REPORT = GET_NET_DEVICE_BONDING_REPORT+1,
        GET_MD_RAID_REPORT = GET_3WARE_RAID_REPORT+1,
        GET_DRBD_REPORT = GET_MD_RAID_REPORT+1,
        GET_HDD_TEMP_REPORT = GET_DRBD_REPORT+1,
        GET_FILESYSTEMS_CSV_REPORT = GET_HDD_TEMP_REPORT+1,
        GET_AO_SERVER_LOADAVG_REPORT = GET_FILESYSTEMS_CSV_REPORT+1,
        GET_AO_SERVER_MEMINFO_REPORT = GET_AO_SERVER_LOADAVG_REPORT+1,
        GET_NET_DEVICE_STATISTICS_REPORT = GET_AO_SERVER_MEMINFO_REPORT+1,

        DONE=0,
        NEXT=DONE+1,
        NEXT_CHUNK=NEXT+1,
        IO_EXCEPTION=NEXT_CHUNK+1,
        SQL_EXCEPTION=IO_EXCEPTION+1
    ;

    public static final int
        FAILOVER_FILE_REPLICATION_NO_CHANGE=0,
        FAILOVER_FILE_REPLICATION_MODIFIED=1,
        FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA=2,
        FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA_CHUNKED=3
    ;

     /**
     * The number of bytes per block when using chunked transfers.
     * This value is optimal for PostgreSQL because it matches its
     * page size.
     */
    public static final int FAILOVER_FILE_REPLICATION_CHUNK_SIZE=8192;

    private AOServDaemonProtocol() {}
}
