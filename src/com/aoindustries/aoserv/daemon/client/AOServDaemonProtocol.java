package com.aoindustries.aoserv.daemon.client;

/*
 * Copyright 2000-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
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
    public static final String CURRENT_VERSION=VERSION_1_28_1;

    /**
     * The protocol codes used between the AOServ Master and the AOServ Daemons
     */
    public static final int
        BACKUP_INTERBASE_DATABASE=0,
        BACKUP_MYSQL_DATABASE=BACKUP_INTERBASE_DATABASE+1,
        BACKUP_POSTGRES_DATABASE=BACKUP_MYSQL_DATABASE+1,
        BACKUP_POSTGRES_DATABASE_SEND_DATA=BACKUP_POSTGRES_DATABASE+1,
        COMPARE_LINUX_ACCOUNT_PASSWORD=BACKUP_POSTGRES_DATABASE_SEND_DATA+1,
        DUMP_INTERBASE_DATABASE=COMPARE_LINUX_ACCOUNT_PASSWORD+1,
        DUMP_MYSQL_DATABASE=DUMP_INTERBASE_DATABASE+1,
        DUMP_POSTGRES_DATABASE=DUMP_MYSQL_DATABASE+1,
        FAILOVER_FILE_REPLICATION=DUMP_POSTGRES_DATABASE+1,
        GET_AUTORESPONDER_CONTENT=FAILOVER_FILE_REPLICATION+1,
        GET_BACKUP_DATA=GET_AUTORESPONDER_CONTENT+1,
        GET_CRON_TABLE=GET_BACKUP_DATA+1,
        GET_DAEMON_PROFILE=GET_CRON_TABLE+1,
        GET_DISK_DEVICE_TOTAL_SIZE=GET_DAEMON_PROFILE+1,
        GET_DISK_DEVICE_USED_SIZE=GET_DISK_DEVICE_TOTAL_SIZE+1,
        GET_ENCRYPTED_INTERBASE_USER_PASSWORD=GET_DISK_DEVICE_USED_SIZE+1,
        GET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD=GET_ENCRYPTED_INTERBASE_USER_PASSWORD+1,
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
        REMOVE_BACKUP_DATA=QUIT+1,
        REMOVE_EMAIL_LIST=REMOVE_BACKUP_DATA+1,
        RESTART_APACHE=REMOVE_EMAIL_LIST+1,
        RESTART_CRON=RESTART_APACHE+1,
        RESTART_INTERBASE=RESTART_CRON+1,
        RESTART_MYSQL=RESTART_INTERBASE+1,
        RESTART_POSTGRES=RESTART_MYSQL+1,
        RESTART_XFS=RESTART_POSTGRES+1,
        RESTART_XVFB=RESTART_XFS+1,
        SET_AUTORESPONDER_CONTENT=RESTART_XVFB+1,
        SET_CRON_TABLE=SET_AUTORESPONDER_CONTENT+1,
        SET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD=SET_CRON_TABLE+1,
        SET_EMAIL_LIST_FILE=SET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD+1,
        SET_IMAP_FOLDER_SUBSCRIBED=SET_EMAIL_LIST_FILE+1,
        SET_INTERBASE_USER_PASSWORD=SET_IMAP_FOLDER_SUBSCRIBED+1,
        SET_LINUX_SERVER_ACCOUNT_PASSWORD=SET_INTERBASE_USER_PASSWORD+1,
        SET_MYSQL_USER_PASSWORD=SET_LINUX_SERVER_ACCOUNT_PASSWORD+1,
        SET_POSTGRES_USER_PASSWORD=SET_MYSQL_USER_PASSWORD+1,
        START_APACHE=SET_POSTGRES_USER_PASSWORD+1,
        START_CRON=START_APACHE+1,
        START_DISTRO=START_CRON+1,
        START_INTERBASE=START_DISTRO+1,
        START_JVM=START_INTERBASE+1,
        START_MYSQL=START_JVM+1,
        START_POSTGRESQL=START_MYSQL+1,
        START_XFS=START_POSTGRESQL+1,
        START_XVFB=START_XFS+1,
        STOP_APACHE=START_XVFB+1,
        STOP_CRON=STOP_APACHE+1,
        STOP_INTERBASE=STOP_CRON+1,
        STOP_JVM=STOP_INTERBASE+1,
        STOP_MYSQL=STOP_JVM+1,
        STOP_POSTGRESQL=STOP_MYSQL+1,
        STOP_XFS=STOP_POSTGRESQL+1,
        STOP_XVFB=STOP_XFS+1,
        STORE_BACKUP_DATA=STOP_XVFB+1,
        TAR_HOME_DIRECTORY=STORE_BACKUP_DATA+1,
        UNTAR_HOME_DIRECTORY=TAR_HOME_DIRECTORY+1,
        WAIT_FOR_REBUILD=UNTAR_HOME_DIRECTORY+1,
        GET_AWSTATS_FILE=WAIT_FOR_REBUILD+1,
        STORE_BACKUP_DATA_DIRECT_ACCESS=GET_AWSTATS_FILE+1,
        GET_MYSQL_SLAVE_STATUS=STORE_BACKUP_DATA_DIRECT_ACCESS+1,
        GET_MYSQL_MASTER_STATUS = GET_MYSQL_SLAVE_STATUS + 1,

        DONE=0,
        NEXT=DONE+1,
        NEXT_CHUNK=NEXT+1,
        IO_EXCEPTION=NEXT_CHUNK+1,
        SQL_EXCEPTION=IO_EXCEPTION+1
    ;

    private AOServDaemonProtocol() {}
}