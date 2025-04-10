/*
 * aoserv-daemon-client - Java client for the AOServ Daemon.
 * Copyright (C) 2000-2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2024  AO Industries, Inc.
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
 * along with aoserv-daemon-client.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoindustries.aoserv.daemon.client;

import com.aoindustries.aoserv.client.schema.AoservProtocol;
import java.util.HashMap;
import java.util.Map;

/**
 * Codes used in communication between the <code>AoservDaemonClient</code> and
 * the <code>AoservMaster</code>.
 *
 * @author  AO Industries, Inc.
 */
public final class AoservDaemonProtocol {

  /** Make no instances. */
  private AoservDaemonProtocol() {
    throw new AssertionError();
  }

  /**
   * Each time the protocols are changed in a way that is not backwards-compatible, please increase this number
   * so that older clients will not be allowed to connect, rather than get undefined erroneous behavior.
   *
   * <p>These versions roughly match the version of {@link AoservProtocol.Version#CURRENT_VERSION} at the time they were first used.</p>
   */
  public enum Version {
    VERSION_1_1("1.1"),
    VERSION_1_2("1.2"),
    VERSION_1_6("1.6"),
    VERSION_1_6_1("1.6.1"),
    VERSION_1_6_2("1.6.2"),
    VERSION_1_8("1.8"),
    VERSION_1_9("1.9"),
    VERSION_1_13("1.13"),
    VERSION_1_18("1.18"),
    VERSION_1_18_1("1.18.1"),
    VERSION_1_28("1.28"),
    VERSION_1_28_1("1.28.1"),
    VERSION_1_29("1.29"),
    VERSION_1_30("1.30"),
    VERSION_1_31("1.31"),
    VERSION_1_35("1.35"),
    VERSION_1_60("1.60"),
    VERSION_1_63("1.63"),
    VERSION_1_76("1.76"),
    VERSION_1_77("1.77"),
    VERSION_1_80_0("1.80.0"),
    VERSION_1_80_1("1.80.1"),
    VERSION_1_81_10("1.81.10"),
    VERSION_1_83_0("1.83.0"),
    VERSION_1_84_11("1.84.11"),
    VERSION_1_84_13("1.84.13"),
    VERSION_1_84_19("1.84.19");
    // New entries will typically also be added to:
    // AoservDaemonConnection.java
    // AoservDaemonServerThread.java.java


    private static final Map<String, Version> versionMap = new HashMap<>();

    static {
      for (Version version : values()) {
        versionMap.put(version.getVersion(), version);
      }
    }

    private final String version;

    private Version(String version) {
      this.version = version;
    }

    public String getVersion() {
      return version;
    }

    /**
     * Gets a specific version given its unique version string.
     *
     * @see  #getVersion()
     *
     * @throws  IllegalArgumentException if version not found
     */
    public static Version getVersion(String version) {
      Version versionEnum = versionMap.get(version);
      if (versionEnum == null) {
        throw new IllegalArgumentException("Version not found: " + version);
      }
      return versionEnum;
    }

    @Override
    public String toString() {
      return version;
    }
  }

  /**
   * The protocol codes used between the AOServ Master and the AOServ Daemons.
   */
  public static final int
      COMPARE_LINUX_ACCOUNT_PASSWORD = 0,
      DUMP_MYSQL_DATABASE = COMPARE_LINUX_ACCOUNT_PASSWORD + 1,
      DUMP_POSTGRES_DATABASE = DUMP_MYSQL_DATABASE + 1,
      FAILOVER_FILE_REPLICATION = DUMP_POSTGRES_DATABASE + 1,
      GET_AUTORESPONDER_CONTENT = FAILOVER_FILE_REPLICATION + 1,
      GET_CRON_TABLE = GET_AUTORESPONDER_CONTENT + 1,
      UNUSED_GET_DAEMON_PROFILE = GET_CRON_TABLE + 1,
      GET_DISK_DEVICE_TOTAL_SIZE = UNUSED_GET_DAEMON_PROFILE + 1,
      GET_DISK_DEVICE_USED_SIZE = GET_DISK_DEVICE_TOTAL_SIZE + 1,
      GET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD = GET_DISK_DEVICE_USED_SIZE + 1,
      GET_ENCRYPTED_MYSQL_USER_PASSWORD = GET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD + 1,
      GET_EMAIL_LIST_FILE = GET_ENCRYPTED_MYSQL_USER_PASSWORD + 1,
      GET_IMAP_FOLDER_SIZES = GET_EMAIL_LIST_FILE + 1,
      GET_INBOX_ATTRIBUTES = GET_IMAP_FOLDER_SIZES + 1,
      GET_MRTG_FILE = GET_INBOX_ATTRIBUTES + 1,
      GET_POSTGRES_PASSWORD = GET_MRTG_FILE + 1,
      GRANT_DAEMON_ACCESS = GET_POSTGRES_PASSWORD + 1,
      //INITIALIZE_HTTPD_SITE_PASSWD_FILE = GRANT_DAEMON_ACCESS + 1,
      IS_PROCMAIL_MANUAL = GRANT_DAEMON_ACCESS + 1,
      QUIT = IS_PROCMAIL_MANUAL + 1,
      REMOVE_EMAIL_LIST = QUIT + 1,
      RESTART_APACHE = REMOVE_EMAIL_LIST + 1,
      RESTART_CRON = RESTART_APACHE + 1,
      RESTART_MYSQL = RESTART_CRON + 1,
      RESTART_POSTGRES = RESTART_MYSQL + 1,
      RESTART_XFS = RESTART_POSTGRES + 1,
      RESTART_XVFB = RESTART_XFS + 1,
      SET_AUTORESPONDER_CONTENT = RESTART_XVFB + 1,
      SET_CRON_TABLE = SET_AUTORESPONDER_CONTENT + 1,
      SET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD = SET_CRON_TABLE + 1,
      SET_EMAIL_LIST_FILE = SET_ENCRYPTED_LINUX_ACCOUNT_PASSWORD + 1,
      UNUSED_SET_IMAP_FOLDER_SUBSCRIBED = SET_EMAIL_LIST_FILE + 1,
      SET_LINUX_SERVER_ACCOUNT_PASSWORD = UNUSED_SET_IMAP_FOLDER_SUBSCRIBED + 1,
      SET_MYSQL_USER_PASSWORD = SET_LINUX_SERVER_ACCOUNT_PASSWORD + 1,
      SET_POSTGRES_USER_PASSWORD = SET_MYSQL_USER_PASSWORD + 1,
      START_APACHE = SET_POSTGRES_USER_PASSWORD + 1,
      START_CRON = START_APACHE + 1,
      START_DISTRO = START_CRON + 1,
      START_JVM = START_DISTRO + 1,
      START_MYSQL = START_JVM + 1,
      START_POSTGRESQL = START_MYSQL + 1,
      START_XFS = START_POSTGRESQL + 1,
      START_XVFB = START_XFS + 1,
      STOP_APACHE = START_XVFB + 1,
      STOP_CRON = STOP_APACHE + 1,
      STOP_JVM = STOP_CRON + 1,
      STOP_MYSQL = STOP_JVM + 1,
      STOP_POSTGRESQL = STOP_MYSQL + 1,
      STOP_XFS = STOP_POSTGRESQL + 1,
      STOP_XVFB = STOP_XFS + 1,
      TAR_HOME_DIRECTORY = STOP_XVFB + 1,
      UNTAR_HOME_DIRECTORY = TAR_HOME_DIRECTORY + 1,
      OLD_WAIT_FOR_REBUILD = UNTAR_HOME_DIRECTORY + 1,
      GET_AWSTATS_FILE = OLD_WAIT_FOR_REBUILD + 1,
      GET_MYSQL_SLAVE_STATUS = GET_AWSTATS_FILE + 1,
      GET_MYSQL_MASTER_STATUS = GET_MYSQL_SLAVE_STATUS + 1,
      GET_NET_DEVICE_BONDING_REPORT = GET_MYSQL_MASTER_STATUS + 1,
      GET_3WARE_RAID_REPORT = GET_NET_DEVICE_BONDING_REPORT + 1,
      GET_MD_STAT_REPORT = GET_3WARE_RAID_REPORT + 1,
      GET_DRBD_REPORT = GET_MD_STAT_REPORT + 1,
      GET_HDD_TEMP_REPORT = GET_DRBD_REPORT + 1,
      GET_FILESYSTEMS_CSV_REPORT = GET_HDD_TEMP_REPORT + 1,
      GET_AO_SERVER_LOADAVG_REPORT = GET_FILESYSTEMS_CSV_REPORT + 1,
      GET_AO_SERVER_MEMINFO_REPORT = GET_AO_SERVER_LOADAVG_REPORT + 1,
      GET_NET_DEVICE_STATISTICS_REPORT = GET_AO_SERVER_MEMINFO_REPORT + 1,
      GET_AO_SERVER_SYSTEM_TIME_MILLIS = GET_NET_DEVICE_STATISTICS_REPORT + 1,
      GET_LVM_REPORT = GET_AO_SERVER_SYSTEM_TIME_MILLIS + 1,
      GET_HDD_MODEL_REPORT = GET_LVM_REPORT + 1,
      VNC_CONSOLE = GET_HDD_MODEL_REPORT + 1,
      GET_MYSQL_TABLE_STATUS = VNC_CONSOLE + 1,
      CHECK_MYSQL_TABLES = GET_MYSQL_TABLE_STATUS + 1,
      CHECK_PORT = CHECK_MYSQL_TABLES + 1,
      CHECK_SMTP_BLACKLIST = CHECK_PORT + 1,
      GET_UPS_STATUS = CHECK_SMTP_BLACKLIST + 1,
      CREATE_VIRTUAL_SERVER = GET_UPS_STATUS + 1,
      REBOOT_VIRTUAL_SERVER = CREATE_VIRTUAL_SERVER + 1,
      SHUTDOWN_VIRTUAL_SERVER = REBOOT_VIRTUAL_SERVER + 1,
      DESTROY_VIRTUAL_SERVER = SHUTDOWN_VIRTUAL_SERVER + 1,
      PAUSE_VIRTUAL_SERVER = DESTROY_VIRTUAL_SERVER + 1,
      UNPAUSE_VIRTUAL_SERVER = PAUSE_VIRTUAL_SERVER + 1,
      GET_VIRTUAL_SERVER_STATUS = UNPAUSE_VIRTUAL_SERVER + 1,
      GET_MD_MISMATCH_REPORT = GET_VIRTUAL_SERVER_STATUS + 1,
      GET_XEN_AUTO_START_LINKS = GET_MD_MISMATCH_REPORT + 1,
      VERIFY_VIRTUAL_DISK = GET_XEN_AUTO_START_LINKS + 1,
      UPDATE_VIRTUAL_DISK_LAST_UPDATED = VERIFY_VIRTUAL_DISK + 1,
      GET_FAILOVER_FILE_REPLICATION_ACTIVITY = UPDATE_VIRTUAL_DISK_LAST_UPDATED + 1,
      WAIT_FOR_HTTPD_SITE_REBUILD = GET_FAILOVER_FILE_REPLICATION_ACTIVITY + 1,
      WAIT_FOR_LINUX_ACCOUNT_REBUILD = WAIT_FOR_HTTPD_SITE_REBUILD + 1,
      WAIT_FOR_MYSQL_DATABASE_REBUILD = WAIT_FOR_LINUX_ACCOUNT_REBUILD + 1,
      WAIT_FOR_MYSQL_DB_USER_REBUILD = WAIT_FOR_MYSQL_DATABASE_REBUILD + 1,
      WAIT_FOR_MYSQL_SERVER_REBUILD = WAIT_FOR_MYSQL_DB_USER_REBUILD + 1,
      WAIT_FOR_MYSQL_USER_REBUILD = WAIT_FOR_MYSQL_SERVER_REBUILD + 1,
      WAIT_FOR_POSTGRES_DATABASE_REBUILD = WAIT_FOR_MYSQL_USER_REBUILD + 1,
      WAIT_FOR_POSTGRES_SERVER_REBUILD = WAIT_FOR_POSTGRES_DATABASE_REBUILD + 1,
      WAIT_FOR_POSTGRES_USER_REBUILD = WAIT_FOR_POSTGRES_SERVER_REBUILD + 1,
      CHECK_SSL_CERTIFICATE = WAIT_FOR_POSTGRES_USER_REBUILD + 1,
      GET_HTTPD_SERVER_CONCURRENCY = CHECK_SSL_CERTIFICATE + 1,

      DONE = 0,
      NEXT = DONE + 1,
      NEXT_CHUNK = NEXT + 1,
      IO_EXCEPTION = NEXT_CHUNK + 1,
      SQL_EXCEPTION = IO_EXCEPTION + 1;

  /**
   * Table IDs used for backwards compatibility with clients that still require
   * {@link #OLD_WAIT_FOR_REBUILD}.  These may be removed once there are no daemons
   * or clients supporting {@link Version#VERSION_1_77} or older.
   */
  public static final int
      OLD_HTTPD_SITES_TABLE_ID = 66,
      OLD_LINUX_ACCOUNTS_TABLE_ID = 86,
      OLD_MYSQL_DATABASES_TABLE_ID = 102,
      OLD_MYSQL_DB_USERS_TABLE_ID = 103,
      OLD_MYSQL_USERS_TABLE_ID = 107,
      OLD_POSTGRES_DATABASES_TABLE_ID = 124,
      OLD_POSTGRES_SERVERS_TABLE_ID = 128,
      OLD_POSTGRES_USERS_TABLE_ID = 129;

  public static final int
      FAILOVER_FILE_REPLICATION_NO_CHANGE = 0,
      FAILOVER_FILE_REPLICATION_MODIFIED = 1,
      FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA = 2,
      FAILOVER_FILE_REPLICATION_MODIFIED_REQUEST_DATA_CHUNKED = 3;

  /**
   * The number of bytes per block when using chunked transfers.
   */
  public static final int FAILOVER_FILE_REPLICATION_CHUNK_SIZE_BITS = 20;
  public static final int FAILOVER_FILE_REPLICATION_CHUNK_SIZE = 1 << FAILOVER_FILE_REPLICATION_CHUNK_SIZE_BITS; // 1 MiB

  /**
   * The GZIP buffer size for compressed transfers.
   * See <a href="https://stackoverflow.com/a/55151757">https://stackoverflow.com/a/55151757</a>.
   */
  public static final int FAILOVER_FILE_REPLICATION_GZIP_BUFFER_SIZE = 65536;
}
