-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- 1021  db2 LEVEL Version 10.2.0  --> 11.5.0  1150

CREATE TABLE ASNCDC.IBMQREP_COLVERSION(
LSN                             VARCHAR( 16) FOR BIT DATA NOT NULL,
TABLEID1                        SMALLINT NOT NULL,
TABLEID2                        SMALLINT NOT NULL,
POSITION                        SMALLINT NOT NULL,
NAME                            VARCHAR(128) NOT NULL,
TYPE                            SMALLINT NOT NULL,
LENGTH                          INTEGER NOT NULL,
NULLS                           CHAR(  1) NOT NULL,
DEFAULT                         VARCHAR(1536),
CODEPAGE                        INTEGER,
SCALE                           INTEGER,
VERSION_TIME                    TIMESTAMP NOT NULL WITH DEFAULT )
	ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMQREP_COLVERSIOX
ON ASNCDC.IBMQREP_COLVERSION(
LSN                             ASC,
TABLEID1                        ASC,
TABLEID2                        ASC,
POSITION                        ASC);

CREATE  INDEX ASNCDC.IX2COLVERSION
ON ASNCDC.IBMQREP_COLVERSION(
TABLEID1                        ASC,
TABLEID2                        ASC);

CREATE TABLE ASNCDC.IBMQREP_TABVERSION(
LSN                             VARCHAR( 16) FOR BIT DATA NOT NULL,
TABLEID1                        SMALLINT NOT NULL,
TABLEID2                        SMALLINT NOT NULL,
VERSION                         INTEGER NOT NULL,
SOURCE_OWNER                    VARCHAR(128) NOT NULL,
SOURCE_NAME                     VARCHAR(128) NOT NULL,
VERSION_TIME                    TIMESTAMP NOT NULL WITH DEFAULT )
	ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMQREP_TABVERSIOX
ON ASNCDC.IBMQREP_TABVERSION(
LSN                             ASC,
TABLEID1                        ASC,
TABLEID2                        ASC,
VERSION                         ASC);

CREATE  INDEX ASNCDC.IX2TABVERSION
ON ASNCDC.IBMQREP_TABVERSION(
TABLEID1                        ASC,
TABLEID2                        ASC);

CREATE  INDEX ASNCDC.IX3TABVERSION
ON ASNCDC.IBMQREP_TABVERSION(
SOURCE_OWNER                    ASC,
SOURCE_NAME                     ASC);

CREATE TABLE ASNCDC.IBMSNAP_APPLEVEL(
ARCH_LEVEL                      CHAR(  4) NOT NULL WITH DEFAULT '1021')
ORGANIZE BY ROW;

INSERT INTO ASNCDC.IBMSNAP_APPLEVEL(ARCH_LEVEL) VALUES (
'1021');

CREATE TABLE ASNCDC.IBMSNAP_CAPMON(
MONITOR_TIME                    TIMESTAMP NOT NULL,
RESTART_TIME                    TIMESTAMP NOT NULL,
CURRENT_MEMORY                  INT NOT NULL,
CD_ROWS_INSERTED                INT NOT NULL,
RECAP_ROWS_SKIPPED              INT NOT NULL,
TRIGR_ROWS_SKIPPED              INT NOT NULL,
CHG_ROWS_SKIPPED                INT NOT NULL,
TRANS_PROCESSED                 INT NOT NULL,
TRANS_SPILLED                   INT NOT NULL,
MAX_TRANS_SIZE                  INT NOT NULL,
LOCKING_RETRIES                 INT NOT NULL,
JRN_LIB                         CHAR( 10),
JRN_NAME                        CHAR( 10),
LOGREADLIMIT                    INT NOT NULL,
CAPTURE_IDLE                    INT NOT NULL,
SYNCHTIME                       TIMESTAMP NOT NULL,
CURRENT_LOG_TIME                TIMESTAMP NOT NULL WITH DEFAULT ,
LAST_EOL_TIME                   TIMESTAMP,
RESTART_SEQ                     VARCHAR( 16) FOR BIT DATA NOT NULL WITH DEFAULT ,
CURRENT_SEQ                     VARCHAR( 16) FOR BIT DATA NOT NULL WITH DEFAULT ,
RESTART_MAXCMTSEQ               VARCHAR( 16) FOR BIT DATA NOT NULL WITH DEFAULT ,
LOGREAD_API_TIME                INT,
NUM_LOGREAD_CALLS               INT,
NUM_END_OF_LOGS                 INT,
LOGRDR_SLEEPTIME                INT,
NUM_LOGREAD_F_CALLS             INT,
TRANS_QUEUED                    INT,
NUM_WARNTXS                     INT,
NUM_WARNLOGAPI                  INT)
	ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_CAPMONX
ON ASNCDC.IBMSNAP_CAPMON(
MONITOR_TIME                    ASC);

ALTER TABLE ASNCDC.IBMSNAP_CAPMON VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_CAPPARMS(
RETENTION_LIMIT                 INT,
LAG_LIMIT                       INT,
COMMIT_INTERVAL                 INT,
PRUNE_INTERVAL                  INT,
TRACE_LIMIT                     INT,
MONITOR_LIMIT                   INT,
MONITOR_INTERVAL                INT,
MEMORY_LIMIT                    SMALLINT,
REMOTE_SRC_SERVER               CHAR( 18),
AUTOPRUNE                       CHAR(  1),
TERM                            CHAR(  1),
AUTOSTOP                        CHAR(  1),
LOGREUSE                        CHAR(  1),
LOGSTDOUT                       CHAR(  1),
SLEEP_INTERVAL                  SMALLINT,
CAPTURE_PATH                    VARCHAR(1040),
STARTMODE                       VARCHAR( 10),
LOGRDBUFSZ                      INT NOT NULL WITH DEFAULT 256,
ARCH_LEVEL                      CHAR( 4) NOT NULL WITH DEFAULT '1021',
COMPATIBILITY                   CHAR( 4) NOT NULL WITH DEFAULT '1021')
	ORGANIZE BY ROW;

INSERT INTO ASNCDC.IBMSNAP_CAPPARMS(
RETENTION_LIMIT,
LAG_LIMIT,
COMMIT_INTERVAL,
PRUNE_INTERVAL,
TRACE_LIMIT,
MONITOR_LIMIT,
MONITOR_INTERVAL,
MEMORY_LIMIT,
SLEEP_INTERVAL,
AUTOPRUNE,
TERM,
AUTOSTOP,
LOGREUSE,
LOGSTDOUT,
CAPTURE_PATH,
STARTMODE,
COMPATIBILITY)
VALUES (
10080,
10080,
30,
300,
10080,
10080,
300,
32,
5,
'Y',
'Y',
'N',
'N',
'N',
NULL,
'WARMSI',
'1021'
);

CREATE TABLE ASNCDC.IBMSNAP_CAPSCHEMAS (
		CAP_SCHEMA_NAME VARCHAR(128 OCTETS) NOT NULL
	)
	ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_CAPSCHEMASX
	ON ASNCDC.IBMSNAP_CAPSCHEMAS
	(CAP_SCHEMA_NAME		ASC);

INSERT INTO ASNCDC.IBMSNAP_CAPSCHEMAS(CAP_SCHEMA_NAME) VALUES (
'ASNCDC');

CREATE TABLE ASNCDC.IBMSNAP_CAPTRACE(
OPERATION                       CHAR( 8) NOT NULL,
TRACE_TIME                      TIMESTAMP NOT NULL,
DESCRIPTION                     VARCHAR(1024) NOT NULL)
	ORGANIZE BY ROW;

CREATE  INDEX ASNCDC.IBMSNAP_CAPTRACEX
ON ASNCDC.IBMSNAP_CAPTRACE(
TRACE_TIME                      ASC);

ALTER TABLE ASNCDC.IBMSNAP_CAPTRACE VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_PRUNCNTL(
TARGET_SERVER                   CHAR(18) NOT NULL,
TARGET_OWNER                    VARCHAR(128) NOT NULL,
TARGET_TABLE                    VARCHAR(128) NOT NULL,
SYNCHTIME                       TIMESTAMP,
SYNCHPOINT                      VARCHAR( 16) FOR BIT DATA,
SOURCE_OWNER                    VARCHAR(128) NOT NULL,
SOURCE_TABLE                    VARCHAR(128) NOT NULL,
SOURCE_VIEW_QUAL                SMALLINT NOT NULL,
APPLY_QUAL                      CHAR( 18) NOT NULL,
SET_NAME                        CHAR( 18) NOT NULL,
CNTL_SERVER                     CHAR( 18) NOT NULL,
TARGET_STRUCTURE                SMALLINT NOT NULL,
CNTL_ALIAS                      CHAR( 8),
PHYS_CHANGE_OWNER               VARCHAR(128),
PHYS_CHANGE_TABLE               VARCHAR(128),
MAP_ID                          VARCHAR(10) NOT NULL)
	ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_PRUNCNTLX
ON ASNCDC.IBMSNAP_PRUNCNTL(
SOURCE_OWNER                    ASC,
SOURCE_TABLE                    ASC,
SOURCE_VIEW_QUAL                ASC,
APPLY_QUAL                      ASC,
SET_NAME                        ASC,
TARGET_SERVER                   ASC,
TARGET_TABLE                    ASC,
TARGET_OWNER                    ASC);

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_PRUNCNTLX1
ON ASNCDC.IBMSNAP_PRUNCNTL(
MAP_ID                          ASC);

CREATE  INDEX ASNCDC.IBMSNAP_PRUNCNTLX2
ON ASNCDC.IBMSNAP_PRUNCNTL(
PHYS_CHANGE_OWNER               ASC,
PHYS_CHANGE_TABLE               ASC);

CREATE  INDEX ASNCDC.IBMSNAP_PRUNCNTLX3
ON ASNCDC.IBMSNAP_PRUNCNTL(
APPLY_QUAL                      ASC,
SET_NAME                        ASC,
TARGET_SERVER                   ASC);

ALTER TABLE ASNCDC.IBMSNAP_PRUNCNTL VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_PRUNE_LOCK(
DUMMY                           CHAR( 1))
	ORGANIZE BY ROW;

CREATE TABLE ASNCDC.IBMSNAP_PRUNE_SET(
TARGET_SERVER                   CHAR( 18) NOT NULL,
APPLY_QUAL                      CHAR( 18) NOT NULL,
SET_NAME                        CHAR( 18) NOT NULL,
SYNCHTIME                       TIMESTAMP,
SYNCHPOINT                      VARCHAR( 16) FOR BIT DATA NOT NULL)
	ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_PRUNE_SETX
ON ASNCDC.IBMSNAP_PRUNE_SET(
TARGET_SERVER                   ASC,
APPLY_QUAL                      ASC,
SET_NAME                        ASC);

ALTER TABLE ASNCDC.IBMSNAP_PRUNE_SET VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_REGISTER(
SOURCE_OWNER                    VARCHAR(128) NOT NULL,
SOURCE_TABLE                    VARCHAR(128) NOT NULL,
SOURCE_VIEW_QUAL                SMALLINT NOT NULL,
GLOBAL_RECORD                   CHAR( 1) NOT NULL,
SOURCE_STRUCTURE                SMALLINT NOT NULL,
SOURCE_CONDENSED                CHAR( 1) NOT NULL,
SOURCE_COMPLETE                 CHAR(  1) NOT NULL,
CD_OWNER                        VARCHAR(128),
CD_TABLE                        VARCHAR(128),
PHYS_CHANGE_OWNER               VARCHAR(128),
PHYS_CHANGE_TABLE               VARCHAR(128),
CD_OLD_SYNCHPOINT               VARCHAR( 16) FOR BIT DATA,
CD_NEW_SYNCHPOINT               VARCHAR( 16) FOR BIT DATA,
DISABLE_REFRESH                 SMALLINT NOT NULL,
CCD_OWNER                       VARCHAR(128),
CCD_TABLE                       VARCHAR(128),
CCD_OLD_SYNCHPOINT              VARCHAR( 16) FOR BIT DATA,
SYNCHPOINT                      VARCHAR( 16) FOR BIT DATA,
SYNCHTIME                       TIMESTAMP,
CCD_CONDENSED                   CHAR(  1),
CCD_COMPLETE                    CHAR(  1),
ARCH_LEVEL                      CHAR(  4) NOT NULL,
DESCRIPTION                     CHAR(254),
BEFORE_IMG_PREFIX               VARCHAR(   4),
CONFLICT_LEVEL                  CHAR(   1),
CHG_UPD_TO_DEL_INS              CHAR(   1),
CHGONLY                         CHAR(   1),
RECAPTURE                       CHAR(   1),
OPTION_FLAGS                    CHAR(   4) NOT NULL,
STOP_ON_ERROR                   CHAR(  1) WITH DEFAULT 'Y',
STATE                           CHAR(  1) WITH DEFAULT 'I',
STATE_INFO                      CHAR(  8))
	ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_REGISTERX
ON ASNCDC.IBMSNAP_REGISTER(
SOURCE_OWNER                    ASC,
SOURCE_TABLE                    ASC,
SOURCE_VIEW_QUAL                ASC);

CREATE  INDEX ASNCDC.IBMSNAP_REGISTERX1
ON ASNCDC.IBMSNAP_REGISTER(
PHYS_CHANGE_OWNER               ASC,
PHYS_CHANGE_TABLE               ASC);

CREATE  INDEX ASNCDC.IBMSNAP_REGISTERX2
ON ASNCDC.IBMSNAP_REGISTER(
GLOBAL_RECORD                   ASC);

ALTER TABLE ASNCDC.IBMSNAP_REGISTER VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_RESTART(
MAX_COMMITSEQ                   VARCHAR( 16) FOR BIT DATA NOT NULL,
MAX_COMMIT_TIME                 TIMESTAMP NOT NULL,
MIN_INFLIGHTSEQ                 VARCHAR( 16) FOR BIT DATA NOT NULL,
CURR_COMMIT_TIME                TIMESTAMP NOT NULL,
CAPTURE_FIRST_SEQ               VARCHAR( 16) FOR BIT DATA NOT NULL)
	ORGANIZE BY ROW;

CREATE TABLE ASNCDC.IBMSNAP_SIGNAL(
SIGNAL_TIME                     TIMESTAMP NOT NULL WITH DEFAULT ,
SIGNAL_TYPE                     VARCHAR( 30) NOT NULL,
SIGNAL_SUBTYPE                  VARCHAR( 30),
SIGNAL_INPUT_IN                 VARCHAR(500),
SIGNAL_STATE                    CHAR( 1) NOT NULL,
SIGNAL_LSN                      VARCHAR( 16) FOR BIT DATA)
DATA CAPTURE CHANGES
	ORGANIZE BY ROW;

CREATE  INDEX ASNCDC.IBMSNAP_SIGNALX
ON ASNCDC.IBMSNAP_SIGNAL(
SIGNAL_TIME                     ASC);

ALTER TABLE ASNCDC.IBMSNAP_SIGNAL VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_SUBS_COLS(
APPLY_QUAL                      CHAR( 18) NOT NULL,
SET_NAME                        CHAR( 18) NOT NULL,
WHOS_ON_FIRST                   CHAR(  1) NOT NULL,
TARGET_OWNER                    VARCHAR(128) NOT NULL,
TARGET_TABLE                    VARCHAR(128) NOT NULL,
COL_TYPE                        CHAR(  1) NOT NULL,
TARGET_NAME                     VARCHAR(128) NOT NULL,
IS_KEY                          CHAR(  1) NOT NULL,
COLNO                           SMALLINT NOT NULL,
EXPRESSION                      VARCHAR(1024) NOT NULL)
ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_SUBS_COLSX
ON ASNCDC.IBMSNAP_SUBS_COLS(
APPLY_QUAL                      ASC,
SET_NAME                        ASC,
WHOS_ON_FIRST                   ASC,
TARGET_OWNER                    ASC,
TARGET_TABLE                    ASC,
TARGET_NAME                     ASC);

ALTER TABLE ASNCDC.IBMSNAP_SUBS_COLS VOLATILE CARDINALITY;

--CREATE UNIQUE INDEX ASNCDC.IBMSNAP_SUBS_EVENTX
--ON ASNCDC.IBMSNAP_SUBS_EVENT(
--EVENT_NAME                      ASC,
--EVENT_TIME                      ASC);


--ALTER TABLE ASNCDC.IBMSNAP_SUBS_EVENT VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_SUBS_MEMBR(
APPLY_QUAL                      CHAR( 18) NOT NULL,
SET_NAME                        CHAR( 18) NOT NULL,
WHOS_ON_FIRST                   CHAR(  1) NOT NULL,
SOURCE_OWNER                    VARCHAR(128) NOT NULL,
SOURCE_TABLE                    VARCHAR(128) NOT NULL,
SOURCE_VIEW_QUAL                SMALLINT NOT NULL,
TARGET_OWNER                    VARCHAR(128) NOT NULL,
TARGET_TABLE                    VARCHAR(128) NOT NULL,
TARGET_CONDENSED                CHAR(  1) NOT NULL,
TARGET_COMPLETE                 CHAR(  1) NOT NULL,
TARGET_STRUCTURE                SMALLINT NOT NULL,
PREDICATES                      VARCHAR(1024),
MEMBER_STATE                    CHAR(  1),
TARGET_KEY_CHG                  CHAR(  1) NOT NULL,
UOW_CD_PREDICATES               VARCHAR(1024),
JOIN_UOW_CD                     CHAR(  1),
LOADX_TYPE                      SMALLINT,
LOADX_SRC_N_OWNER               VARCHAR( 128),
LOADX_SRC_N_TABLE               VARCHAR(128))
ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_SUBS_MEMBRX
ON ASNCDC.IBMSNAP_SUBS_MEMBR(
APPLY_QUAL                      ASC,
SET_NAME                        ASC,
WHOS_ON_FIRST                   ASC,
SOURCE_OWNER                    ASC,
SOURCE_TABLE                    ASC,
SOURCE_VIEW_QUAL                ASC,
TARGET_OWNER                    ASC,
TARGET_TABLE                    ASC);

ALTER TABLE ASNCDC.IBMSNAP_SUBS_MEMBR VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_SUBS_SET(
APPLY_QUAL                      CHAR( 18) NOT NULL,
SET_NAME                        CHAR( 18) NOT NULL,
SET_TYPE                        CHAR(  1) NOT NULL,
WHOS_ON_FIRST                   CHAR(  1) NOT NULL,
ACTIVATE                        SMALLINT NOT NULL,
SOURCE_SERVER                   CHAR( 18) NOT NULL,
SOURCE_ALIAS                    CHAR(  8),
TARGET_SERVER                   CHAR( 18) NOT NULL,
TARGET_ALIAS                    CHAR(  8),
STATUS                          SMALLINT NOT NULL,
LASTRUN                         TIMESTAMP NOT NULL,
REFRESH_TYPE                    CHAR( 1) NOT NULL,
SLEEP_MINUTES                   INT,
EVENT_NAME                      CHAR( 18),
LASTSUCCESS                     TIMESTAMP,
SYNCHPOINT                      VARCHAR( 16) FOR BIT DATA,
SYNCHTIME                       TIMESTAMP,
CAPTURE_SCHEMA                  VARCHAR(128) NOT NULL,
TGT_CAPTURE_SCHEMA              VARCHAR(128),
FEDERATED_SRC_SRVR              VARCHAR( 18),
FEDERATED_TGT_SRVR              VARCHAR( 18),
JRN_LIB                         CHAR( 10),
JRN_NAME                        CHAR( 10),
OPTION_FLAGS                    CHAR(  4) NOT NULL,
COMMIT_COUNT                    SMALLINT,
MAX_SYNCH_MINUTES               SMALLINT,
AUX_STMTS                       SMALLINT NOT NULL,
ARCH_LEVEL                      CHAR( 4) NOT NULL)
ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_SUBS_SETX
ON ASNCDC.IBMSNAP_SUBS_SET(
APPLY_QUAL                      ASC,
SET_NAME                        ASC,
WHOS_ON_FIRST                   ASC);

ALTER TABLE ASNCDC.IBMSNAP_SUBS_SET VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_SUBS_STMTS(
APPLY_QUAL                      CHAR( 18) NOT NULL,
SET_NAME                        CHAR( 18) NOT NULL,
WHOS_ON_FIRST                   CHAR(  1) NOT NULL,
BEFORE_OR_AFTER                 CHAR(  1) NOT NULL,
STMT_NUMBER                     SMALLINT NOT NULL,
EI_OR_CALL                      CHAR(  1) NOT NULL,
SQL_STMT                        VARCHAR(1024),
ACCEPT_SQLSTATES                VARCHAR( 50))
ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_SUBS_STMTSX
ON ASNCDC.IBMSNAP_SUBS_STMTS(
APPLY_QUAL                      ASC,
SET_NAME                        ASC,
WHOS_ON_FIRST                   ASC,
BEFORE_OR_AFTER                 ASC,
STMT_NUMBER                     ASC);

ALTER TABLE ASNCDC.IBMSNAP_SUBS_STMTS VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_UOW(
IBMSNAP_UOWID                   CHAR( 10) FOR BIT DATA NOT NULL,
IBMSNAP_COMMITSEQ               VARCHAR( 16) FOR BIT DATA NOT NULL,
IBMSNAP_LOGMARKER               TIMESTAMP NOT NULL,
IBMSNAP_AUTHTKN                 VARCHAR(30) NOT NULL,
IBMSNAP_AUTHID                  VARCHAR(128) NOT NULL,
IBMSNAP_REJ_CODE                CHAR(  1) NOT NULL WITH DEFAULT ,
IBMSNAP_APPLY_QUAL              CHAR( 18) NOT NULL WITH DEFAULT )
	ORGANIZE BY ROW;

CREATE UNIQUE INDEX ASNCDC.IBMSNAP_UOWX
ON ASNCDC.IBMSNAP_UOW(
IBMSNAP_COMMITSEQ               ASC,
IBMSNAP_LOGMARKER               ASC);

ALTER TABLE ASNCDC.IBMSNAP_UOW VOLATILE CARDINALITY;

CREATE TABLE ASNCDC.IBMSNAP_CAPENQ (
		LOCK_NAME CHAR(9 OCTETS)
	)
	ORGANIZE BY ROW
	DATA CAPTURE NONE 
	COMPRESS NO;
