/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oceanbase.table;

import org.apache.flink.cdc.connectors.oceanbase.OceanBaseTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/** Integration tests for OceanBase MySQL mode table source. */
@RunWith(Parameterized.class)
public class OceanBaseMySQLModeITCase extends OceanBaseTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMySQLModeITCase.class);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    private static final String NETWORK_MODE = "host";
    private static final String OB_SYS_PASSWORD = "123456";

    @ClassRule
    public static final GenericContainer<?> OB_SERVER =
            new GenericContainer<>("oceanbase/oceanbase-ce:4.2.0.0")
                    .withNetworkMode(NETWORK_MODE)
                    .withEnv("MODE", "slim")
                    .withEnv("OB_ROOT_PASSWORD", OB_SYS_PASSWORD)
                    .withEnv("OB_DATAFILE_SIZE", "1G")
                    .withEnv("OB_LOG_DISK_SIZE", "4G")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("ddl/mysql/docker_init.sql"),
                            "/root/boot/init.d/init.sql")
                    .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                    .withStartupTimeout(Duration.ofMinutes(4))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final GenericContainer<?> LOG_PROXY =
            new GenericContainer<>("whhe/oblogproxy:1.1.3_4x")
                    .withNetworkMode(NETWORK_MODE)
                    .withEnv("OB_SYS_PASSWORD", OB_SYS_PASSWORD)
                    .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                    .withStartupTimeout(Duration.ofMinutes(1))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(OB_SERVER, LOG_PROXY)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        Stream.of(OB_SERVER, LOG_PROXY).forEach(GenericContainer::stop);
        LOG.info("Containers are stopped.");
    }

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    }

    private final String rsList;

    public OceanBaseMySQLModeITCase(
            String username,
            String password,
            String hostname,
            int port,
            String logProxyHost,
            int logProxyPort,
            String tenant,
            String rsList) {
        super("mysql", username, password, hostname, port, logProxyHost, logProxyPort, tenant);
        this.rsList = rsList;
    }

    @Parameterized.Parameters
    public static List<Object[]> parameters() {
        return Collections.singletonList(
                new Object[] {
                    "root@test",
                    "123456",
                    "127.0.0.1",
                    2881,
                    "127.0.0.1",
                    2983,
                    "test",
                    "127.0.0.1:2882:2881"
                });
    }

    @Override
    protected String logProxyOptionsString() {
        return super.logProxyOptionsString()
                + " , "
                + String.format(" 'rootserver-list' = '%s'", rsList);
    }

    @Override
    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:mysql://" + hostname + ":" + port + "/?useSSL=false", username, password);
    }

    @Test
    public void testTableList() throws Exception {
        initializeTable("inventory");

        String sourceDDL =
                String.format(
                        "CREATE TABLE ob_source ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") WITH ("
                                + initialOptionsString()
                                + ", "
                                + " 'table-list' = '%s'"
                                + ")",
                        "inventory.products");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '30'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM ob_source");

        waitForSinkSize("sink", 9);
        int snapshotSize = sinkSize("sink");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistant white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistant white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        waitForSinkSize("sink", snapshotSize + 7);

        /*
         * <pre>
         * The final database table looks like this:
         *
         * > SELECT * FROM products;
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | id  | name               | description                                             | weight |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | 101 | scooter            | Small 2-wheel scooter                                   |   3.14 |
         * | 102 | car battery        | 12V car battery                                         |    8.1 |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |    0.8 |
         * | 104 | hammer             | 12oz carpenter's hammer                                 |   0.75 |
         * | 105 | hammer             | 14oz carpenter's hammer                                 |  0.875 |
         * | 106 | hammer             | 18oz carpenter hammer                                   |      1 |
         * | 107 | rocks              | box of assorted rocks                                   |    5.1 |
         * | 108 | jacket             | water resistant black wind breaker                      |    0.1 |
         * | 109 | spare tire         | 24 inch spare tire                                      |   22.2 |
         * | 110 | jacket             | new water resistant white wind breaker                  |    0.5 |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * </pre>
         */

        List<String> expected =
                Arrays.asList(
                        "+I(101,scooter,Small 2-wheel scooter,3.1400000000)",
                        "+I(102,car battery,12V car battery,8.1000000000)",
                        "+I(103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000)",
                        "+I(104,hammer,12oz carpenter's hammer,0.7500000000)",
                        "+I(105,hammer,14oz carpenter's hammer,0.8750000000)",
                        "+I(106,hammer,16oz carpenter's hammer,1.0000000000)",
                        "+I(107,rocks,box of assorted rocks,5.3000000000)",
                        "+I(108,jacket,water resistant black wind breaker,0.1000000000)",
                        "+I(109,spare tire,24 inch spare tire,22.2000000000)",
                        "+U(106,hammer,18oz carpenter hammer,1.0000000000)",
                        "+U(107,rocks,box of assorted rocks,5.1000000000)",
                        "+I(110,jacket,water resistant white wind breaker,0.2000000000)",
                        "+I(111,scooter,Big 2-wheel scooter ,5.1800000000)",
                        "+U(110,jacket,new water resistant white wind breaker,0.5000000000)",
                        "+U(111,scooter,Big 2-wheel scooter ,5.1700000000)",
                        "-D(111,scooter,Big 2-wheel scooter ,5.1700000000)");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertContainsInAnyOrder(expected, actual);

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testMetadataColumns() throws Exception {
        initializeTable("inventory");

        String sourceDDL =
                String.format(
                        "CREATE TABLE ob_source ("
                                + " tenant STRING METADATA FROM 'tenant_name' VIRTUAL,"
                                + " database STRING METADATA FROM 'database_name' VIRTUAL,"
                                + " `table` STRING METADATA FROM 'table_name' VIRTUAL,"
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") WITH ("
                                + initialOptionsString()
                                + ","
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        "^inventory$",
                        "^products$");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " tenant STRING,"
                        + " database STRING,"
                        + " `table` STRING,"
                        + " `id` DECIMAL(20, 0) NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " primary key (tenant, database, `table`, `id`) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM ob_source");

        waitForSinkSize("sink", 9);
        int snapshotSize = sinkSize("sink");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
        }

        waitForSinkSize("sink", snapshotSize + 1);

        List<String> expected =
                Arrays.asList(
                        "+I("
                                + tenant
                                + ",inventory,products,101,scooter,Small 2-wheel scooter,3.1400000000)",
                        "+I("
                                + tenant
                                + ",inventory,products,102,car battery,12V car battery,8.1000000000)",
                        "+I("
                                + tenant
                                + ",inventory,products,103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000)",
                        "+I("
                                + tenant
                                + ",inventory,products,104,hammer,12oz carpenter's hammer,0.7500000000)",
                        "+I("
                                + tenant
                                + ",inventory,products,105,hammer,14oz carpenter's hammer,0.8750000000)",
                        "+I("
                                + tenant
                                + ",inventory,products,106,hammer,16oz carpenter's hammer,1.0000000000)",
                        "+I("
                                + tenant
                                + ",inventory,products,107,rocks,box of assorted rocks,5.3000000000)",
                        "+I("
                                + tenant
                                + ",inventory,products,108,jacket,water resistant black wind breaker,0.1000000000)",
                        "+I("
                                + tenant
                                + ",inventory,products,109,spare tire,24 inch spare tire,22.2000000000)",
                        "+U("
                                + tenant
                                + ",inventory,products,106,hammer,18oz carpenter hammer,1.0000000000)");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertContainsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testAllDataTypes() throws Exception {
        String serverTimeZone = "+00:00";
        setGlobalTimeZone(serverTimeZone);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of(serverTimeZone));

        initializeTable("column_type_test");
        String sourceDDL =
                String.format(
                        "CREATE TABLE ob_source (\n"
                                + "    `id` INT NOT NULL,\n"
                                + "    bit1_c BOOLEAN,\n"
                                + "    tiny1_c BOOLEAN,\n"
                                + "    boolean_c BOOLEAN,\n"
                                + "    tiny_c TINYINT,\n"
                                + "    tiny_un_c SMALLINT,\n"
                                + "    small_c SMALLINT ,\n"
                                + "    small_un_c INT ,\n"
                                + "    medium_c INT,\n"
                                + "    medium_un_c INT,\n"
                                + "    int11_c INT,\n"
                                + "    int_c INT,\n"
                                + "    int_un_c BIGINT,\n"
                                + "    big_c BIGINT,\n"
                                + "    big_un_c DECIMAL(20, 0),\n"
                                + "    real_c FLOAT,\n"
                                + "    float_c FLOAT,\n"
                                + "    double_c DOUBLE,\n"
                                + "    decimal_c DECIMAL(8, 4),\n"
                                + "    numeric_c DECIMAL(6, 0),\n"
                                + "    big_decimal_c STRING,\n"
                                + "    date_c DATE,\n"
                                + "    time_c TIME(0),\n"
                                + "    datetime3_c TIMESTAMP(3),\n"
                                + "    datetime6_c TIMESTAMP(6),\n"
                                + "    timestamp_c TIMESTAMP,\n"
                                + "    timestamp3_c TIMESTAMP(3),\n"
                                + "    timestamp6_c TIMESTAMP(6),\n"
                                + "    char_c CHAR(3),\n"
                                + "    varchar_c VARCHAR(255),\n"
                                + "    file_uuid BINARY(16),\n"
                                + "    bit_c BINARY(8),\n"
                                + "    text_c STRING,\n"
                                + "    tiny_blob_c BYTES,\n"
                                + "    medium_blob_c BYTES,\n"
                                + "    blob_c BYTES,\n"
                                + "    long_blob_c BYTES,\n"
                                + "    year_c INT,\n"
                                + "    set_c ARRAY<STRING>,\n"
                                + "    enum_c STRING,\n"
                                + "    json_c STRING,\n"
                                + "    primary key (`id`) not enforced"
                                + ") WITH ("
                                + initialOptionsString()
                                + ","
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = '%s'"
                                + ")",
                        "^column_type_test$",
                        "^full_types$",
                        serverTimeZone);
        String sinkDDL =
                "CREATE TABLE sink ("
                        + "    `id` INT NOT NULL,\n"
                        + "    bit1_c BOOLEAN,\n"
                        + "    tiny1_c BOOLEAN,\n"
                        + "    boolean_c BOOLEAN,\n"
                        + "    tiny_c TINYINT,\n"
                        + "    tiny_un_c SMALLINT,\n"
                        + "    small_c SMALLINT ,\n"
                        + "    small_un_c INT ,\n"
                        + "    medium_c INT,\n"
                        + "    medium_un_c INT,\n"
                        + "    int11_c INT,\n"
                        + "    int_c INT,\n"
                        + "    int_un_c BIGINT,\n"
                        + "    big_c BIGINT,\n"
                        + "    big_un_c DECIMAL(20, 0),\n"
                        + "    real_c FLOAT,\n"
                        + "    float_c FLOAT,\n"
                        + "    double_c DOUBLE,\n"
                        + "    decimal_c DECIMAL(8, 4),\n"
                        + "    numeric_c DECIMAL(6, 0),\n"
                        + "    big_decimal_c STRING,\n"
                        + "    date_c DATE,\n"
                        + "    time_c TIME(0),\n"
                        + "    datetime3_c TIMESTAMP(3),\n"
                        + "    datetime6_c TIMESTAMP(6),\n"
                        + "    timestamp_c TIMESTAMP,\n"
                        + "    timestamp3_c TIMESTAMP(3),\n"
                        + "    timestamp6_c TIMESTAMP(6),\n"
                        + "    char_c CHAR(3),\n"
                        + "    varchar_c VARCHAR(255),\n"
                        + "    file_uuid STRING,\n"
                        + "    bit_c BINARY(8),\n"
                        + "    text_c STRING,\n"
                        + "    tiny_blob_c BYTES,\n"
                        + "    medium_blob_c BYTES,\n"
                        + "    blob_c BYTES,\n"
                        + "    long_blob_c BYTES,\n"
                        + "    year_c INT,\n"
                        + "    set_c ARRAY<STRING>,\n"
                        + "    enum_c STRING,\n"
                        + "    json_c STRING,\n"
                        + "    primary key (`id`) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '3'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT id,\n"
                                + "bit1_c,\n"
                                + "tiny1_c,\n"
                                + "boolean_c,\n"
                                + "tiny_c,\n"
                                + "tiny_un_c,\n"
                                + "small_c ,\n"
                                + "small_un_c,\n"
                                + "medium_c,\n"
                                + "medium_un_c,\n"
                                + "int11_c,\n"
                                + "int_c,\n"
                                + "int_un_c,\n"
                                + "big_c,\n"
                                + "big_un_c,\n"
                                + "real_c,\n"
                                + "float_c,\n"
                                + "double_c,\n"
                                + "decimal_c,\n"
                                + "numeric_c,\n"
                                + "big_decimal_c,\n"
                                + "date_c,\n"
                                + "time_c,\n"
                                + "datetime3_c,\n"
                                + "datetime6_c,\n"
                                + "timestamp_c,\n"
                                + "timestamp3_c,\n"
                                + "timestamp6_c,\n"
                                + "char_c,\n"
                                + "varchar_c,\n"
                                + "TO_BASE64(DECODE(file_uuid, 'UTF-8')),\n"
                                + "bit_c,\n"
                                + "text_c,\n"
                                + "tiny_blob_c,\n"
                                + "medium_blob_c,\n"
                                + "blob_c,\n"
                                + "long_blob_c,\n"
                                + "year_c,\n"
                                + "set_c,\n"
                                + "enum_c,\n"
                                + "json_c\n"
                                + " FROM ob_source");

        waitForSinkSize("sink", 1);
        int snapshotSize = sinkSize("sink");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE column_type_test.full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }

        waitForSinkSize("sink", snapshotSize + 1);

        List<String> expected =
                Arrays.asList(
                        "+I(1,false,true,true,127,255,32767,65535,8388607,16777215,2147483647,2147483647,4294967295,9223372036854775807,18446744073709551615,123.102,123.102,404.4443,123.4567,346,34567892.1,2020-07-17,18:00:22,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17T18:00:22,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,abc,Hello World,ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=,[4, 4, 4, 4, 4, 4, 4, 4],text,[16],[16],[16],[16],2022,[a, b],red,{\"key1\": \"value1\"})",
                        "+U(1,false,true,true,127,255,32767,65535,8388607,16777215,2147483647,2147483647,4294967295,9223372036854775807,18446744073709551615,123.102,123.102,404.4443,123.4567,346,34567892.1,2020-07-17,18:00:22,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17T18:33:22,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,abc,Hello World,ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=,[4, 4, 4, 4, 4, 4, 4, 4],text,[16],[16],[16],[16],2022,[a, b],red,{\"key1\": \"value1\"})");

        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertContainsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testTimezoneBerlin() throws Exception {
        testTimeDataTypes("+02:00");
    }

    @Test
    public void testTimezoneShanghai() throws Exception {
        testTimeDataTypes("+08:00");
    }

    public void testTimeDataTypes(String serverTimeZone) throws Exception {
        setGlobalTimeZone(serverTimeZone);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of(serverTimeZone));

        initializeTable("column_type_test");
        String sourceDDL =
                String.format(
                        "CREATE TABLE ob_source (\n"
                                + "    `id` INT NOT NULL,\n"
                                + "    date_c DATE,\n"
                                + "    time_c TIME(0),\n"
                                + "    datetime3_c TIMESTAMP(3),\n"
                                + "    datetime6_c TIMESTAMP(6),\n"
                                + "    timestamp_c TIMESTAMP,\n"
                                + "    primary key (`id`) not enforced"
                                + ") WITH ("
                                + initialOptionsString()
                                + ","
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = '%s'"
                                + ")",
                        "column_type_test",
                        "full_types",
                        serverTimeZone);

        String sinkDDL =
                "CREATE TABLE sink ("
                        + "    `id` INT NOT NULL,\n"
                        + "    date_c DATE,\n"
                        + "    time_c TIME(0),\n"
                        + "    datetime3_c TIMESTAMP(3),\n"
                        + "    datetime6_c TIMESTAMP(6),\n"
                        + "    timestamp_c TIMESTAMP,\n"
                        + "    primary key (`id`) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT `id`, date_c, time_c, datetime3_c, datetime6_c, cast(timestamp_c as timestamp) FROM ob_source");

        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", 1);
        int snapshotSize = sinkSize("sink");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE column_type_test.full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }

        waitForSinkSize("sink", snapshotSize + 1);

        List<String> expected =
                Arrays.asList(
                        "+I(1,2020-07-17,18:00:22,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17T18:00:22)",
                        "+U(1,2020-07-17,18:00:22,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17T18:33:22)");

        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertContainsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testSnapshotOnly() throws Exception {
        initializeTable("inventory");

        String sourceDDL =
                String.format(
                        "CREATE TABLE ob_source ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") WITH ("
                                + snapshotOptionsString()
                                + ", "
                                + " 'table-list' = '%s'"
                                + ")",
                        "inventory.products");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '30'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM ob_source");

        waitForSinkSize("sink", 9);

        List<String> expected =
                Arrays.asList(
                        "+I(101,scooter,Small 2-wheel scooter,3.1400000000)",
                        "+I(102,car battery,12V car battery,8.1000000000)",
                        "+I(103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000)",
                        "+I(104,hammer,12oz carpenter's hammer,0.7500000000)",
                        "+I(105,hammer,14oz carpenter's hammer,0.8750000000)",
                        "+I(106,hammer,16oz carpenter's hammer,1.0000000000)",
                        "+I(107,rocks,box of assorted rocks,5.3000000000)",
                        "+I(108,jacket,water resistant black wind breaker,0.1000000000)",
                        "+I(109,spare tire,24 inch spare tire,22.2000000000)");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertContainsInAnyOrder(expected, actual);
    }
}
