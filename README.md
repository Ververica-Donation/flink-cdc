# CDC Connectors for Apache Flink<sup>®</sup>

CDC Connectors for Apache Flink<sup>®</sup> is a set of source connectors for Apache Flink<sup>®</sup>, ingesting changes from different databases using change data capture (CDC).
CDC Connectors for Apache Flink<sup>®</sup> integrates Debezium as the engine to capture data changes. So it can fully leverage the ability of Debezium. See more about what is [Debezium](https://github.com/debezium/debezium).

This README is meant as a brief walkthrough on the core features of CDC Connectors for Apache Flink<sup>®</sup>. For a fully detailed documentation, please see [Documentation](https://ververica.github.io/flink-cdc-connectors/master/).

## Supported (Tested) Databases

| Connector                                                  | Database                                                                                                                                                                                                                                                                                                                                                                                               | Driver                    |
|------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| [mongodb-cdc](docs/content/connectors/mongodb-cdc.md)      | <li> [MongoDB](https://www.mongodb.com): 3.6, 4.x, 5.0, 6.0                                                                                                                                                                                                                                                                                                                                            | MongoDB Driver: 4.3.4     |
| [mysql-cdc](docs/content/connectors/mysql-cdc.md)          | <li> [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x <li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x <li> [PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x <li> [Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x <li> [MariaDB](https://mariadb.org): 10.x <li> [PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | JDBC Driver: 8.0.28       |
| [oceanbase-cdc](/docs/content/connectors/oceanbase-cdc.md) | <li> [OceanBase CE](https://open.oceanbase.com): 3.1.x, 4.x <li> [OceanBase EE](https://www.oceanbase.com/product/oceanbase): 2.x, 3.x, 4.x                                                                                                                                                                                                                                                            | OceanBase Driver: 2.4.x   |
| [oracle-cdc](docs/content/connectors/oracle-cdc.md)        | <li> [Oracle](https://www.oracle.com/index.html): 11, 12, 19, 21                                                                                                                                                                                                                                                                                                                                       | Oracle Driver: 19.3.0.0   |
| [postgres-cdc](docs/content/connectors/postgres-cdc.md)    | <li> [PostgreSQL](https://www.postgresql.org): 9.6, 10, 11, 12, 13, 14                                                                                                                                                                                                                                                                                                                                 | JDBC Driver: 42.5.1       |
| [sqlserver-cdc](docs/content/connectors/sqlserver-cdc.md)  | <li> [Sqlserver](https://www.microsoft.com/sql-server): 2012, 2014, 2016, 2017, 2019                                                                                                                                                                                                                                                                                                                   | JDBC Driver: 9.4.1.jre8   |
| [tidb-cdc](docs/content/connectors/tidb-cdc.md)            | <li> [TiDB](https://www.pingcap.com): 5.1.x, 5.2.x, 5.3.x, 5.4.x, 6.0.0                                                                                                                                                                                                                                                                                                                                | JDBC Driver: 8.0.27       |
| [Db2-cdc](docs/content/connectors/db2-cdc.md)              | <li> [Db2](https://www.ibm.com/products/db2): 11.5                                                                                                                                                                                                                                                                                                                                                     | Db2 Driver: 11.5.0.0      |
| [Vitess-cdc](docs/content/connectors/vitess-cdc.md)        | <li> [Vitess](https://vitess.io/): 8.0.x, 9.0.x                                                                                                                                                                                                                                                                                                                                                        | MySql JDBC Driver: 8.0.26 |

## Features

1. Supports reading database snapshot and continues to read transaction logs with **exactly-once processing** even failures happen.
2. CDC connectors for DataStream API, users can consume changes on multiple databases and tables in a single job without Debezium and Kafka deployed.
3. CDC connectors for Table/SQL API, users can use SQL DDL to create a CDC source to monitor changes on a single table.

## Quick Start

The example shows how to continuously synchronize data, including snapshot data and incremental data, from multiple business tables in MySQL database to Doris for creating the ODS layer.

1. Install Flink CDC and required dependencies.
   1. Download and extract the flink-cdc-3.0.tar file to a local directory.
   2. Download the required CDC Pipeline Connector JAR from Maven and place it in the lib directory (automated pulling using a script can be optimized in the future).
   3. Configure the FLINK_HOME environment variable to load the Flink cluster configuration from the flink-conf.yaml file located in the $FLINK_HOME/conf directory.
2. Write Flink CDC task YAML. 
```yaml
source:
  type: mysql
  host: localhost
  port: 3306
  username: admin
  password: pass
  table: adb.*, bdb.user_table_[0-9]+, [app|web]_order_.*

sink:
  type: doris
  fenodes: FE_IP:HTTP_PORT
  username: admin
  password: pass

pipeline:
  name: mysql-sync-doris
  parallelism: 4
```
3. Submit the job to Flink cluster.
```bash
# Submit Pipeline
$ ./bin/flink-cdc.sh mysql-to-doris.yaml
Pipeline "mysql-sync-kafka" is submitted with Job ID "DEADBEEF".
```

During the execution of the flink-cdc.sh script, the CDC task configuration is parsed and translated into a DataStream job, which is then submitted to the specified Flink cluster.

## Building from source

- Prerequisites:
    - git
    - Maven
    - At least Java 8

```
git clone https://github.com/ververica/flink-cdc-connectors.git
cd flink-cdc-connectors
mvn clean install -DskipTests
```

The dependencies are now available in your local `.m2` repository.

## License

The code in this repository is licensed under the [Apache Software License 2](https://github.com/ververica/flink-cdc-connectors/blob/master/LICENSE).

## Contributing

CDC Connectors for Apache Flink<sup>®</sup> welcomes anyone that wants to help out in any way, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. You can report problems to request features in the [GitHub Issues](https://github.com/ververica/flink-cdc-connectors/issues).

### Code Contribute

1. Left comment under the issue that you want to take
2. Fork Flink CDC project to your GitHub repositories
3. Clone and compile your Flink CDC project
```bash
git clone https://github.com/your_name/flink-cdc-connectors.git
cd flink-cdc-connectors
mvn clean install -DskipTests
```
4. Check to a new branch and start your work
```bash
git checkout -b my_feature
-- develop and commit
```
5. Push your branch to your github
```bash
git push origin my_feature
```
6. Open a PR to https://github.com/ververica/flink-cdc-connectors

### Code Style

#### Code Formatting

You need to install the google-java-format plugin. Spotless together with google-java-format is used to format the codes.

It is recommended to automatically format your code by applying the following settings:

1. Go to "Settings" → "Other Settings" → "google-java-format Settings".
2. Tick the checkbox to enable the plugin.
3. Change the code style to "Android Open Source Project (AOSP) style".
4. Go to "Settings" → "Tools" → "Actions on Save".
5. Under "Formatting Actions", select "Optimize imports" and "Reformat file".
6. From the "All file types list" next to "Reformat code", select "Java".

For earlier IntelliJ IDEA versions, the step 4 to 7 will be changed as follows.

- 4.Go to "Settings" → "Other Settings" → "Save Actions".
- 5.Under "General", enable your preferred settings for when to format the code, e.g. "Activate save actions on save".
- 6.Under "Formatting Actions", select "Optimize imports" and "Reformat file".
- 7.Under "File Path Inclusions", add an entry for `.*\.java` to avoid formatting other file types.
Then the whole project could be formatted by command `mvn spotless:apply`.

#### Checkstyle

Checkstyle is used to enforce static coding guidelines.

1. Go to "Settings" → "Tools" → "Checkstyle".
2. Set "Scan Scope" to "Only Java sources (including tests)".
3. For "Checkstyle Version" select "8.14".
4. Under "Configuration File" click the "+" icon to add a new configuration.
5. Set "Description" to "Flink cdc".
6. Select "Use a local Checkstyle file" and link it to the file `tools/maven/checkstyle.xml` which is located within your cloned repository.
7. Select "Store relative to project location" and click "Next".
8. Configure the property `checkstyle.suppressions.file` with the value `suppressions.xml` and click "Next".
9. Click "Finish".
10. Select "Flink cdc" as the only active configuration file and click "Apply".

You can now import the Checkstyle configuration for the Java code formatter.

1. Go to "Settings" → "Editor" → "Code Style" → "Java".
2. Click the gear icon next to "Scheme" and select "Import Scheme" → "Checkstyle Configuration".
3. Navigate to and select `tools/maven/checkstyle.xml` located within your cloned repository.

Then you could click "View" → "Tool Windows" → "Checkstyle" and find the "Check Module" button in the opened tool window to validate checkstyle. Or you can use the command `mvn clean compile checkstyle:checkstyle` to validate.

### Documentation Contribute

Flink cdc documentations locates at `docs/content`.

The contribution step is the same as the code contribution. We use markdown as the source code of the document.

## Community

* [DingTalk](https://www.dingtalk.com/) Chinese User Group

  You can search the group number [**33121212**] or scan the following QR code to join in the group.
  
  <div align=center>
     <img src="https://user-images.githubusercontent.com/5163645/233297896-0195d0ae-eb1c-4604-977b-1d08e424c7e7.png" width=400 />
   </div>

## Documents
To get started, please see https://ververica.github.io/flink-cdc-connectors/
