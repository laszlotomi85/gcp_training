# pylint: disable=C0116,E0401,C0411,C0413,C0412
# E0401: Unable to import 'impala.dbapi' (import-error)
import os
import sys
import logging
import time
from abc import abstractmethod
from argparse import ArgumentParser, RawDescriptionHelpFormatter

# from impala.dbapi import connect # because of invalidate metadt
import pyspark.sql.functions as funcs
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

SPARK_CONFIG = {
    "spark.executor.memory": "20g",
    "spark.executor.cores": "4",
    "spark.kryoserializer.buffer.max": "2047m",
    "spark.dynamicAllocation.maxExecutors": "10",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.driver.memory": "20g",
    "spark.driver.cores": "3",
    "spark.ui.showConsoleProgress": "false",
    "spark.dynamicAllocation.enabled": True,
    "spark.shuffle.service.enabled": True,
    "spark.sql.cbo.enabled": True,
    "spark.executor.memoryOverhead": "600",
    "spark.rdd.compress": "true",
    "spark.storage.memoryFraction": "1",
    "spark.core.connection.ack.wait.timeout": "600",
    "spark.driver.maxResultSize": "0",
    "spark.task.maxFailures": "100",
    "spark.shuffle.io.maxRetries": "20",
    "spark.sql.session.timeZone": "Europe/Berlin",
    "spark.sql.parquet.int96TimestampConversion": "true",
    "spark.ui.enabled": True,
    "spark.hadoop.hive.exec.dynamic.partition": True,
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation": "true",
}


class AMngdEtl:

    LOG_FORMAT = "%(asctime)s - %(message)s"

    def __init__(self, argv, spark_app_name="run_managed_etl_script"):

        logger.info("AMngdEtl.__init__")
        # BdmpMain.__init__(self, argv)
        if argv is not None:
            # remove first argument and
            # bugfix: remove args like "--exec-env=JAVA" and "--log-level=DEBUG"
            self.argv = []
            for arg in argv[1:]:
                if (not arg.startswith("--exec-env=")) and (not arg.startswith("--log-level=")):
                    self.argv.append(arg)
                else:
                    logger.warning("Cli arg removed: %s", arg)

            self.args = self.parse_args()

        logger.info("CL parameters used to run: %s", " ".join(argv))

        self.__config = {}
        self.__get_config()

        if "custom_spark_config" in self.__config:
            SPARK_CONFIG.update(self.__config["custom_spark_config"])

        logger.info("Obtaining spark session")
        if os.getenv("CHASSIS", "") != 'Notebook':
            my_builder = SparkSession.builder.appName(spark_app_name)
            for key, value in SPARK_CONFIG.items():
                my_builder = my_builder.config(key, value)
            self.spark = my_builder.enableHiveSupport().getOrCreate()
        elif os.getenv("CHASSIS", "") == 'Notebook':
            jar_path = os.path.join(os.environ["JDBC_DRIVER_PATH"], "mysql-connector-java-8.0.30.jar")
            self.spark = SparkSession \
                .builder \
                .config("spark.jars", jar_path) \
                .master("local").appName("PySpark MySQL localhost") \
                .getOrCreate()
            self.spark.sparkContext.setLogLevel("ERROR")
            logger.info("Obtaining spark session in NOTEBOOK mode finished")
        logger.info("Obtaining spark session DONE")

        logger.info("spark_session config:\n\t%s", ",\n\t".join([f"{e[0]}: {e[1]}" for e in self.spark.sparkContext.getConf().getAll()]))

    def __get_config(self):
        config_name: str = None
        if "cdsw" in os.getcwd() or os.getenv("CHASSIS", "") == 'Notebook':
            assert self.args.config_name is not None, "'--config_name' parameter must be set if running on CDSW!"
            config_name = self.args.config_name
            assert config_name is not None and config_name != "", "'config_name' parameter can not be empty when running on CDSW!"
        else:
            config_name = os.getenv("BDMP_ENV")
            assert config_name is not None and config_name != "", "'BDMP_ENV' environment can not be empty!"

        self.get_config(config_name)

    @staticmethod
    def is_logical_env() -> bool:
        if os.getenv("BDMP_ENV") in ["d200", "d201"]:
            return True

        return False

    @abstractmethod
    def get_config(self, config_name: str) -> None:
        raise NotImplementedError("Method not implemented yet 'get_config(self, config_name)'")

    def parse_args(self):
        """
        Parse arguments and do some actions on system defined parameters
        """

        program_name = os.path.basename(self.argv[0])
        program_shortdesc = __import__("__main__").__doc__

        try:
            # Setup argument parser
            parser = ArgumentParser(description=program_shortdesc, formatter_class=RawDescriptionHelpFormatter)

            self.add_specific_args(parser)
            logger.info("Arguments to parse: %s", self.argv)

            # Process arguments
            args = parser.parse_args(self.argv)

            return args

        except KeyboardInterrupt:
            # handle keyboard interrupt
            sys.exit(0)

        except Exception as e:
            indent = len(program_name) * " "
            logger.error("%s: %s", program_name, repr(e))
            logger.error("%s  for help use --help", indent)
            sys.exit(2)

    def table_reader(self, location):

        schema, table = location.split(".")
        try:
            if os.getenv("CHASSIS", "") != "Notebook":
                try:
                    output_df = self.spark.read.table(location)
                except Exception:
                    logger.error("Table %s doesn't exist.", ".".join([schema, table]))
                    sys.exit(1)
            else:
                try:
                    output_df = self.spark.read \
                        .format("jdbc") \
                        .option("url", f"jdbc:mysql://localhost:3306/{schema}") \
                        .option("driver", "com.mysql.cj.jdbc.Driver") \
                        .option("dbtable", table) \
                        .option("user", "root") \
                        .option("password", "root") \
                        .load()
                except Exception:
                    logger.error("Table %s doesn't exist.", ".".join([schema, table]))
                    sys.exit(1)
            return output_df
        except Exception:
            logger.error("Couldn't connect MySQL database.")
            sys.exit(1)

    @abstractmethod
    def run(self):
        """
        Main method of the executable application
        """
        return

    @abstractmethod
    def add_specific_args(self, parser):
        """
        Overwrite this method to add more arguments to the program
        """
        return

    def write_data(
        self, df_all, output_db_table="db_prd_rai_pwr.tmp", partitions=None, write=False, mode="overwrite_or_append", truncate=False, DEBUG=False
    ):
        # pylint: disable=R0912,R0915
        """
        method to write dataframe into database.
        DEBUG is passed for possible debugging.
        """
        logger.debug("Debugging mode: %s", str(DEBUG))

        try:
            df_in_database = self.spark.sql(f"select * from {output_db_table} limit 5")
            df_cols_l, db_cols_l = [c.lower() for c in df_all.columns], [c.lower() for c in df_in_database.columns]
            missing_cols_list = [i for i in db_cols_l if i not in df_cols_l]
            extra_cols_list = [i for i in df_cols_l if i not in db_cols_l]

            logger.info(
                """
                      missing columns from current dataframe which were in the stored table:
                      %s,
                      they will be replaced by Null values.
                """,
                missing_cols_list,
            )
            logger.info(
                """
                      extra columns from current dataframe which were not in the stored table:
                      %s,
                      add them to Innovator if you need them!
                """,
                extra_cols_list,
            )

            # Removing column from partitions list if it is not found in the database table
            logger.info(
                """Removed partitioning columns:
                        %s
                """,
                [i for i in partitions if i in extra_cols_list],
            )
            partitions = [i for i in partitions if i not in extra_cols_list]

            # adding missing columns so that we can insert the rows to the table
            for ccol in missing_cols_list:
                df_all = df_all.withColumn(ccol, funcs.lit(""))
            df_all = df_all.select(df_in_database.columns)
            logger.info("df_all:\n%s", df_all)
        except Exception as e:
            logger.error("%s %s in write data", type(e), e)
            logger.error("----- OutputTable not found. This should never happen in managed area. In .pwr we recreate the table")

        if write:
            if os.getenv("CHASSIS", "") == "Notebook":
                schema, table = output_db_table.split(".")
                df_all.write \
                    .format("jdbc") \
                    .option("url", f"jdbc:mysql://localhost:3306/{schema}") \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("dbtable", table) \
                    .option("user", "root") \
                    .option("password", "root") \
                    .mode(mode)\
                    .save()
            else:
                st_time = time.time()
                if partitions is not None and len(partitions) > 0:
                    df_all = df_all.repartition(*partitions)
                    df_all.write.partitionBy(*partitions).mode(mode).saveAsTable(output_db_table)
                truncate_was_succesful = False
                if truncate:
                    try:
                        self.spark.sql(f"TRUNCATE TABLE {output_db_table}")
                        truncate_was_succesful = True
                        logger.info("----------------------------------------- TRUNCATE was successful")
                    except Exception as e:
                        truncate_was_succesful = False
                        logger.error("----------------------------------------- TRUNCATE not successful (should not happen in managed but on cdsw)")
                        logger.error("truncate error message type: %s - %s", type(e), e)

                    try:
                        if truncate_was_succesful:
                            errormessage = "Writing/appending was not succesful!!!"
                            df_all.write.mode("append").insertInto(output_db_table)
                            logger.info("----------------------------------------- append was successful after truncation")
                        elif mode == "overwrite" or truncate:
                            errormessage = "Writing/appending was not succesful!!!"
                            df_all.write.partitionBy(*partitions).format("hive").mode("overwrite").saveAsTable(output_db_table)
                            logger.info("--------------------- overwrite was successful (should not happen in managed but on cdsw)")
                        elif mode == "append":
                            errormessage = "Writing/appending was not succesful!!!"
                            df_all.write.partitionBy(*partitions).format("hive").mode("append").saveAsTable(output_db_table)
                            logger.info("----------------------------------------- append was successful without truncation")
                        else:
                            errormessage = "invalid options in write, fix them!"
                            raise Exception(errormessage)
                        logger.info("%s is written in %s seconds, partitions was %s", output_db_table, str(time.time() - st_time), partitions)
                    except Exception as e:
                        if truncate_was_succesful and truncate:
                            logger.error("----------------------------------------- append was not successful after truncation!")
                        elif mode == "overwrite":
                            logger.error("----------------------------------------- overwrite was not successful")
                        elif mode == "append":
                            logger.error("----------------------------------------- append was not successful without truncation!")
                        else:
                            logger.error("something went wrong unexpectedly, check the settings")
                        logger.error("error message type: %s error message is: %s", type(e), e)
                        raise Exception from e
        else:
            logger.error("write is off, no data written")

    @staticmethod
    def get_impala_host() -> str:
        return os.getenv("BDMP_IMPALASERVER_HOST") if os.getenv("BDMP_IMPALASERVER_HOST") else "odl-gn02.odl.telekom.de"

    @staticmethod
    def get_impala_port() -> int:
        return int(os.getenv("BDMP_IMPALASERVER_PORT")) if os.getenv("BDMP_IMPALASERVER_PORT") else 21050

    def _refresh(self, tbl):
        # pylint: disable=W0702
        """
        Refresh a table with spark and gReturn False if not possible
        """
        try:
            self.spark.catalog.refreshTable(tbl)
            return True

        except:
            print(f"unable to refresh {tbl} in spark catalog")
            return False

    def invalidate_metadt(self, tab_list):
        # imp_con = None
        # imp_cur = None
        # try:
        #     if "cdsw" in os.getcwd():
        #         # may be it works on cdsw without user (?) then we wouldn't need this if statement
        #         imp_con = connect(
        #             host=self.get_impala_host(),
        #             port=self.get_impala_port(),
        #             use_ssl=True,
        #             user=os.environ.get("HADOOP_USER_NAME"),
        #             kerberos_service_name="impala",
        #             auth_mechanism="GSSAPI",
        #         )
        #     else:
        #         imp_con = connect(host=self.get_impala_host(), port=self.get_impala_port(), use_ssl=True, auth_mechanism="GSSAPI")
        #     imp_cur = imp_con.cursor()
        #
        #     for tab_name in tab_list:
        #         tab_name = tab_name[0]  # because we save both name and DF as a tuple!
        #         logger.info("invalidating %s", tab_name)
        #         imp_cur.execute(f"invalidate metadata {tab_name}")
        #         imp_cur.execute(f"compute stats {tab_name}")
        # except Exception as e:
        #     logger.warning("Unexpected exception during invalidate metadata: %s", e)
        # finally:
        #     if imp_cur is not None:
        #         try:
        #             imp_cur.close()
        #         except Exception:
        #             pass
        #     if imp_con is not None:
        #         try:
        #             imp_con.close()
        #         except Exception:
        #             pass
        logger.warning("invalidate_metadt not implemented: %s", tab_list)
