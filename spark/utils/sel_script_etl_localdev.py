from abc import abstractmethod
import logging
from argparse import ArgumentParser
from datetime import datetime
from de.telekom.bdmp.rai.transformation.cfm_selection.managed_etl_localdev import AMngdEtl
from pyspark.sql.functions import lit, col, concat, row_number, desc
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


# pylint: disable=W0223
# W0223 : not overridden (abstract-method)
class ASelScriptEtl(AMngdEtl):

    # default time delta days for sel scripts
    TIME_DELTA_DAYS = 1

    def __init__(self, argv, spark_app_name="run_managed_etl_script"):

        logger.info("ASelScriptEtl.__init__")
        AMngdEtl.__init__(self, argv, spark_app_name)

        # Output write (True/False)
        self._output_write = self.args.output_write
        logger.info("output write: %s.", str(self._output_write))

        # Debug (True/False)
        self._debug = self.args.debug
        logger.info("debug: %s.", str(self._debug))

    @abstractmethod
    def get_config(self, config_name: str) -> None:
        raise NotImplementedError("Method not implemented yet 'get_config(self, config_name)'")

    def add_specific_args(self, parser: ArgumentParser):
        """
        Add specific arguments for the application:
            -output_write  (--output_write)  - If to write the outpu
            -debug (--debug) - If to debug

        :param parser: Argument parser for cmd parameters
        """
        parser.add_argument(
            "-output_write",
            "--output_write",
            required=True,
            action="store",
            dest="output_write",
            help="True if the output dataframe should be written, False otherwise",
        )

        parser.add_argument(
            "-debug",
            "--debug",
            required=True,
            action="store",
            dest="debug",
            help="For debugginf purposes set this to True to turn off some of the method runs",
        )

        parser.add_argument(
            "-config_name",
            "--config_name",
            required=False,
            action="store",
            dest="config_name",
            help="Set config_name if used on cdsw",
        )

    @staticmethod
    def _show_results(df, survey, mode, partition2, tbl_name) -> None:

        cnt = df.count()
        logger.info("%d rows %s on %s", cnt, mode, tbl_name)

        out = f"""
        #Survey: {survey}
        #Amount: {cnt}
        #From  : {partition2}
        #Mode  : {mode} on {tbl_name}
        #Status: Completed
        """
        print(out)

    @staticmethod
    def generate_odl_id(df, survey_nr, partition2=datetime.today().strftime("%Y%m%d")):

        # generate studiennummer column
        df = df.withColumn("studiennummer", lit(survey_nr))

        # create window over studiennummer
        sorting = [desc(c) for c in df.columns]
        w = Window.orderBy(*sorting)

        # add row number
        df = df.withColumn("nr", row_number().over(w))

        # build odl_id
        df = df.withColumn("odl_id", concat(lit("1"), col("studiennummer"), lit(partition2), col("nr") + 100000))

        return df
