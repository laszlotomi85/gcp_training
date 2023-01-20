#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0201,C0206,R0902,E0401
import sys
import os
import logging
import time
# from datetime import datetime
from dateutil.relativedelta import relativedelta

import string
import random
from random import randint
import datetime

import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import IntegerType

# from impala_shell_connector import ImpalaShell
from gcp_training.spark.utils.sel_script_etl_localdev import ASelScriptEtl
from gcp_training.spark.utils.sel_script_etl_localdev.cfg.config_4100 import config

logger = logging.getLogger(__name__)

# GLOBAL VARIABLES
# List of tables used for querying


class MakeDummyData(ASelScriptEtl):

    def __init__(self, argv):
        ASelScriptEtl.__init__(self, argv, spark_app_name="run_CfmSelectionCHAT")
        self.test_data_dict = {"1": {"table" : "db_prd_cj_base.al_cfm_ergebnisdaten_mt",
                                     "id_col": {"irrp_interaction_id_ps": "interac",},
                                     "fixes": {"studiennummer": ["4100", "4201", "4202"],
                                               "odl_id_ps": None}
                                     },
                               "2": {"table" : "db_prd_cj_base.al_irrp_chat_metadata_mt",
                                     "id_col": {"interaction_id_ps": "interac",
                                                "eun_uni_id_ps": "sdom"},
                                     "fixes": {"media_type": ["Android", "iOS"]}
                                     },
                               "3": {"table" : "db_prd_gsa_base_nws.al_sdom_unit_mt",
                                     "id_col": {"uni_id_ps": "sdom",
                                                "uni_uni_id_ps": "sdom_uni"},
                                     "fixes": {"acl_dop": ["CHECK", "THIS", "OUT"],
                                               "uni_name": ["SOME", "SDOM", "ATTRIBUTES"],
                                               "bdmp_partstamp": ["202211"],
                                               "uni_valid_to": ["2050-01-01 00:00:00"],
                                               "uni_unt_id": [1, 2, 3, 4]}
                                     },
                               }

        self.sample_size = 10
        self.specified_id_generating()

        self.write = self._output_write in ("True", "true", "TRUE")
        self.DEBUG = self._debug in ("True", "true", "TRUE")

        self.counter = 0

    def get_config(self, config_name: str) -> None:
        assert config_name in config, f"config_name is not defined under configs: '{config_name}'!"

        self.__config = config[config_name]

        logger.debug("self.__config: %s", self.__config)

        survey = "TEXT_CHAT_4100"

        cfg_item = self.__config["cfg"][survey]

        for key in ["survey_type", "survey_nr", "table_name", "mode"]:
            logger.info("Checking %s survey parameter %s", survey, key)
            assert key in cfg_item, f"Configuration error '{key}' is missing under 'cfg'!"

        cfg_item["survey"] = survey

        # pylint: disable=W0201
        # W0201: attribute-defined-outside-init, get_config is the implementation of the abstract method in ASelScriptEtl
        self.__cfg_item = cfg_item

    def specified_id_generating(self, ):
        id_str = [val['id_col'] for val in self.test_data_dict.values()]
        runing_nr = [str(i) for i in range(self.sample_size)]
        self.str_dic = {}

        for l in id_str:
            random.seed()
            random.shuffle(runing_nr)
            for k,v in l.items():
                self.str_dic[v] = [v+"_"+datetime.datetime.now().strftime("%Y%m%d%H%M")+"_"+runner for runner in runing_nr]

    def random_string_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        # Returns a random string in <size> digit long
        return ''.join(random.choice(chars) for _ in range(size))

    def _dummy_string(self):
        string = self.random_string_generator(10)
        return string

    def random_number_generator(self, size=6, chars=string.digits):
        # Returns a random number in <size> digit long
        return ''.join(random.choice(chars) for _ in range(size))

    def _dummy_integer(self):
        # calls random number generator function
        integer = self.random_number_generator(3)
        return int(integer)

    def _dummy_long(self):
        # calls random number generator function
        long = self.random_number_generator(6)
        return int(long)

    def _dummy_date(self):
        #returns a random timestamp from the last 7 days
        startdate=datetime.datetime.now()
        date=startdate-datetime.timedelta(randint(100,700)/100)
        return date

    def _dummy_id(self, str_part):

        str_part = str_part
        day = datetime.datetime.now().strftime("%Y%m%d")

        new_id = "_".join([str_part, day, str(self.counter)])
        # self.counter +=1
        return new_id

    def _data_allocator(self, type_part):
        input, id_str_part = type_part
        if id_str_part is None:
            if input == "string":
                data = self._dummy_string()
            elif input == "integer":
                data = self._dummy_integer()
            elif input == "long":
                data = self._dummy_long()
            elif input == "timestamp":
                data = self._dummy_date()
        else:
            data = self._dummy_id(id_str_part)
        return data

    def record_multiplier(self, row_count, dtype_lista, fix_val, id_str_part):
        data = []
        for i in range(row_count):
            if fix_val is None:
                if id_str_part is None:
                    id_str_part = [None]*len(dtype_lista)
                    new_row = [self._data_allocator(type_part) for type_part in zip(dtype_lista,id_str_part)]
                else:
                    new_row = [self._data_allocator(type_part) for type_part in zip(dtype_lista,id_str_part)]
            else:
                new_row = list([random.choice(i) if type(i) == list else i for i in fix_val])
            data.append(new_row)

        return data

    def dtype_parser(self, schema):
        return [col["type"] for col in schema.jsonValue()["fields"] if col["name"]]

    def cols_to_list(self, schema):
        return [col["name"] for col in schema.jsonValue()["fields"] ]

    def df_generator(self, schema, nr_of_rec, order_col, fix_val=None, id_str_part = None):
        dtype_list_cont = self.dtype_parser(schema)
        my_dummy_datas = self.record_multiplier(nr_of_rec, dtype_list_cont, fix_val ,id_str_part)

        my_df = self.spark.createDataFrame(data= my_dummy_datas,
                                                    schema = schema) \
            .withColumn("row_number",
                        row_number().over(self.windowSpec(order_col)))
        return my_df

    def windowSpec(self, col):
        return Window.orderBy(col)

    def make_id_df(self, tab, schema, order_col):
        str_dic_filtered={}
        for key, val in self.test_data_dict[tab]["id_col"].items():
            str_dic_filtered[key] = self.str_dic[val]

        my_prepared_ids=[]
        for a in zip(*list(str_dic_filtered.values())):
            my_prepared_ids.append(a)

        my_df = self.spark.createDataFrame(data= my_prepared_ids,
                                           schema = schema) \
            .withColumn("row_number",
                        row_number().over(self.windowSpec(order_col[0])))

        return my_df


if __name__ == '__main__':
    # Initiate classes - spark, selector class
    # impala = ImpalaShell()
    logging.basicConfig(level=logging.INFO, format=ASelScriptEtl.LOG_FORMAT)
    print(sys.argv)
    selector = MakeDummyData(sys.argv)
    # Execute ETL process with running selector script
    table_dict = selector.test_data_dict
    nr_of_rec = selector.sample_size

    for tab in table_dict.keys():
        # Encode config table_dict
        pk_id_col = list(table_dict[tab]["id_col"].keys())
        id_str_part = list(table_dict[tab]["id_col"].values())
        table_name = table_dict[tab]["table"]
        try:
            fix_col = list(table_dict[tab]["fixes"].keys())
            droppers = pk_id_col+fix_col
        except:
            fix_col = ''
            droppers = pk_id_col

        # Make content part
        schema_content = selector.table_reader(table_name).drop(*droppers).schema
        my_data_df = selector.df_generator(schema_content, nr_of_rec, schema_content.fieldNames()[1] )

        # Make ID part
        schema_id = selector.table_reader(table_name).select(*pk_id_col).schema
        my_id_df = selector.make_id_df(tab, schema_id, pk_id_col)

        # Join content with ID
        new_dummy_df = my_id_df.join(my_data_df, "row_number", "inner")

        if fix_col!='':
            schema_fix = selector.table_reader(table_name).select(fix_col).schema
            my_fix_df = selector.df_generator(schema_fix, nr_of_rec, fix_col, fix_val = table_dict[tab]["fixes"].values())
            new_dummy_df = new_dummy_df.join(my_fix_df, "row_number", "inner")
        new_dummy_df = new_dummy_df.drop("row_number")
        selector.write_data(new_dummy_df,
                            table_name,
                            partitions=[schema_content.fieldNames()[-1]],
                            write=selector.write,
                            mode="append",
                            truncate=False,
                            DEBUG=selector.DEBUG)


else:
    logging.basicConfig(level=logging.ERROR, format=ASelScriptEtl.LOG_FORMAT)

#
# test_df_1 = selector.table_reader(selector.test_data_dict["1"]["table"]).select("irrp_interaction_id_ps")
# test_df_2 = selector.table_reader(selector.test_data_dict["2"]["table"]).select("interaction_id_ps",
#                                                                                 "eun_uni_id_ps",
#                                                                                 "media_type")
# test_df_3 = selector.table_reader(selector.test_data_dict["3"]["table"]).select("eun_uni_id_ps",
#                                                                                 "acl_dop")
#
# join_df = test_df_1.join(test_df_2,
#                          on=test_df_1.irrp_interaction_id_ps==test_df_2.interaction_id_ps,
#                          how="inner"
#                          )
# join_df_final = join_df.join(test_df_3,
#                              on=join_df.eun_uni_id_ps==test_df_3.eun_uni_id_ps,
#                              how="inner" )
# join_df_final.show(truncate=False)
