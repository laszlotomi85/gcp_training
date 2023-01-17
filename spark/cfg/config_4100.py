# the environments 'prd', 'd200' and 'd201' must be defined
# all others belongs to cdsw environments

__custom_spark_config = {"spark.executor.cores": "1", "spark.driver.memory": "8g", "spark.driver.cores": "1"}

config = {
    "prd": {
        "custom_spark_config": __custom_spark_config,
        "cfg": {
            "TEXT_CHAT_4100": {
                "survey_type": "cfm_text_chat_4100",
                "survey_nr": 4100,
                "table_name": "db_prd_rai_core_cfm.cl_p_cfm_text_chat",
                "mode": "append",
            }
        },
    },
    "cdsw_prd": {
        "custom_spark_config": __custom_spark_config,
        "cfg": {
            "TEXT_CHAT_4100": {
                "survey_type": "cfm_text_chat_4100",
                "survey_nr": 4100,
                "table_name": "db_prd_rai_cfm_pwr.cl_p_cfm_text_chat_mt",
                "mode": "append",
            }
        },
    },
    "d200": {
        "custom_spark_config": __custom_spark_config,
        "cfg": {
            "TEXT_CHAT_4100": {
                "survey_type": "cfm_text_chat_4100",
                "survey_nr": 4100,
                "table_name": "db_d200_rai_core_cfm.cl_p_cfm_text_chat",
                "mode": "append",
            }
        },
    },
    "d201": {
        "custom_spark_config": {"spark.executor.cores": "1", "spark.driver.memory": "4g", "spark.driver.cores": "1"},
        "cfg": {
            "TEXT_CHAT_4100": {
                "survey_type": "cfm_text_chat_4100",
                "survey_nr": 4100,
                "table_name": "db_d201_rai_core_cfm.cl_p_cfm_text_chat",
                "mode": "append",
            }
        },
    },
    "cdsw_andras1": {
        "custom_spark_config": __custom_spark_config,
        "cfg": {
            "TEXT_CHAT_4100": {
                "survey_type": "cfm_text_chat_4100",
                "survey_nr": 4100,
                "table_name": "db_prd_rai_pwr.andras_cl_p_cfm_text_chat",
                "mode": "append",
            }
        },
    },
    "cdsw_max1": {
        "custom_spark_config": __custom_spark_config,
        "cfg": {
            "TEXT_CHAT_4100": {
                "survey_type": "cfm_text_chat_4100",
                "survey_nr": 4100,
                "table_name": "db_prd_rai_pwr.max_cl_p_cfm_text_chat",
                "mode": "append",
            }
        },
    },
    "cdsw_janos": {
        "custom_spark_config": __custom_spark_config,
        "cfg": {
            "TEXT_CHAT_4100": {
                "survey_type": "cfm_text_chat_4100",
                "survey_nr": 4100,
                "table_name": "db_prd_rai_cfm_pwr.janos_cl_p_cfm_text_chat",
                "mode": "append",
            }
        },
    },
    "cdsw_talaszlo": {
        "custom_spark_config": __custom_spark_config,
        "cfg": {
            "TEXT_CHAT_4100": {
                "survey_type": "cfm_text_chat_4100",
                "survey_nr": 4100,
                "table_name": "db_prd_rai_pwr.talaszlo_cl_p_cfm_text_chat",
                "mode": "append",
            }
        },
    },
    "local_talaszlo": {
        "custom_spark_config": __custom_spark_config,
        "cfg": {
            "TEXT_CHAT_4100": {
                "survey_type": "cfm_text_chat_4100",
                "survey_nr": 4100,
                "table_name": "db_prd_rai_pwr.talaszlo_cl_p_cfm_text_chat",
                "mode": "append",
            }
        },
    },
}
