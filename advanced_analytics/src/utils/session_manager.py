from . import CustomLogger, Settings

import json
from pyspark.sql import SparkSession

logger = CustomLogger("SessionManager")
_session = None


def _initialize():
    logger.info("Initializing Spark session")
    global _session
    try:
        _session = (
            SparkSession.builder.enableHiveSupport().getOrCreate()
            if not _session
            else _session
        )
        _session.sparkContext.setLogLevel(Settings.log_root_level)
        logger.info("Initialized Spark session")
        logger.info(
            "Spark configurarion: {}".format(json.dumps(get_config(), indent=4))
        )
    except Exception as e:
        logger.error("Error while initializing session")
        raise e


def get_session():
    global _session
    if not _session:
        _initialize()
    return _session


def get_context():
    global _session
    return _session.sparkContext


def stop():
    global _session
    if _session:
        _session.stop()
        logger.info("Spark session stopped")
    else:
        logger.warning("Spark session is not initialized")


def get_config():
    global _session
    spark_config = {}
    for item in sorted(_session.sparkContext.getConf().getAll()):
        spark_config[item[0]] = item[1]
    return spark_config
