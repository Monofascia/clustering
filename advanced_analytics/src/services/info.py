import src.utils.session_manager as session_manager
from ..config.custom_logging import CustomLogger
from ..dal.datasets import Datasets
from ..config.settings import Settings

import pyspark.sql.types as T

logger = CustomLogger("InfoService")
_data = {}
_schema = T.StructType(
    [
        T.StructField("key", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
    ]
)


def add(d):
    logger.debug("Add {}".format(d))
    global _data
    for k, v in d.items():
        _data[k] = v


def save():
    global _data
    global _schema
    session = session_manager.get_session()
    info = Datasets.get("info", _schema)
    if _data:
        if info.rdd.isEmpty():
            df = _data
            logger.debug("Saving info {}".format(df))
        else:
            df = info.rdd.collectAsMap()
            df.update(_data)
            logger.debug("Updating info {}".format(df))
        result = session.createDataFrame(df.items(), _schema)
        Datasets.save(result.coalesce(1), "{}/info".format(Settings.saving_path))
