# coding: utf-8

import src.services.info as info

from . import cache, CustomLogger

import pyspark.sql.functions as F

from pyspark.sql.types import StringType

logger = CustomLogger("CommonUtils")


@cache
def get_archives(df):
    logger.debug("Loading archives")

    try:
        result = df.select("archive").distinct().rdd.map(lambda r: str(r[0])).collect()
        info.add({"sample_archives_num": len(result)})
        return result
    except Exception as e:
        logger.error(e)
        raise (e)


@cache
def get_archives_description(df):
    logger.debug("Loading archives descriptions")

    archives_descriptions_map_raw = df.toPandas().set_index("archive").T.to_dict("list")
    result = {}
    for archive, description in archives_descriptions_map_raw.items():
        result[archive] = description[0].encode("utf-8")
    return result


def add_description(df, columns, columns_descriptions_map):
    """
    A User Defined Function to find descriptions for archives contained in columns which return a df with new columns with names starting with 'desc_'

    - df: a spark dataframe

    - columns: one or more df's columns populated with archives value

    - columns_descriptions_map: a dictionary with {archive: description} as key-value
    """

    def generate_description(value):
        if type(value) == list:
            return ["{}: {}".format(v, columns_descriptions_map[v]) for v in value]
        # elif type(value) == int:
        #     return "{}".format(columns_descriptions_map[value])
        else:
            msg = "Expected list object, '{}' found".format(type(value).__name__)
            raise TypeError(msg)

    get_description = F.udf(
        lambda col: generate_description(col),
        StringType(),
    )
    if not columns:
        raise ValueError("columns must have at least one value.")
    for column in columns:
        df = df.withColumn(
            "{}_description".format(column),
            get_description(F.col(column)).cast(StringType()),
        )
    return df


def range_inclusive(start, end):
    return range(start, end + 1)


def split_segment(string, segment_length):
    len_string = len(string)
    if segment_length > len_string:
        logger.warn(
            "Segment longer than entire string: segment: {}, string: {}".format(
                segment_length, len(string)
            )
        )

    return [
        string[i : i + segment_length] for i in range(0, len_string, segment_length)
    ]
