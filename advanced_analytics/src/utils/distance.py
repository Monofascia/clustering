from . import CustomLogger

import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T
from scipy.spatial.distance import pdist

logger = CustomLogger("DistanceUtils")


def vector_distance(df, k, prediction_col="prediction"):
    logger.info("Evaluating vector distance - {}".format(k))
    to_array = F.udf(lambda v: v.toArray().tolist(), T.ArrayType(T.FloatType()))
    vector_distance = F.udf(lambda v: float(np.max(pdist(np.array(v)))), T.FloatType())

    return (
        df.withColumn("value", to_array(F.col("features")))
        .repartition(k, prediction_col)
        .groupBy(prediction_col)
        .agg(F.collect_set(F.col("value")).alias("value"))
        .withColumn("value", vector_distance(F.col("value")))
        .select(F.lit(k).alias("cluster"), "value")
    )
