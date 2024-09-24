# coding: utf-8

from . import cache, CustomLogger, Settings

from pyspark.ml.clustering import KMeans

logger = CustomLogger("KmeansUtil")


@cache
def get_model(df, k, feature_col, seed=1):
    logger.info("Evaluating Kmeans on column {} - {}".format(feature_col, k))
    try:
        kmeans = (
            KMeans()
            .setK(k)
            .setFeaturesCol(feature_col)
            .setPredictionCol("prediction")
            .setSeed(seed)
        )
        return kmeans.fit(df)
    except Exception as e:
        logger.error(e)
        raise (e)


def get_models(df, feature_col, k_list, seed=1):
    logger.info(
        "Start training KMeans for column {} from {} clusters to {}".format(
            feature_col, min(k_list), max(k_list)
        )
    )
    models = []
    try:
        for k in k_list:
            model = get_model(df, k, feature_col, seed)
            models.append(model)
        return models
    except Exception as e:
        logger.error(e)
        raise (e)
