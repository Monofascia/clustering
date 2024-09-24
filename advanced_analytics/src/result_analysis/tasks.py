import src.utils.common as cu
import src.services.info as info
from ..config.custom_logging import CustomLogger
from ..config.settings import Settings
from ..dal.datasets import Datasets
from ..services.analysis import (
    getRawAnalysis,
    getPCAnalysis,
    getRawHierAnalysis,
    getPCHierAnalysis,
)

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window


def archives_frequencies():
    logger = CustomLogger("ArchivesFrequencies")
    logger.info("Start calculate archives frequencies")

    tarchge = Datasets().tarchge

    def evaluate(analysis, model_type):
        df = analysis.evaluate_prediction().select(
            "id", F.col("prediction").alias("cluster")
        )
        archives_frequencies_df = tarchge.join(df, "id")

        Datasets.cache(archives_frequencies_df)

        result = (
            archives_frequencies_df.groupBy("cluster", "archive")
            .agg(F.count("*").cast(T.IntegerType()).alias("archive_occurrences"))
            .withColumn("analysis_type", F.lit(analysis.analysis_type))
        )

        Datasets.save(
            result.withColumn("model_type", F.lit(model_type)),
            "{}/{}/{}".format(save_path, model_type, analysis.analysis_type),
        )

        Datasets.unpersist(archives_frequencies_df)

    save_path = "{}/archives_frequencies".format(Settings.saving_path)
    try:
        # k ottimo
        raw_opt_k_analysis = getRawAnalysis(
            tarchge, "features", [Settings.clst_km_opt_k_raw]
        )
        evaluate(raw_opt_k_analysis, "opt_k_kmeans")

        pca_opt_k_analysis = getPCAnalysis(
            tarchge, "features", "pca_features", [Settings.clst_km_opt_k_pca]
        )
        evaluate(pca_opt_k_analysis, "opt_k_kmeans")

        # K
        raw_K_analysis = getRawAnalysis(tarchge, "features", [Settings.clst_km_K])
        evaluate(raw_K_analysis, "K_kmeans")

        pca_K_analysis = getPCAnalysis(
            tarchge, "features", "pca_features", [Settings.clst_km_K]
        )
        evaluate(pca_K_analysis, "K_kmeans")

        # Gerarchico t ottimo
        raw_hier_analysis = getRawHierAnalysis(
            tarchge, "features", Settings.clst_km_K, Settings.clst_km_opt_t_raw
        )
        evaluate(raw_hier_analysis, "hierarchical")

        pca_hier_analysis = getPCHierAnalysis(
            tarchge,
            "features",
            "pca_features",
            Settings.clst_km_K,
            Settings.clst_km_opt_t_pca,
        )
        evaluate(pca_hier_analysis, "hierarchical")

    except Exception as e:
        logger.error(e)


def trend_vector():
    logger = CustomLogger("TrendVector")
    logger.info("Start calculate trend vector")

    tarchge = Datasets().tarchge
    items = Datasets().items
    limit_top_vectors = Settings.top_trend_vectors

    archives_occurrences_path = "{}/archives_occurrences".format(Settings.saving_path)
    trend_vector_path = "{}/trend_vector".format(Settings.saving_path)

    def evaluate(analysis, model_type):
        df = analysis.evaluate_prediction()
        occurrences_df = (
            items.join(df, "id", "left")
            .groupBy("prediction", "archives")
            .agg(F.count("*").cast(T.IntegerType()).alias("archives_occurrences"))
            .withColumn('partitioned_count', F.sum("archives_occurrences").over(
                    Window.partitionBy(F.col("prediction"))
                    )
            )
            .withColumn("relative_frequency", F.round(
                F.col("archives_occurrences") / F.col("partitioned_count"), 5
                )
            )
            .withColumn("rank", 
                    F.row_number().over(
                        Window.partitionBy(F.col("prediction"))
                        .orderBy(F.col("archives_occurrences").desc())
                    ))
            .filter(F.col("rank") <= limit_top_vectors)
            .withColumnRenamed("prediction", "cluster")
            .drop("partitioned_count", "rank")
        )
        
        Datasets.cache(occurrences_df)

        Datasets.save(
            occurrences_df.withColumn("analysis_type", F.lit(analysis.analysis_type))
            .withColumn("archives", F.col("archives").cast(T.StringType()))
            .withColumn("model_type", F.lit(model_type)),
            "{}/{}/{}".format(
                archives_occurrences_path, model_type, analysis.analysis_type
            ),
        )

        trend_vector_df = evaluate_trend_vector(occurrences_df, analysis.analysis_type)
        Datasets.save(
            trend_vector_df.withColumn("model_type", F.lit(model_type)),
            "{}/{}/{}".format(trend_vector_path, model_type, analysis.analysis_type),
        )
        Datasets.unpersist(occurrences_df)

    def evaluate_trend_vector(df, analysis_type):
        result = (
            df.withColumn(
                "max_archives",
                F.max("archives_occurrences").over(
                    Window.partitionBy(F.col("cluster"))
                ),
            )
            .withColumn(
                "total_archives_occurrences",
                F.sum("archives_occurrences").over(
                    Window.partitionBy(F.col("cluster"))
                ),
            )
            .filter(F.col("archives_occurrences") == F.col("max_archives"))
            .withColumn(
                "relative_frequency",
                F.round(
                    (
                        F.col("archives_occurrences")
                        / F.col("total_archives_occurrences")
                    ),
                    5,
                ),
            )
            .drop("max_archives", "total_archives_occurrences")
        )
        return (
            cu.add_description(
                result, ["archives"], cu.get_archives_description(Datasets.tarcdege)
            )
            .withColumn("analysis_type", F.lit(analysis_type))
            .withColumn("archives", F.col("archives").cast(T.StringType()))
        )

    try:
        # k ottimo
        raw_opt_k_analysis = getRawAnalysis(
            tarchge, "features", [Settings.clst_km_opt_k_raw]
        )
        evaluate(raw_opt_k_analysis, "opt_k_kmeans")

        pca_opt_k_analysis = getPCAnalysis(
            tarchge, "features", "pca_features", [Settings.clst_km_opt_k_pca]
        )
        evaluate(pca_opt_k_analysis, "opt_k_kmeans")

        # K
        raw_K_analysis = getRawAnalysis(tarchge, "features", [Settings.clst_km_K])
        evaluate(raw_K_analysis, "K_kmeans")

        pca_K_analysis = getPCAnalysis(
            tarchge, "features", "pca_features", [Settings.clst_km_K]
        )
        evaluate(pca_K_analysis, "K_kmeans")

        # Gerarchico t ottimo
        raw_hier_analysis = getRawHierAnalysis(
            tarchge, "features", Settings.clst_km_K, Settings.clst_km_opt_t_raw
        )
        evaluate(
            raw_hier_analysis,
            "hierarchical",
        )

        pca_hier_analysis = getPCHierAnalysis(
            tarchge,
            "features",
            "pca_features",
            Settings.clst_km_K,
            Settings.clst_km_opt_t_pca,
        )
        evaluate(
            pca_hier_analysis,
            "hierarchical",
        )

    except Exception as e:
        logger.error(e)


def update_info():
    info_dict = {
        "K": Settings.clst_km_K,
        "optimal_k_raw": Settings.clst_km_opt_k_raw,
        "optimal_k_pca": Settings.clst_km_opt_k_pca,
        "optimal_t_raw": Settings.clst_km_opt_t_raw,
        "optimal_t_pca": Settings.clst_km_opt_t_pca,
        "top_trend_vectors": Settings.top_trend_vectors
    }
    info.add(info_dict)
