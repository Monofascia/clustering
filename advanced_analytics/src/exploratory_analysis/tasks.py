import src.services.info as info
import src.utils.common as cu
import src.utils.distance as du
import src.utils.hierarchy as hu
import src.utils.session_manager as session_manager

from ..config.custom_logging import CustomLogger
from ..config.settings import Settings
from ..dal.datasets import Datasets
from ..services.analysis import (
    getRawAnalysis,
    getPCAnalysis,
    getRawHierAnalysis,
    getPCHierAnalysis,
)

import base64
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
import pyspark.sql.types as T
from io import BytesIO
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import Window
from scipy.cluster import hierarchy


def association_rules():
    logger = CustomLogger("AssociationRules")
    logger.info("Start calculate association rules")

    tarchge = Datasets().tarchge
    tarcdege = Datasets().tarcdege

    items = Datasets.items.select(F.col("archives"))

    try:
        fp = FPGrowth(itemsCol="archives", minSupport=0.05, minConfidence=0.5)
        logger.info("Evaluating FPGrowth")
        fpm = fp.fit(items)

        frequencies = (
            fpm.freqItemsets.filter(F.size(F.col("items")) > 1)
            .sort("freq", ascending=False)
            .withColumnRenamed("items", "item")
        )

        confidences = fpm.associationRules.orderBy("confidence", ascending=False)

        predictions = (
            fpm.transform(items).filter(F.size(F.col("prediction")) >= 1).distinct()
        )

        archives_descriptions_map_raw = (
            tarcdege.toPandas().set_index("archive").T.to_dict("list")
        )

        logger.debug("Loading archives descriptions")
        archives_descriptions_map = {}
        for archive, description in archives_descriptions_map_raw.items():
            archives_descriptions_map[archive] = description[0].encode("utf-8")

        frequencies_desc = (
            cu.add_description(frequencies, ["item"], archives_descriptions_map)
            .withColumn("item", F.col("item").cast(T.StringType()))
            .withColumn("freq", F.col("freq").cast(T.IntegerType()))
        )

        Datasets.save(frequencies_desc, "{}/frequencies".format(Settings.saving_path))

        confidences_desc = (
            cu.add_description(
                confidences, ["antecedent", "consequent"], archives_descriptions_map
            )
            .withColumn("antecedent", F.col("antecedent").cast(T.StringType()))
            .withColumn("consequent", F.col("consequent").cast(T.StringType()))
            .withColumn("confidence", F.round(F.col("confidence"), 3))
        )

        Datasets.save(confidences_desc, "{}/confidences".format(Settings.saving_path))

        predictions_desc = (
            cu.add_description(
                predictions, ["archives", "prediction"], archives_descriptions_map
            )
            .withColumn("archives", F.col("archives").cast(T.StringType()))
            .withColumn("prediction", F.col("prediction").cast(T.StringType()))
        )

        Datasets.save(predictions_desc, "{}/predictions".format(Settings.saving_path))
    except Exception as e:
        logger.error(e)


def top_n():
    logger = CustomLogger("TopPcs")
    n_load = Settings.n_loadings
    logger.info("Start calculate top {} pc".format(n_load))

    session = session_manager.get_session()
    tarchge = Datasets().tarchge

    try:
        pca_analysis = getPCAnalysis(tarchge, "features", "pca_features")
        pca_model = pca_analysis.model
        pc_columns = [str(i) for i in range(pca_model.getOrDefault("k"))]
        loadings_columns = pc_columns[:]
        loadings_columns.insert(0, "archive")
        archives = cu.get_archives(tarchge)
        loadings_data = []

        for i, v in enumerate(pca_model.pc.toArray().tolist()):
            v.insert(0, int(archives[i]))
            loadings_data.append(v)

        archives_loadings_matrix = session.createDataFrame(
            loadings_data, loadings_columns
        )

        logger.info("Creating archives loadings")

        archives_loadings = session.createDataFrame(
            pd.melt(
                frame=archives_loadings_matrix.toPandas(),
                id_vars="archive",
                value_vars=pc_columns,
                var_name="pc",
                value_name="loading",
            )
        )

        archives_loadings_top10 = (
            archives_loadings.withColumn(
                "row_num",
                F.row_number().over(
                    Window.partitionBy(F.col("pc")).orderBy(
                        F.abs(F.col("loading")).desc()
                    )
                ),
            )
            .filter(F.col("row_num") <= n_load)
            .drop("row_num")
            .withColumn("pc", F.col("pc").cast(T.IntegerType()))
            .withColumn("loading", F.round(F.col("loading"), 5))
        )

        archives_loadings_top10_desc = archives_loadings_top10.join(
            F.broadcast(Datasets.tarcdege), "archive", "left"
        ).withColumnRenamed("description", "archive_description")

        Datasets.save(
            archives_loadings_top10_desc.coalesce(1),
            "{}/top_pcs".format(Settings.saving_path),
        )

        logger.info("Evaluating principal components explained variance")
        pc_explained_variance_data = [
            (i, round(pc, 3)) for i, pc in enumerate(pca_model.explainedVariance)
        ]
        ev_schema = T.StructType(
            [
                T.StructField("pc", T.IntegerType(), False),
                T.StructField("explained_variance", T.DoubleType(), False),
            ]
        )
        pc_explained_variance = session.createDataFrame(
            pc_explained_variance_data, ev_schema
        )

        Datasets.save(
            pc_explained_variance.coalesce(1),
            "{}/pc_explained_variance".format(Settings.saving_path),
        )
    except Exception as e:
        logger.error(e)


def elbow():
    logger = CustomLogger("Elbow")

    logger.info("Start calculate elbow")

    elbow_schema = T.StructType(
        [
            T.StructField("cluster", T.IntegerType(), True),
            T.StructField("cost", T.DoubleType(), True),
        ]
    )

    def evaluate(analysis):
        logger.info("Creating dataframe")
        session = session_manager.get_session()
        df = analysis.df
        result = []
        for k, model in analysis.kmeans_models.items():
            cost = model.computeCost(df)
            result.append((k, cost))
        return session.createDataFrame(result, elbow_schema)

    tarchge = Datasets().tarchge

    try:
        clusters = cu.range_inclusive(
            Settings.clst_km_lower_k, Settings.clst_km_upper_k
        )

        raw_analysis = getRawAnalysis(tarchge, "features", clusters)
        raw_df = evaluate(raw_analysis)

        pca_analysis = getPCAnalysis(tarchge, "features", "pca_features", clusters)
        pca_df = evaluate(pca_analysis)

        result = raw_df.withColumn("analysis_type", F.lit("raw")).union(
            pca_df.withColumn("analysis_type", F.lit("pca"))
        )

        Datasets.save(result.coalesce(1), "{}/elbow".format(Settings.saving_path))

    except Exception as e:
        logger.error(e)


def silhouette():
    logger = CustomLogger("Silhouette")

    logger.info("Start calculate silhouette")

    silhouette_schema = T.StructType(
        [
            T.StructField("cluster", T.IntegerType(), True),
            T.StructField("score", T.DoubleType(), True),
        ]
    )

    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")

    def evaluate(analysis):
        logger.info("Creating dataframe")
        session = session_manager.get_session()
        df = analysis.df
        result = []
        for k, model in analysis.kmeans_models.items():
            prediction = model.transform(df)
            score = evaluator.evaluate(prediction)
            result.append((k, score))
        return session.createDataFrame(result, silhouette_schema)

    tarchge = Datasets().tarchge

    try:
        clusters = cu.range_inclusive(
            Settings.clst_km_lower_k, Settings.clst_km_upper_k
        )

        raw_analysis = getRawAnalysis(tarchge, "features", clusters)
        raw_df = evaluate(raw_analysis)

        pca_analysis = getPCAnalysis(tarchge, "features", "pca_features", clusters)
        pca_df = evaluate(pca_analysis)

        result = raw_df.withColumn("analysis_type", F.lit("raw")).union(
            pca_df.withColumn("analysis_type", F.lit("pca"))
        )

        Datasets.save(result.coalesce(1), "{}/silhouette".format(Settings.saving_path))

    except Exception as e:
        logger.error(e)


def distance_matrix():
    logger = CustomLogger("DistanceMatrix")

    logger.info("Start calculate distance matrix")

    tarchge = Datasets().tarchge
    save_path = "{}/distance_matrix/kmeans".format(Settings.saving_path)

    def evaluate(analysis):
        df = analysis.vector_distance()
        Datasets.save(
            df.coalesce(1)
            .withColumn("analysis_type", F.lit(analysis.analysis_type))
            .withColumn("model_type", F.lit("kmeans")),
            "{}/{}".format(save_path, analysis.analysis_type),
        )

    try:
        clusters = cu.range_inclusive(
            Settings.clst_km_lower_k, Settings.clst_km_upper_k
        )

        raw_analysis = getRawAnalysis(tarchge, "features", clusters)
        evaluate(raw_analysis)

        pca_analysis = getPCAnalysis(tarchge, "features", "pca_features", clusters)
        evaluate(pca_analysis)

    except Exception as e:
        logger.error(e)


def hierarchical_distance_matrix():
    logger = CustomLogger("HierarchicalDistanceMatrix")

    logger.info("Start calculate hierarchical distance matrix")

    tarchge = Datasets().tarchge
    save_path = "{}/distance_matrix/hierarchical".format(Settings.saving_path)

    def evaluate(analysis):
        df = analysis.vector_distance()
        Datasets.save(
            df.coalesce(1)
            .withColumn("analysis_type", F.lit(analysis.analysis_type))
            .withColumn("model_type", F.lit("hierarchical")),
            "{}/{}".format(save_path, analysis.analysis_type),
        )

    try:
        clusters = cu.range_inclusive(Settings.clst_h_lower_t, Settings.clst_h_upper_t)
        raw_analysis = getRawHierAnalysis(
            tarchge, "features", Settings.clst_km_K, clusters
        )
        evaluate(raw_analysis)

        pca_analysis = getPCHierAnalysis(
            tarchge, "features", "pca_features", Settings.clst_km_K, clusters
        )
        evaluate(pca_analysis)

    except Exception as e:
        logger.error(e)


def dendrogram():
    logger = CustomLogger("Dendrogram")

    logger.info("Start create dendrogram")

    dendr_schema = T.StructType(
        [
            T.StructField("index", T.IntegerType(), True),
            T.StructField("analysis_type", T.StringType(), True),
            T.StructField("content", T.StringType(), True),
        ]
    )
    tarchge = Datasets().tarchge

    def evaluate(analysis):
        session = session_manager.get_session()
        model = analysis.kmeans_models.values()[0]
        link_matrix = hu.get_link_matrix(model)
        plt.figure(figsize=(Settings.dendr_img_width, Settings.dendr_img_height))
        dendrogram = hierarchy.dendrogram(link_matrix, orientation="top")
        plt.tight_layout()
        info.add({"{}".format(analysis.analysis_type): dendrogram["leaves"]})

        img_buffer = BytesIO()

        plt.savefig(img_buffer, format="png", dpi=Settings.dendr_img_dpi)
        plt.close()

        img_buffer.seek(0)
        img_bytes = img_buffer.getvalue()
        img_base64 = base64.b64encode(img_bytes).decode("utf-8")
        img_base64_splitted = cu.split_segment(img_base64, Settings.dendr_img_segment_len)
        result = [
            (i, analysis.analysis_type, v) for i, v in enumerate(img_base64_splitted)
        ]

        return session.createDataFrame(result, dendr_schema)

    try:
        raw_analysis = getRawAnalysis(tarchge, "features", [Settings.clst_km_K])
        raw_df = evaluate(raw_analysis)

        pca_analysis = getPCAnalysis(
            tarchge, "features", "pca_features", [Settings.clst_km_K]
        )
        pca_df = evaluate(pca_analysis)
        logger.info("Evaluating pca model")

        result = raw_df.union(pca_df)

        Datasets.save(
            result.coalesce(1),
            "{}/dendrogram".format(Settings.saving_path),
        )

    except Exception as e:
        logger.error(e)


def update_info():
    info_dict = {
        "saving_path": Settings.saving_path,
        "db_name": Settings.db_name,
        "data_table": Settings.tarchge_name,
        "registry_table": Settings.tarchge_name,
        "sample_data_table": Settings.tarchge_sample_dimension,
        "K": Settings.clst_km_K,
        "lower_k": Settings.clst_km_lower_k,
        "upper_k": Settings.clst_km_upper_k,
        "lower_t": Settings.clst_h_lower_t,
        "upper_t": Settings.clst_h_upper_t,
        "variance_lower_perc": Settings.variance_lower_perc,
        "variance_upper_perc": Settings.variance_upper_perc,
        "n_loadings": Settings.n_loadings,
    }
    info.add(info_dict)
