import src.services.info as info
import src.utils.common as cu
import src.utils.distance as du
import src.utils.hierarchy as hu
import src.utils.kmeans as kmu
import src.utils.session_manager as session_manager

from ..config.custom_logging import CustomLogger
from ..config.settings import Settings
from ..dal.datasets import Datasets
from ..utils.cache_manager import cache

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import PCA
from pyspark.ml.feature import VectorAssembler


_vectore_distance_schema = T.StructType(
    [
        T.StructField("cluster", T.IntegerType(), True),
        T.StructField("value", T.DoubleType(), True),
    ]
)


@cache
def getRawAnalysis(df, input_col, k):
    return _RawAnalysis(df, input_col, k)


@cache
def getPCAnalysis(df, input_col, output_col, k=None):
    return _PCAnalysis(df, input_col, output_col, k)


@cache
def getRawHierAnalysis(df, input_col, k, t):
    return _RawHierAnalysis(df, input_col, k, t)


@cache
def getPCHierAnalysis(df, input_col, output_col, k, t):
    return _PCHierAnalysis(df, input_col, output_col, k, t)


class _RawAnalysis(object):
    def __init__(self, df, input_col, k):
        self.logger = CustomLogger("RawAnalysis")
        self.analysis_type = "raw"
        self.k = k
        self.input_col = input_col
        self.features_col = input_col
        self._source_df = df
        self._df = None
        self._kmeans_models = None
        self._prediction = None
        self._vector_distance = None

    @property
    def df(self):
        @cache
        def helper(df):
            try:
                archives = cu.get_archives(df)

                self.logger.debug("Creating boolean matrix")
                archives_matrix = (
                    df.repartition("id")
                    .withColumn("dummie", F.lit(1))
                    .groupBy("id")
                    .pivot("archive", archives)
                    .agg(F.first("dummie"))
                    .na.fill(0)
                )

                assembler = VectorAssembler(
                    inputCols=archives, outputCol=self.input_col
                )

                self.logger.debug("Creating boolean vectors")
                result = assembler.transform(archives_matrix).select(
                    "id", self.input_col
                )
                Datasets.cache(result)

                info.add({"sample_count": result.count()})

                return result

            except Exception as e:
                self.logger.error(e)
                raise (e)

        if not self._df:
            self._df = helper(self._source_df)
        return self._df

    @property
    def kmeans_models(self):
        if not isinstance(self.k, list):
            raise Exception(
                "Expected list object, '{}' found".format(type(self.k).__name__)
            )
        if not self._kmeans_models:
            self._kmeans_models = {
                model.getOrDefault("k"): model
                for model in kmu.get_models(self.df, self.features_col, self.k)
            }
        return self._kmeans_models

    def vector_distance(self):
        self.logger.debug("Evaluating distance matrix")

        results = []
        session = session_manager.get_session()
        if not isinstance(self.k, list):
            raise Exception(
                "Expected list object, '{}' found".format(type(self.k).__name__)
            )
        for k in self.k:
            df = self.evaluate_prediction(k)
            result = du.vector_distance(df, k)
            if len(self.k) == 1:
                return result
            else:
                results.extend(result.collect())
        return session.createDataFrame(results, _vectore_distance_schema)

    def evaluate_prediction(self, k=None):
        k = self.k[0] if not k else k
        model = self.kmeans_models[k]
        return model.transform(self.df)


class _PCAnalysis(_RawAnalysis):
    def __init__(self, df, input_col, output_col, k):
        super(_PCAnalysis, self).__init__(df, input_col, k)
        self.logger = CustomLogger("PCAnalysis")
        self.analysis_type = "pca"
        self.output_col = output_col
        self.features_col = output_col
        self._model = None

    @property
    def df(self):
        if not self._df:
            self._df = self.model.transform(super(_PCAnalysis, self).df)
        return self._df

    @property
    def model(self):
        @cache
        def helper(df):
            """
            Function to find the right number of Principal Components in a specified range of desired variance explained
            (perc_low >= #PC <= perc_up)
            """
            lower = 1
            upper = len(cu.get_archives(self._source_df))
            perc_low = Settings.variance_lower_perc
            perc_up = Settings.variance_upper_perc
            self.logger.info(
                "Calculating optimal PCA -> Variance: {:.1%} - {:.1%} k: {} to {}".format(
                    perc_low, perc_up, lower, upper
                )
            )
            try:
                while lower < upper:
                    middle = (lower + upper) >> 1
                    model = PCA(
                        k=middle, inputCol=self.input_col, outputCol=self.output_col
                    ).fit(df)
                    variance = sum(model.explainedVariance)

                    self.logger.debug(
                        "Verify k = {} - explainedVariance = {}".format(
                            middle, variance
                        )
                    )

                    if variance < perc_low:
                        lower = middle + 1
                    elif variance > perc_up:
                        upper = middle
                    else:
                        self.logger.info(
                            "Chosen k = {} - explainedVariance = {}".format(
                                middle, variance
                            )
                        )
                        return model

                self.logger.warn(
                    "No variance found in given range. Chosen k = {} - explainedVariance = {}".format(
                        middle, variance
                    )
                )
                return model

            except Exception as e:
                self.logger.error(e)
                raise (e)

        if not self._model:
            self._model = helper(super(_PCAnalysis, self).df)
        return self._model


class _RawHierAnalysis(_RawAnalysis):
    def __init__(self, df, input_col, k, t):
        super(_RawHierAnalysis, self).__init__(df, input_col, [k])
        self.logger = CustomLogger("RawHierAnalysis")
        self.t = t

    def evaluate_prediction(self, t=None):
        self.logger.debug("Evaluating prediction")

        k = self.k[0]
        t = self.t if not t else t
        if not isinstance(t, int):
            raise Exception("Expected int object, '{}' found".format(type(t).__name__))
        session = session_manager.get_session()
        model = self.kmeans_models[k]
        predictions = super(_RawHierAnalysis, self).evaluate_prediction(k)
        output = session.createDataFrame(
            hu.hierarchical_prediction(model, t),
            ["prediction", "hier_prediction"],
        )
        return (
            predictions.join(output, "prediction", "left")
            .drop("prediction")
            .withColumnRenamed("hier_prediction", "prediction")
        )

    def vector_distance(self):
        self.logger.debug("Evaluating distance matrix")

        results = []
        session = session_manager.get_session()
        for t in self.t:
            df = self.evaluate_prediction(t)
            result = du.vector_distance(df, t)
            if len(self.t) == 1:
                return result
            else:
                results.extend(result.collect())

        return session.createDataFrame(results, _vectore_distance_schema)


class _PCHierAnalysis(_PCAnalysis):
    def __init__(self, df, input_col, output_col, k, t):
        super(_PCHierAnalysis, self).__init__(df, input_col, output_col, [k])
        self.logger = CustomLogger("PCHierAnalysis")
        self.t = t

    def evaluate_prediction(self, t=None):
        self.logger.debug("Evaluating prediction")

        k = self.k[0]
        t = self.t if not t else t
        if not isinstance(t, int):
            raise Exception("Expected int object, '{}' found".format(type(t).__name__))
        session = session_manager.get_session()
        model = self.kmeans_models[k]
        predictions = super(_PCHierAnalysis, self).evaluate_prediction(k)
        output = session.createDataFrame(
            hu.hierarchical_prediction(model, t),
            ["prediction", "hier_prediction"],
        )
        return (
            predictions.join(output, "prediction", "left")
            .drop("prediction")
            .withColumnRenamed("hier_prediction", "prediction")
        )

    def vector_distance(self):
        self.logger.debug("Evaluating distance matrix")

        results = []
        session = session_manager.get_session()
        for t in self.t:
            df = self.evaluate_prediction(t)
            result = du.vector_distance(df, t).withColumnRenamed(
                "prediction", "cluster"
            )
            if len(self.t) == 1:
                return result
            else:
                results.extend(result.collect())

        return session.createDataFrame(results, _vectore_distance_schema)
