import src.utils.session_manager as session_manager
from ..config.custom_logging import CustomLogger
from ..config.settings import Settings

import pyspark.sql.functions as F
from pyspark.storagelevel import StorageLevel


class Datasets(object):
    def __new__(cls):
        def full_table_name(table):
            return "{db}.{table}".format(db=Settings.db_name, table=table)

        if not hasattr(cls, "instance"):
            cls.instance = super(Datasets, cls).__new__(cls)
            cls.logger = CustomLogger("DatasetsDAL")

            session = session_manager.get_session()
            sc = session_manager.get_context()
            EXCLUDED_ARCHIVES = sc.broadcast([0, 8, 500, 901, 801, 802])
            sample_percentage = Settings.tarchge_sample_dimension

            cls.tarchge = (
                session.table(full_table_name(Settings.tarchge_name))
                .filter(
                    (F.col("flag_movimento") == "I")
                    & (~F.col("archge").isin(EXCLUDED_ARCHIVES.value))
                )
                .withColumn("id", F.concat(F.col("cfccc16"), F.col("progr6")))
                .withColumnRenamed("archge", "archive")
                .select("id", "archive")
            ).sample(sample_percentage)

            cls.tarcdege = (
                session.table(full_table_name(Settings.tarcdege_name))
                .filter(
                    (F.col("archge_ige").isNotNull())
                    & (~F.col("archge_ige").isin(EXCLUDED_ARCHIVES.value))
                )
                .sort(["archge_ige", "progetto_ge"])
                .withColumn("description", F.trim(F.col("chiaro_ge")))
                .withColumn("archive", F.col("archge_ige"))
                .groupBy(F.col("archive"))
                .agg(F.first("description").alias("description"))
                # .select(F.col("archge_ige").alias("archive"), "description")
            )

            Datasets.cache(cls.tarchge)
            Datasets.cache(cls.tarcdege)

            cls.items = (
                cls.tarchge.repartition("id")
                .groupBy("id")
                .agg(F.sort_array(F.collect_set("archive")).alias("archives"))
            )
        return cls.instance

    @classmethod
    def save(cls, df, path, partition_columns=None):
        try:
            if partition_columns and isinstance(partition_columns, (str, list)):
                df.write.partitionBy(partition_columns).mode("overwrite").format(
                    "orc"
                ).option("compression", "snappy").save(path)
            else:
                df.write.mode("overwrite").format("orc").option(
                    "compression", "snappy"
                ).save(path)
            cls.logger.info("Successfully saved {}".format(path))
        except Exception as e:
            cls.logger.error("An error occurred while saving {}".format(path))
            raise e

    @staticmethod
    def get(table_name, schema=None, options={}):
        session = session_manager.get_session()
        path = "{}/{}".format(Settings.saving_path, table_name)
        if schema:
            return session.read.options(**options).schema(schema).orc(path)
        else:
            return session.read.options(**options).orc(path)

    @staticmethod
    def cache(df):
        df.persist(StorageLevel.DISK_ONLY)

    @staticmethod
    def unpersist(df):
        df.unpersist()
