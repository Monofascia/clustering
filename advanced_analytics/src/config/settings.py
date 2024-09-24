from custom_errors import FileNotFoundError

import os

from configobj import ConfigObj


class Settings(object):
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(Settings, cls).__new__(cls)
            # Load file
            config = None
            config_file = "config.ini"
            for root, _, files in os.walk("."):
                if config_file in files:
                    config = ConfigObj(os.path.join(root, config_file))
                    break
            if not config:
                raise FileNotFoundError(config_file)

            cls.log_root_level = config["logging"]["level"]["root"]
            cls.log_level = config["logging"]["level"]["app"]
            cls.saving_path = config["paths"]["saving"]
            cls.db_name = config["hive"]["db_name"]
            cls.tarchge_name = config["hive"]["tarchge_name"]
            cls.tarcdege_name = config["hive"]["tarcdege_name"]
            cls.tarchge_sample_dimension = config.get("hive").as_float("sample_dimension")
            cls.clst_km_lower_k = (
                config.get("clustering").get("kmeans").as_int("lower_k")
            )
            cls.clst_km_K = config.get("clustering").get("kmeans").as_int("K")
            cls.clst_km_upper_k = (
                config.get("clustering").get("kmeans").as_int("upper_k")
            )
            cls.clst_h_lower_t = (
                config.get("clustering").get("hierarchical").as_int("lower_t")
            )
            cls.clst_h_upper_t = (
                config.get("clustering").get("hierarchical").as_int("upper_t")
            )
            cls.clst_km_opt_k_raw = (
                config.get("clustering").get("kmeans").as_int("optimal_k_raw")
            )
            cls.clst_km_opt_k_pca = (
                config.get("clustering").get("kmeans").as_int("optimal_k_pca")
            )
            cls.clst_km_opt_t_raw = (
                config.get("clustering").get("kmeans").as_int("optimal_t_raw")
            )
            cls.clst_km_opt_t_pca = (
                config.get("clustering").get("kmeans").as_int("optimal_t_pca")
            )
            cls.top_trend_vectors = config.get("clustering").as_int("top_trend_vectors")
            cls.dendr_img_segment_len = (
                config.get("dendrogram").get("img").as_int("split_segment_len")
            )
            cls.dendr_img_width = config.get("dendrogram").get("img").as_float("width")
            cls.dendr_img_height = (
                config.get("dendrogram").get("img").as_float("height")
            )
            cls.dendr_img_dpi = config.get("dendrogram").get("img").as_int("dpi")
            cls.variance_lower_perc = config.get("pca").as_float("variance_lower_perc")
            cls.variance_upper_perc = config.get("pca").as_float("variance_upper_perc")
            cls.n_loadings = config.get("pca").as_int("n_loadings")

        return cls.instance
