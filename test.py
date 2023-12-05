import hydra
from omegaconf import DictConfig, OmegaConf
from pyspark.sql import SparkSession

@hydra.main(version_base=None, config_path="conf", config_name="config")
def my_app(cfg : DictConfig) -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG {cfg['data']['catalog']}")
    spark.sql(f"USE DATABASE {cfg['data']['schema']}")
    spark.sql(f"SELECT * FROM {cfg['data']['table']} LIMIT 10").show()

if __name__ == "__main__":
    my_app()