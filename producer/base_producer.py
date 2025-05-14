from abc import abstractmethod, ABC
from pyspark.sql import SparkSession

class BaseProducer(ABC):

    def __init__(self, appname):
        self.spark = SparkSession.builder \
            .appName(appname) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()

    @abstractmethod
    def produce_message(self, output_df, topic, checkpoint_location):
        pass