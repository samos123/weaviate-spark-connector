import time
import logging
import os
import re

from pyspark.sql import SparkSession
import pytest
import docker
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import weaviate


def get_connector_version():
    version_file = open("version.sbt", "r")
    version_str = version_file.read()
    version_file.close()
    return re.search(r"version :=\ \"(.*)\"", version_str).group(1)


connector_version = os.environ.get("CONNECTOR_VERSION", get_connector_version())
scala_version = os.environ.get("SCALA_VERSION", "2.12")
weaviate_version = os.environ.get("WEAVIATE_VERSION", "1.17.2")
spark_connector_jar_path = os.environ.get(
    "CONNECTOR_JAR_PATH", f"target/scala-{scala_version}/spark-connector-assembly-{connector_version}.jar")


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("Weaviate Pyspark Tests") \
        .master('local') \
        .config("spark.jars", spark_connector_jar_path) \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()


@pytest.fixture
def weaviate_client():
    container_name = "weaviate-python-integration-tests"
    client = docker.from_env()
    client.containers.run(
        f"semitechnologies/weaviate:{weaviate_version}",
        detach=True,
        name=container_name,
        ports={"8080/tcp": "8080"},
        environment={"QUERY_DEFAULTS_LIMIT": 25,
                     "AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true",
                     "DEFAULT_VECTORIZER_MODULE": "none",
                     "CLUSTER_HOSTNAME": "node1",
                     "PERSISTENCE_DATA_PATH": "./data"},
    )
    time.sleep(0.5)
    wclient = weaviate.Client('http://localhost:8080')
    test_class_name = "TestWillBeRemoved"
    retries = 3
    while retries > 0:
        try:
            wclient.schema.create_class({"class": test_class_name})
            wclient.schema.delete_class(test_class_name)
            retries -= 1
        except Exception as e:
            logging.info(f"error connecting to weaviate with client. Retrying in 1 second. Exception: {e}")
            time.sleep(1)
    yield wclient
    client.containers.get(container_name).remove(force=True)


def test_string_arrays(spark: SparkSession, weaviate_client: weaviate.Client):
    article = {"class": "Article",
               "properties": [
                   {"name": "title", "dataType": ["string"]},
                   {"name": "keywords", "dataType": ["string[]"]},
               ]}
    weaviate_client.schema.create_class(article)

    spark_schema = StructType([
        StructField('title', StringType(), True),
        StructField('keywords', ArrayType(StringType()), True)
    ])
    articles = [("Sam and Sam", []), ("Not Sam", ["keyword1", "keyword2"])]
    df = spark.createDataFrame(data=articles, schema=spark_schema)
    df.write.format("io.weaviate.spark.Weaviate") \
        .option("scheme", "http") \
        .option("host", "localhost:8080") \
        .option("className", "Article") \
        .mode("append").save()

    weaviate_articles = weaviate_client.data_object.get(class_name="Article").get("objects")
    assert len(weaviate_articles) == 2
