import os
from pathlib import Path

from pyspark.sql import SparkSession

from utils import get_project_root

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"


def main() -> None:
    spark: SparkSession = (SparkSession.builder.appName("ReadCSV")
                           .config("spark.driver.extraJavaOptions",
                                   "--add-opens=java.base/java.lang=ALL-UNNAMED "
                                   "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                                   "--add-opens=java.base/java.util=ALL-UNNAMED "
                                   "--add-opens=java.base/javax.security.auth=ALL-UNNAMED")
                           .config("spark.executor.extraJavaOptions",
                                   "--add-opens=java.base/java.lang=ALL-UNNAMED "
                                   "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                                   "--add-opens=java.base/java.util=ALL-UNNAMED "
                                   "--add-opens=java.base/javax.security.auth=ALL-UNNAMED")
                           .getOrCreate())

    fpath: Path = get_project_root(fpath=__file__).joinpath("raw_data", "breweries.csv")

    df = spark.read.csv(str(fpath), header=True, inferSchema=True)
    df.show()
    return None


if __name__ == "__main__":
    main()
