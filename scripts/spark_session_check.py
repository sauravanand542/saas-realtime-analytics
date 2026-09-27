"""Prove spark-submit can build a session without Ivy using a missing home.

CI runs this under spark-submit as a uid that is not in the image's passwd
file at build time. The entrypoint has to add that uid first.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
home = spark._jvm.java.lang.System.getProperty("user.home")
print(f"JAVA_USER_HOME={home}")
if not home or not str(home).startswith("/") or "?" in str(home):
    raise SystemExit(f"Java user.home is not an absolute directory: {home!r}")
print(f"SPARK_SESSION_STARTED {spark.version}")
spark.stop()
