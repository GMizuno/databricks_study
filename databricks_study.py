from pyspark.sql.functions import col

df_pedidos = spark.read.format("parquet").load("gs://estudo_basico/pedidos/*")
df_entregas = spark.read.option("multiline", "true").format("json").load("gs://estudo_basico/entregas/*")
df_lojas = spark.read.option("multiline", "true").format("json").load("gs://estudo_basico/lojas/*")

df_entregas = df_entregas.filter(df_entregas["entrega_id"].isNotNull())
display(df_entregas)

df_pedidos.printSchema()
