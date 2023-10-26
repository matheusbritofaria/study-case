from pyspark.sql import SparkSession
from decimal import Decimal
from pyspark.sql.functions import sum, coalesce, lit


spark = SparkSession.builder.appName("DesafioEngenheiroQ2").getOrCreate()

# Definicao das tabelas equivalentes ao banco de dados enviado
transacao_data = [
    (1, Decimal(3000), Decimal(6.99)),
    (2, Decimal(57989), Decimal(1.45)),
    (4, Decimal(1), None),
    (5, Decimal(34), Decimal(0)),
]

# Criacao dos dataframes com o respectivo schema
transacao = spark.createDataFrame(
    transacao_data,
    "transacao_id bigint, total_bruto decimal(10,2), percentual_desconto decimal(10,2)",
)

# Realizacao do calculo do total liquido com a formula mencionada na questao
total_liquido = (
    transacao.withColumn(
        "total_liquido",
        coalesce("total_bruto", lit(0))
        * (1 - (coalesce("percentual_desconto", lit(0)) / 100)),
    )
    .select(sum("total_liquido").alias("total_liquido"))
    .show(truncate=False)
)
