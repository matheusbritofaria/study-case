from pyspark.sql import SparkSession
from decimal import Decimal


spark = SparkSession.builder.appName("DesafioEngenheiroQ1").getOrCreate()

# Definicao das tabelas equivalentes ao banco de dados enviado
cliente_data = [
    (1, "Cliente A"),
    (2, "Cliente B"),
    (3, "Cliente C"),
    (4, "Cliente D"),
]

contrato_data = [
    (1, True, Decimal(2), 1),
    (2, False, Decimal(1.95), 1),
    (3, True, Decimal(1), 2),
    (4, True, Decimal(3), 4),
]

transacao_data = [
    (1, 1, Decimal(3000), Decimal(6.99)),
    (2, 2, Decimal(4500), Decimal(15)),
    (3, 1, Decimal(57989), Decimal(1.45)),
    (4, 4, Decimal(1), Decimal(0)),
    (5, 4, Decimal(35), None),
]

# Criacao dos dataframes com os respectivos schemas
cliente = spark.createDataFrame(
    cliente_data,
    "cliente_id bigint, nome string",
)
contrato = spark.createDataFrame(
    contrato_data,
    "contrato_id bigint, ativo boolean, percentual decimal(10,2), cliente_id bigint",
)
transacao = spark.createDataFrame(
    transacao_data,
    "transacao_id bigint, contrato_id bigint, valor_total decimal(10,2), percentual_desconto decimal(10,2)",
)

# Criacao das tabelas temporarias para uso no SparkSQL
cliente.createOrReplaceTempView("cliente")
contrato.createOrReplaceTempView("contrato")
transacao.createOrReplaceTempView("transacao")

"""
Resolucao da questao utilizando left join entre as tabelas dimensoes e fato.
Posteriormente é feito um group by para agrupar os valores por cliente.
O valor do cliente A obtido foi diferente do apresentado no exemplo da questao.
O valor do cliente B também, visto que o valor apresentado é referente ao cliente D.
"""

spark.sql(
    """
    SELECT 
          cliente.nome,
          SUM(valor_total * percentual/100) AS ganho
    FROM 
        cliente 
    LEFT JOIN 
        contrato ON cliente.cliente_id = contrato.cliente_id
    LEFT JOIN 
        transacao ON contrato.contrato_id = transacao.contrato_id
    WHERE
        contrato.ativo = True
        AND transacao.contrato_id IS NOT NULL
    GROUP BY
        cliente.nome
          """
).show(truncate=False)
