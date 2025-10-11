from pyspark.sql import SparkSession

spark = (
        SparkSession.builder
        .appName("IngestCliente")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

DB_HOST = 'db'
DB_PORT = '5432'
DB_NAME =  'mydb'
DB_USER = 'myuser'
BD_PASSWORD = 'mypassword'

url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

properties = {
    "user": DB_USER,
    "password": BD_PASSWORD,
    "driver": "org.postgresql.Driver"
}

query = "select id, nome, email, telefone, data_cadastro, is_date from db_loja.clientes"

df = (
    spark.read.format("jdbc")
    .option("url", url)
    .option("query", query)       # <- nome da tabela
    .option("user", DB_USER)
    .option("password", BD_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

df.show(5)