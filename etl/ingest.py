from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import current_date
import logging
import os

# TODO:
# Extrair os arquivos CSV da camada Stage para Raw
# Adicionar os campos DT_CARGA (tipo DATE)
# Enconding UTF-8


def load_csv_file(spark: SparkSession, path: str) -> DataFrame:
    """Efetua a carga de dados no formato csv

    Args:
        spark: objeto SparkSession
        path (str): caminho de origem do arquivo a ser carregado

    Returns:
        DataFrameReader: retorna um Spark Dataframe
    """

    output_df = spark.read.options(
        delimiter="|", header=True, encoding="UTF-8", inferSchema=True
    ).csv(path)

    return output_df


def insert_current_date(df: DataFrame) -> DataFrame:
    """Insere um campo com data do dia

    Args:
        df (DataFrame): Recebe o dataframe que receberá a nova coluna

    Returns:
        DataFrame: com a inserção de um campo com a data do dia
    """
    output_df = df.withColumn("DT_CARGA", current_date())

    return output_df


def save_parquet_file(
    df: DataFrame,
    division_data: int,
    parquet_path: str,
    mode: str,
    compression: str,
    partition_by=None,
) -> None:
    """Salva o df no formato parquet

    Args:
        df (DataFrame): Dataframe a ser salvo no formato parquet
        division_data (int): indica o parametro utilizado pelo método coalesce
        parquet_path (str): local onde o arquivo deverá ser salvo
        mode (str): 'append', 'overwrite'
        partition_by (None): nome do campo a ser usado na partição, ou, uma lista de nomes de campos
        compression (str): informar um tipo de compressão para o salvamento
    """
    df.coalesce(division_data).write.parquet(
        path=parquet_path, mode=mode, partitionBy=partition_by, compression=compression
    )


def main():

    ROOT_PATH = "../lake/"
    STAGE_DB_EXAMES = f"{ROOT_PATH}stage/einstein_exames/EINSTEIN_Exames_2_082021.csv"
    STAGE_DB_PACIENTES = (
        f"{ROOT_PATH}stage/einstein_pacientes/EINSTEIN_Pacientes_2_082021.csv"
    )
    RAW_EXAMES_PATH = f"{ROOT_PATH}raw/einstein_exames/"
    RAW_PACIENTES_PATH = f"{ROOT_PATH}raw/einstein_pacientes/"

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)

    spark = SparkSession.builder.appName("app_einstein").getOrCreate()
    logger.info("Spark session iniciada!")

    tbl_exames = load_csv_file(spark=spark, path=STAGE_DB_EXAMES)
    logger.info(f"Carga do arquivo de exames da camada STAGE realizada")

    insere_data_exames = insert_current_date(df=tbl_exames)
    logger.info(f"Inserido o campo com a data da carga no dataset exames")

    os.makedirs(name=RAW_EXAMES_PATH, exist_ok=True)
    save_parquet_file(
        df=insere_data_exames,
        division_data=1,
        mode="overwrite",
        partition_by="DT_COLETA",
        parquet_path=RAW_EXAMES_PATH,
        compression="snappy",
    )
    logger.info(f"Arquivo em formato parquet, salvo na camada RAW")

    tbl_pacientes = load_csv_file(spark=spark, path=STAGE_DB_PACIENTES)
    logger.info(f"Carga do arquivo de pacientes da camada STAGE realizada")

    insere_data_pacientes = insert_current_date(df=tbl_pacientes)
    logger.info(f"Inserido o campo com a data da carga")

    os.makedirs(name=RAW_PACIENTES_PATH, exist_ok=True)
    save_parquet_file(
        df=insere_data_pacientes,
        division_data=1,
        mode="overwrite",
        partition_by="CD_UF",
        parquet_path=RAW_PACIENTES_PATH,
        compression="snappy",
    )
    logger.info(f"Arquivo salvo em formato parquet na camada RAW do Data Lake")

    logger.info(f"Cargas realizadas com sucesso!")
    spark.stop()
    logger.info("Encerrada sessão Spark.")


if __name__=='__main__':
    main()