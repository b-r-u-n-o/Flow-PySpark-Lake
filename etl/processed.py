import os
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import current_date, lit, months_between, col
from ingest import save_parquet_file
import logging


# TODO: Criar um novo dataset
# Realizar um join com as bases exames e pacientes
# Usar o ID_PACIENTE como chave
# PACIENTES -> Desconsiderar os campos AA_NASCIMENTO e DT_CARGA
#              Criar um campo calculado para VL_IDADE
# EXAMES -> Desconsiderar DT_CARGA
# Criar exames_por_pacientes na camada Service
# Criar exames_por_pacientes_sp na camada Service


def load_parquet_file(spark: SparkSession, parquet_path: str) -> DataFrame:
    """Efetua a carga de arquivo do tipo parquet

    Args:
        spark (SparkSession): SparkSession
        parquet_path (str): caminho de origem do arquivo to/path/parquetfile

    Returns:
        DataFrame
    """
    output_df = spark.read.parquet(parquet_path)
    return output_df


def join_data(
    df: DataFrame, other_df: DataFrame, on_column: str, method: str
) -> DataFrame:
    """Realiza a junção entre dataframes

    Args:
        df (DataFrame): dataframe principal
        other_df (DataFrame): dataframe a ser juntado
        on_column (str): coluna chave para a junção
        method (str): método para a realização ('left', 'inner', 'outer', 'right')

    Returns:
        DataFrame: resultado da junção
    """
    output_df = df.join(other_df, on_column, method)
    return output_df


def insert_calculated_age(df: DataFrame, new_column: str, ref_column: str) -> DataFrame:
    """Insere campo com a idade calculada

    Args:
        df (DataFrame): dataframe alvo
        new_column (str): nome da nova coluna
        ref_column (str): coluna de referência

    Returns:
        DataFrame: modificado com a nova coluna
    """
    output_df = df.withColumn(
        new_column, round(months_between(current_date(), col(ref_column)) / lit(12))
    )
    return output_df


def main():

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)

    PATH_RAW_EXAMES = "../lake/raw/einstein_exames"
    PATH_RAW_PACIENTES = "../lake/raw/einstein_pacientes"
    PATH_SERVICE_EXM_PACIENTES = "../lake/service/exames_por_pacientes"

    PATH_SERVICE_EXM_PACIENTES_SP = "../lake/service/exames_por_pacientes_sp"

    spark = SparkSession.builder.appName("appProcessed").getOrCreate()
    logger.info("Spark session criada")

    df_exames = load_parquet_file(spark=spark, parquet_path=PATH_RAW_EXAMES)
    df_pacientes = load_parquet_file(spark=spark, parquet_path=PATH_RAW_PACIENTES)
    logger.info("Dados carregados com sucesso")

    join_df_pacientes_exames = join_data(
        df=df_pacientes, other_df=df_exames, on_column=["ID_PACIENTE"], method="left"
    )
    logger.info("Join entre os dados de exames e pacientes realizado")

    insere_idade_paciente_df = insert_calculated_age(
        df=join_df_pacientes_exames, new_column="VL_IDADE", ref_column="AA_NASCIMENTO"
    )
    logger.info("Campo calculado com a idade dos pacientes criado")

    df_exames_pacientes_ajustado = insere_idade_paciente_df.drop(
        "DT_CARGA", "AA_NASCIMENTO"
    )
    logger.info("Subset exames por pacientes criado")

    os.makedirs(name=PATH_SERVICE_EXM_PACIENTES, exist_ok=True)
    salvadf_exames_pacientes = save_parquet_file(
        df=df_exames_pacientes_ajustado,
        parquet_path=PATH_SERVICE_EXM_PACIENTES,
        mode="overwrite",
        partition_by=["CD_PAIS", "CD_UF"],
        compression="snappy",
        division_data=1,
    )
    logger.info("Novo dataset criado: exames_por_pacientes")

    novo_df_exames_pacientes_sp = load_parquet_file(
        spark=spark, parquet_path=f"{PATH_SERVICE_EXM_PACIENTES}/CD_PAIS=BR/CD_UF=SP"
    )
    logger.info("Carga do dataset exames_por_pacientes")

    os.makedirs(name=PATH_SERVICE_EXM_PACIENTES_SP, exist_ok=True)
    save_parquet_file(
        df=novo_df_exames_pacientes_sp,
        parquet_path=PATH_SERVICE_EXM_PACIENTES_SP,
        mode="overwrite",
        compression="snappy",
        division_data=1,
    )
    logger.info("Criado novo dataset: exames_por_pacientes_sp")

    spark.stop()
    logger.info("Finalizada sessão Spark.")

if __name__ == "__main__":
    main()