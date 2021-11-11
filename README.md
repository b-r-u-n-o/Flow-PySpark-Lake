# Data-Flow-Einstein

O fluxo apresentado de maneira macro segue as atividade mencionadas no escopo do teste

![Fluxo-dados](resources/fluxo_datalake.png)

## Proposta

A ideia foi representar o fluxo de dados, percorrendo de forma geral:

- Ingestão dos dados na camada raw, onde foi utilizado o PySpark para desenvolver
- Processamento dos dados exames e pacientes da camada raw, criando um novo dataframe na camada service
- Realização da análise com os dados exames_por_pacientes_sp da camada service
  
## Instalação

Para utlizar a aplicação desenvolvida, precisaremos realizar as seguintes etapas:

Primeiramente será necessário realizar a clonagem do repositório:

```shell
# Cria um repositório local com base no remoto

git clone https://github.com/b-r-u-n-o/Data-Flow.git

```
Dentro da pasta raiz do projeto executar o comando:
```shell
# Instala o gerenciador de pacotes pipenv e inicia o ambiente virtual

make pipenv-run
```
## Autor

- [Bruno Soares](https://www.linkedin.com/in/tsbruno/)

## Construido com

- [Python-3.9](https://www.python.org/downloads/release/python-397/): Linguagem usada para o desenvolvimento.
- [PySpark](http://spark.apache.org/docs/latest/api/python/): Framework usado em ambientes distribuídos para manipular grandes quantidades de dados 
- [Pandas-1.3.3](https://pandas.pydata.org/docs/getting_started/install.html): Framework para trabalhar com dados em formatos tabulares
- [Matplotlib-3.4.3](https://matplotlib.org/): Framework usado para a criar as visualizações de dados
- [jupyterlab-3.2.2](https://jupyterlab.readthedocs.io/en/stable/): Usado para gerar as análises gráficas.

## Licença

