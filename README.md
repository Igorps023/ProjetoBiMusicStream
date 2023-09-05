
# Data Lake - Pipeline para ingestão de dados (batch)

![image](https://github.com/Igorps023/ProjetoBiMusicStream/assets/98396618/f7c567ae-e5d1-4064-8653-36d068a71a86)
### Diagrama
![image](https://github.com/Igorps023/ProjetoBiMusicStream/assets/98396618/ffc56b21-cb7a-442b-8785-461051fbf9d9)
### Conforme o diagrama proposto, este projeto utilizará 3 tabelas
- 1 tabela fato - Todos os registros de músicas escutadas por usuário
- 1 tabela dimensão - Todos os usuários
- 1 tabela dimensão - Todas as musicas/artista
- Diagrama feito no Draw.io
### Dashboard desenvolvido para a área de BI
![image](https://github.com/Igorps023/ProjetoBiMusicStream/assets/98396618/19eb7715-fc05-4172-8838-d5d82262ec5f)
![image](https://github.com/Igorps023/ProjetoBiMusicStream/assets/98396618/3316343b-f569-4978-ac05-fe569aa525bf)
### Principais Pontos do Dashboard:
- Conexão realizada via Athena
- Dados históricos de streaming para o ano de 2020
###Ideia principal:
- O projeto e dashboard foi desenvolvido partindo do pressuposto que uma gravadora/agência estava interessada em compreender quais são os principais artistas e gêneros consumidos por uma parcela de usuários de streaming.
- Com isso, é possível analisar quais artistas podem ser selecionados para festivais, eventos e também para criação de setlists (com maior assertividade).
- A partir de informações dos álbuns (ficha técnica), é possível mapear, os responsáveis pela produção, gravação e masterização.
- Com base em preferências do consumidor, é possível realizar segmentação de anúncios.
- Apresentar o comportamento de streamings ao longo do ano de 2020, mapeando o principal gênero dos usuários.
- Quais artistas, álbuns e data de lançamento de discos e singles apresentaram maiores resultados.
- Fornecer um dashboard para entendimento macro das informações históricas, e futuramente desenvolver estudos específicos, de acordo com a necessidade da empresa.
- O arquivo PBIX para edição do dashboard está diponível no GitHub.

Principais Pontos do README 🚧

✔ Apresentar, de forma clara, um pipeline para ingestão de dados em um Data Lake hospedado na AWS.

✔ As pastas estão separadas por serviços, facilitando o entendimento e consulta aos scripts respectivos para:

✔ Lambdas (Scripts para lambda functions, as Lambdas são responsáveis por inserir determinado código .Py no EMR)

✔ Jobs (Codigos .Py salvos no S3 e que estao acoplados às Lambdas)

✔ AWS Step Functions (Responsável pela orquestração do pipeline, com configuração para iniciar o pipeline com base em scheduler Cron)

✔ Configuracao de Cluster (EMR, basicamente, utilizamos M5XLarge 1 CORE, 1 NODE)

✔ Metadados (Data Lineage)

✔ Tabelas de Controle do Processamento

✔ Análise exploratória para verificar inconsistências e erros prensentes nas tabelas.

### Pontos Essenciais para entendimento:
- Arquivos sincronizados de um desktop e salvos em um bucket (S3) no formato .csv
![image](https://github.com/Igorps023/ProjetoBiMusicStream/assets/98396618/45b89013-7722-40b6-bc09-3927dd0b228e)

- AWS CLI Commands (Powershell)
  ![image](https://github.com/Igorps023/ProjetoBiMusicStream/assets/98396618/618951cf-cd99-4da2-8aca-8000bbbb885d)
- Enviar arquivos para Transient Zone (camada de ingestão de dados)
![image](https://github.com/Igorps023/ProjetoBiMusicStream/assets/98396618/54c31214-3ad6-4660-b293-770a96a5badb)

```
#Enviar arquivos para S3
aws s3 cp c:\sync s3://atasync1/sync --recursive
```



### O script abaixo é padrão para o processamento de todos os arquivos que chegarem para as camadas do lake
### Iniciar sessão Spark
```
#!/usr/bin/env python
# coding: utf-8
```
```
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
```
### Bibliotecas que serao utilizadas
```
import os
import sys
import pytz
import numpy as np
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf, lpad, translate
from datetime import datetime 
from datetime import timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import *
from pyspark.sql.functions import count, avg
```
### Biblioteca pra controle de data/hora do processamento
```
agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
dthproc = agora.strftime("%Y%m%d%H%M%S")
```

### Criando sessão Spark
```
spark = SparkSession.builder.appName("t_Music_Info").getOrCreate()
sqlContext = SQLContext(spark.sparkContext)
```
```
file = "Music_Info.csv"
```
```
ts_file_generation = file.split("-")[-1].replace(".csv", "") + "00"
print(ts_file_generation)
```
### Buckets S3
```
bucket_raw = "bkt-musicstream-bi/Files/RawZone"
bucket_ingestion = "bkt-musicstream-bi/Files/TransientZone"
bucket_control = "bkt-musicstream-bi/Files/Control"
bucket_trusted = "bkt-musicstream-bi/Files/NA/NATrusted"
```
### Arquivo de ingestao
```
output_lake = "Music_Catalog"
```
```
full_path_ingestion = "s3://{bkt}/{file}".format(bkt=bucket_ingestion, file=file)
print(full_path_ingestion)
```

- s3://bkt-musicstream-bi/Files/TransientZone/Music_Info.csv

```
pouso = spark.read.format(
    "com.databricks.spark.csv").option(
    "header", "true").option(
    "enconding", "ISO-8859-1").option(
    "enconding", "UTF-8").option(
    "inferSchema", "false").option(
    "delimiter", ",").load(
    full_path_ingestion)
pouso.registerTempTable("pouso")
pouso.cache()
qtd=pouso.count() 
print('registros ingestao de dados:', qtd)
```
- registros ingestao de dados: 50683

### Listando Colunas
```
for col in pouso.columns:
    print(col + ",")
```
track_id,
name,
artist,
spotify_preview_url,
spotify_id,
tags,
genre,
year,
duration_ms,
danceability,
energy,
key,
loudness,
mode,
speechiness,
acousticness,
instrumentalness,
liveness,
valence,
tempo,
time_signature,

```
lake = spark.sql(
    """ 
        select
            -- padrao para todas as tabelas
            
            --int(date_format(created_at, 'yyyyMMMM')) as ref,
            --int(date_format(created_at, 'yyyyMMMM')) as ref_partition,
            
            --{tsfileger} as ts_file_generation,
            --{tsfileger} as ts_file_generation_partition,
            
            {pdthproc} as ts_proc,
            {pdthproc} as ts_proc_partition,
            
            year as ref_year,
            year as ref_year_partition,
            
            -- campos do arquivos
            
            track_id,
            name,
            artist,
            spotify_preview_url,
            spotify_id,
            tags,
            genre,
            year,
            duration_ms,
            danceability,
            energy,
            key,
            loudness,
            mode,
            speechiness,
            acousticness,
            instrumentalness,
            liveness,
            valence,
            tempo,
            time_signature
            
        from
            pouso
    
    """.format(tsfileger=ts_file_generation, pdthproc=dthproc))
lake.registerTempTable("lake")
lake.cache()
lake.count()
```
50683
```
lake.columns
```
['ts_proc', 'ts_proc_partition', 'ref_year', 'ref_year_partition', 'track_id', 'name', 'artist', 'spotify_preview_url', 'spotify_id', 'tags', 'genre', 'year', 'duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature']

### Salvando o arquivo em formato .Parquet em outra camada
```
path_raw = os.path.join('s3://', bucket_raw, output_lake)
print(path_raw)

lake.coalesce(1).write. \
partitionBy("ref_year_partition"). \
parquet(path_raw, mode='overwrite')
```



### 🚧 O particionamento da tabela lake foi feito pela perspectiva de consumo dessas informacoes, ou seja, faz mais sentido que a informação seja consumida e armazenada por data de lançamento do álbum.

## Criando uma tabela de controle do processamento
```
varDataFile = output_lake
```
```
controle = spark.sql(
        """
            select 
                '{tb}' as name_file,
                '{tb}' as name_file_partition,
                
                --ref,
                --ref_partition,
                
                ts_proc,
                ts_proc_partition,
                
                count(*) as qtd_registros
            
            from
                lake as a
            group by
                1, 2, 3, 4
            order by
                1, 2, 3, 4
        """.format(tb=varDataFile))
```
```
controle.show(10)
```
```
+-------------+-------------------+--------------+-----------------+-------------+
|    name_file|name_file_partition|       ts_proc|ts_proc_partition|qtd_registros|
+-------------+-------------------+--------------+-----------------+-------------+
|Music_Catalog|      Music_Catalog|20230726205238|   20230726205238|        50683|
+-------------+-------------------+--------------+-----------------+-------------+
```
### Criando uma view
```
controle.registerTempTable("controle")
controle.cache()
qtd = controle.count()
```
```
controle.show(truncate=False)
```
```
+-------------+-------------------+--------------+-----------------+-------------+
|name_file    |name_file_partition|ts_proc       |ts_proc_partition|qtd_registros|
+-------------+-------------------+--------------+-----------------+-------------+
|Music_Catalog|Music_Catalog      |20230726205238|20230726205238   |50683        |
+-------------+-------------------+--------------+-----------------+-------------+
```
### Salvando a tabela de controle do bucket S3 de controle
```
path_control = os.path.join("s3://", bucket_control, "tb_0001_controle_processamento_raw")
print(path_control)
controle.coalesce(1).write. \
parquet(path_control, mode="append")
```

### 🚧 Atenção: Análise Exploratória (EDA), se estiver interessado no entendimento da arquitetura, ou configuração dos serviços AWS, recomendo que vá direto para as outras pastas presentes no repositório. Deste ponto em diante, somente temos códigos destinados ao entendimento dos arquivos que foram ingeridos na camada Transient.
### Principais etapas:
- Leitura de arquivos
- Verificar Schema
- Linhas duplicadas
- Correções de erros
- Tipagem de colunas
### S3 Datasets
```
music_info_raw = spark.read.csv("s3://bkt-musicstream-bi/Files/RawZone/Music_Info.csv", sep=",", header=True)
user_listening_history_raw = spark.read.csv("s3://bkt-musicstream-bi/Files/RawZone/User_Listening_History.csv", sep=",", header=True)
```
### Num Linhas/Colunas (Shape)
```
#numero colunas e linhas
num_rows = music_info_raw.count()
num_cols = len(music_info_raw.columns)
```
```
print('Shape: ({}, {})'.format(num_rows, num_cols))
```
Shape: (50683, 21)
```
#numero colunas e linhas
num_rows1 = user_listening_history_raw.count()
num_cols1 = len(user_listening_history_raw.columns)
```
Shape: (9711301, 3)

### Desc Colunas (Columns description)
```
#colunas
music_info_raw.columns
```
```
#colunas
user_listening_history_raw.columns
```

### Schema
```
#Schema Tabela
#Podemos notar que todos os dados foram armazenados como string, faremos alteracoes futuramente para melhor performance
user_listening_history_raw.printSchema()
```
```
 |-- track_id: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- playcount: string (nullable = true)
```
```
#Schema Tabela
#Podemos notar que todos os dados foram armazenados como string, faremos alteracoes futuramente para melhor performance
music_info_raw.printSchema()
```
```
 |-- track_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- artist: string (nullable = true)
 |-- spotify_preview_url: string (nullable = true)
 |-- spotify_id: string (nullable = true)
 |-- tags: string (nullable = true)
 |-- genre: string (nullable = true)
 |-- year: string (nullable = true)
 |-- duration_ms: string (nullable = true)
 |-- danceability: string (nullable = true)
 |-- energy: string (nullable = true)
 |-- key: string (nullable = true)
 |-- loudness: string (nullable = true)
 |-- mode: string (nullable = true)
 |-- speechiness: string (nullable = true)
 |-- acousticness: string (nullable = true)
 |-- instrumentalness: string (nullable = true)
 |-- liveness: string (nullable = true)
 |-- valence: string (nullable = true)
 |-- tempo: string (nullable = true)
 |-- time_signature: string (nullable = true)
 ```

### 
Verificando a existência de registros nulos
### Criando uma funcao
```
def check_nulls(dataframe, name) -> None:
    '''
    Verifica e exibe a quantidade de valores nulos em cada coluna do dataframe.

    :param dataframe: DataFrame
        Dataframe a ser analisado.
    :param name: str
        Nome identificando o dataframe para exibição na saída.
    '''
    print(f'\n{name.upper()} { "-" * (100 - len(name))}')
    for coluna in dataframe.columns:
        qty = dataframe.filter(dataframe[coluna].isNull()).count()
        if qty >= 1:
            print(f'{coluna}: {qty}')
```

### Aplicando a funcao
```
#Utilizando a def
check_nulls(user_listening_history_raw, "listening_history")
check_nulls(music_info_raw,"music_info")
```
```
LISTENING_HISTORY -----------------------------------------------------------------------------------

MUSIC_INFO ------------------------------------------------------------------------------------------
tags: 1127
genre: 28335
```
```
#Podemos verificar que o historico de musicas nao apresenta nenhum valor nulo, por se tratar de uma tabela fato
#Verificaremos posteriormente se todos os itens da Fato estao presentes nas tabelas dimensao
#Por questao de boas praticas em modelagem de dados
```

### Verificando a existência de linhas duplicadas
### Def para checar linhas em duplicidade
```
def check_duplicates(dataframe, fields) -> None:
    '''
    Verifica e exibe uma amostra de 5 registros duplicados com base em um ou mais campos especificados.

    :param dataframe: DataFrame
        Dataframe a ser analisado.
    :param fields: str ou list de str
        Nome do campo ou lista de campos a serem usados como referência para identificar duplicatas.
    '''
    duplicate = dataframe.groupBy(fields) \
        .agg(count('*').alias('qty')) \
        .where(col('qty') > 1) \
        .orderBy(desc('qty'))
    duplicate.show(5, truncate=False)

```

### Aplicando a funcao
```
for column in music_info_raw.columns:
    duplicates = check_duplicates(music_info_raw, column)
    duplicates.show(20, truncate=False)

```
Output
```
+--------+---+
|track_id|qty|
+--------+---+
+--------+---+

+----+---+
|name|qty|
+----+---+
+----+---+

+------------------+---+
|artist            |qty|
+------------------+---+
|The Rolling Stones|132|
|Radiohead         |111|
|Autechre          |105|
|Tom Waits         |100|
|Bob Dylan         |98 |
|The Cure          |94 |
|Metallica         |85 |
|Johnny Cash       |84 |
|Nine Inch Nails   |83 |
|Sonic Youth       |81 |
|In Flames         |76 |
|Elliott Smith     |76 |
|Iron Maiden       |76 |
|Boards of Canada  |75 |
|Mogwai            |75 |
|Amorphis          |74 |
|Korn              |72 |
|Beastie Boys      |70 |
|Animal Collective |70 |
|Foo Fighters      |70 |
+------------------+---+
only showing top 20 rows

+-----------------------------------------------------------------------------------------------------------+---+
|spotify_preview_url                                                                                        |qty|
+-----------------------------------------------------------------------------------------------------------+---+
|https://p.scdn.co/mp3-preview/e09004cfd16b379c205d7741bff8f9868de2df7e?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/8a56933b07d1d49cef5cd9464d9b550c3e1f3527?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/5609a00d9b5d74b400b9b9579f89986e789a6fe2?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/c2f39f9fc418b48e5f48cc247defcdeb390ba7bc?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/9f7452ca3193988a009e110d2d9ff2eb27da7d4a?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/b8fe85d7a3bed9f98fc376078eebf6b67c0769c8?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/678e38329b33d5f31c752a70ab52cd9174a4694c?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/2dce0f0281d7f104788c5629ea9a97196518ffe5?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/fc256eb2d041b8454f2d63346a3ca23e314c599a?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/422e01f05a43397d566ea5f1765d914a7136b47c?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/a419e2253c652e5e74b385ab4b5b92396165d68f?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/d0cc957765ac5313b7354f5ad438425f9b08f61a?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/c3b196d1d1278f1886d99fcd9399564e425cf1f6?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/ffd6117f5aba530e8ff142fd0e33ba0121597476?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/b08d7121360ab621d22dfcd603db95ff645ef280?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/9a6c5d69bfe8e2fcf8a0f41327be12b7869bb0cf?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/470f7b6fd30137a94a80838c112a206d1121e8c6?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/446bc8308afb573c9a0a6e598043de69ea5cff5a?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/8a5e4f8dce6b1a957e235185a4677841fcaafe90?cid=774b29d4f13844c495f206cafdad9c86|2  |
|https://p.scdn.co/mp3-preview/5fcdcfe7ef20abd006bba666b4a7dff01dd5ec21?cid=774b29d4f13844c495f206cafdad9c86|2  |
+-----------------------------------------------------------------------------------------------------------+---+
only showing top 20 rows

+----------------------+---+
|spotify_id            |qty|
+----------------------+---+
|02VsIBmSkhc7uHNyPViZR3|2  |
|0ndKJL8gA4zLl317M7vndn|2  |
|0thdzbW0cRKCx12VbBRB6T|2  |
|3MUviQJP5DSYI3Li4EbYTQ|2  |
|1Ntzk4JoxcAsrWi73MoBjr|2  |
|00otCiz9kUb3Vg7LPKNCZG|2  |
|5vYA1mW9g2Coh1HUFUSmlb|2  |
|09jsAIZF9ThihIzdrw4KAS|2  |
|22Ty5gK6zbw0hRtGypTuX5|2  |
+----------------------+---+

+--------------------------------+----+
|tags                            |qty |
+--------------------------------+----+
|null                            |1127|
|country                         |506 |
|reggae                          |454 |
|black_metal                     |442 |
|rap, hip_hop                    |378 |
|drum_and_bass                   |365 |
|ska                             |305 |
|industrial                      |283 |
|grindcore                       |279 |
|death_metal, grindcore          |277 |
|jazz                            |240 |
|punk, punk_rock                 |223 |
|death_metal                     |198 |
|doom_metal                      |197 |
|trance                          |192 |
|death_metal, melodic_death_metal|185 |
|thrash_metal                    |184 |
|new_age                         |176 |
|reggae, ska                     |165 |
|french                          |143 |
+--------------------------------+----+
only showing top 20 rows

+----------+-----+
|genre     |qty  |
+----------+-----+
|null      |28335|
|Rock      |9965 |
|Electronic|3710 |
|Metal     |2516 |
|Pop       |1145 |
|Rap       |821  |
|Jazz      |793  |
|RnB       |696  |
|Reggae    |691  |
|Country   |607  |
|Punk      |383  |
|Folk      |355  |
|New Age   |237  |
|Blues     |189  |
|World     |140  |
|Latin     |100  |
+----------+-----+

+----+----+
|year|qty |
+----+----+
|2007|4221|
|2008|3948|
|2009|3827|
|2006|3453|
|2005|3086|
|2010|2775|
|2004|2626|
|2003|2290|
|2011|2055|
|2002|1912|
|2012|1817|
|2013|1806|
|2001|1776|
|2014|1584|
|2000|1319|
|1999|1160|
|1998|1040|
|1997|995 |
|1996|814 |
|1995|746 |
+----+----+
only showing top 20 rows

+-----------+---+
|duration_ms|qty|
+-----------+---+
|214666     |21 |
|240000     |14 |
|218200     |14 |
|205800     |13 |
|216000     |13 |
|218666     |13 |
|211133     |12 |
|160000     |12 |
|224866     |12 |
|247040     |12 |
|269000     |12 |
|214600     |12 |
|258000     |12 |
|200000     |12 |
|230226     |12 |
|217533     |11 |
|217906     |11 |
|229866     |11 |
|210266     |11 |
|216533     |11 |
+-----------+---+
only showing top 20 rows

+------------+---+
|danceability|qty|
+------------+---+
|0.53        |136|
|0.513       |132|
|0.514       |130|
|0.527       |129|
|0.471       |126|
|0.502       |126|
|0.481       |125|
|0.47        |125|
|0.525       |125|
|0.499       |123|
|0.548       |123|
|0.521       |122|
|0.509       |122|
|0.503       |121|
|0.574       |121|
|0.491       |120|
|0.507       |120|
|0.447       |119|
|0.445       |119|
|0.508       |118|
+------------+---+
only showing top 20 rows

+------+---+
|energy|qty|
+------+---+
|0.988 |196|
|0.977 |179|
|0.979 |177|
|0.976 |176|
|0.978 |173|
|0.994 |167|
|0.973 |165|
|0.972 |163|
|0.948 |162|
|0.98  |162|
|0.995 |161|
|0.982 |159|
|0.991 |158|
|0.981 |157|
|0.96  |157|
|0.974 |156|
|0.989 |153|
|0.993 |151|
|0.947 |150|
|0.946 |150|
+------+---+
only showing top 20 rows

+---+----+
|key|qty |
+---+----+
|9  |5908|
|7  |5871|
|2  |5853|
|0  |5744|
|1  |4520|
|4  |4337|
|11 |4098|
|5  |3652|
|6  |3235|
|10 |3025|
|8  |3021|
|3  |1419|
+---+----+

+--------+---+
|loudness|qty|
+--------+---+
|-5.631  |16 |
|-5.717  |16 |
|-4.905  |14 |
|-5.739  |14 |
|-4.841  |14 |
|-4.897  |14 |
|-4.218  |14 |
|-7.125  |14 |
|-6.981  |14 |
|-5.309  |14 |
|-6.583  |14 |
|-5.066  |13 |
|-4.508  |13 |
|-4.929  |13 |
|-6.999  |13 |
|-5.877  |13 |
|-5.055  |13 |
|-5.478  |13 |
|-4.086  |13 |
|-4.445  |13 |
+--------+---+
only showing top 20 rows

+----+-----+
|mode|qty  |
+----+-----+
|1   |31984|
|0   |18699|
+----+-----+

+-----------+---+
|speechiness|qty|
+-----------+---+
|0.0334     |170|
|0.0335     |169|
|0.0316     |168|
|0.0299     |166|
|0.0326     |165|
|0.0312     |165|
|0.0308     |163|
|0.0296     |163|
|0.0311     |163|
|0.0315     |162|
|0.0313     |162|
|0.0307     |159|
|0.0318     |159|
|0.033      |159|
|0.034      |158|
|0.0338     |158|
|0.0305     |156|
|0.107      |155|
|0.0339     |155|
|0.0301     |154|
+-----------+---+
only showing top 20 rows

+------------+---+
|acousticness|qty|
+------------+---+
|0.108       |59 |
|0.109       |58 |
|0.136       |58 |
|0.103       |57 |
|0.132       |57 |
|0.105       |57 |
|0.111       |55 |
|0.119       |55 |
|0.118       |54 |
|0.133       |54 |
|0.115       |53 |
|0.135       |53 |
|0.162       |53 |
|0.107       |53 |
|0.114       |52 |
|0.106       |52 |
|0.192       |51 |
|0.153       |51 |
|0.126       |51 |
|0.102       |51 |
+------------+---+
only showing top 20 rows

+----------------+----+
|instrumentalness|qty |
+----------------+----+
|0.0             |8040|
|0.878           |79  |
|0.906           |72  |
|0.877           |71  |
|0.881           |62  |
|0.907           |62  |
|0.909           |60  |
|0.919           |60  |
|0.887           |59  |
|0.902           |59  |
|0.904           |59  |
|0.882           |59  |
|0.867           |57  |
|0.866           |57  |
|0.885           |57  |
|0.871           |56  |
|0.875           |56  |
|0.892           |56  |
|0.911           |56  |
|0.85            |55  |
+----------------+----+
only showing top 20 rows

+--------+---+
|liveness|qty|
+--------+---+
|0.111   |523|
|0.112   |502|
|0.108   |490|
|0.11    |473|
|0.107   |472|
|0.109   |467|
|0.105   |439|
|0.106   |438|
|0.104   |428|
|0.102   |417|
|0.101   |395|
|0.114   |395|
|0.113   |373|
|0.103   |357|
|0.115   |341|
|0.116   |337|
|0.117   |315|
|0.118   |312|
|0.119   |310|
|0.123   |284|
+--------+---+
only showing top 20 rows

+-------+---+
|valence|qty|
+-------+---+
|0.962  |98 |
|0.961  |98 |
|0.192  |89 |
|0.255  |89 |
|0.233  |87 |
|0.336  |87 |
|0.228  |86 |
|0.198  |86 |
|0.397  |85 |
|0.139  |85 |
|0.356  |85 |
|0.35   |85 |
|0.194  |85 |
|0.142  |84 |
|0.383  |84 |
|0.175  |83 |
|0.197  |83 |
|0.358  |83 |
|0.29   |83 |
|0.14   |81 |
+-------+---+
only showing top 20 rows

+-------+---+
|tempo  |qty|
+-------+---+
|129.998|20 |
|120.012|19 |
|129.996|18 |
|120.001|17 |
|120.009|16 |
|130.0  |16 |
|127.991|14 |
|120.015|14 |
|119.987|14 |
|119.985|14 |
|119.998|14 |
|130.004|14 |
|119.974|13 |
|100.002|13 |
|99.998 |13 |
|119.999|13 |
|139.983|13 |
|120.02 |13 |
|130.018|13 |
|120.013|13 |
+-------+---+
only showing top 20 rows

+--------------+-----+
|time_signature|qty  |
+--------------+-----+
|4             |44989|
|3             |4501 |
|5             |732  |
|1             |451  |
|0             |10   |
+--------------+-----+
```

### Vamos verificar o motivo de algumas linhas estarem duplicadas na coluna 'spotify_id'
```
# Exemplo
music_info_filtro = music_info_raw.filter("spotify_id = '1Ntzk4JoxcAsrWi73MoBjr'")
```

```
# Note: O nome da musica na coluna 'name' apresenta erro de digitacao
music_info_filtro.show()
```
```
+------------------+---------+-------------------+--------------------+--------------------+--------------------+-----+----+-----------+------------+------+---+--------+----+-----------+------------+----------------+--------+-------+------+--------------+
|          track_id|     name|             artist| spotify_preview_url|          spotify_id|                tags|genre|year|duration_ms|danceability|energy|key|loudness|mode|speechiness|acousticness|instrumentalness|liveness|valence| tempo|time_signature|
+------------------+---------+-------------------+--------------------+--------------------+--------------------+-----+----+-----------+------------+------+---+--------+----+-----------+------------+----------------+--------+-------+------+--------------+
|TRXUYQW128F42370DB|hHallmark|Broken Social Scene|https://p.scdn.co...|1Ntzk4JoxcAsrWi73...|alternative, indi...| Rock|2004|     233706|       0.523| 0.583|  0| -10.694|   1|     0.0434|      0.0879|           0.109|   0.144|  0.172|119.98|             3|
|TRCUHWL128F4249F1A| Hallmark|Broken Social Scene|https://p.scdn.co...|1Ntzk4JoxcAsrWi73...|indie, alternativ...| null|2004|     233706|       0.523| 0.583|  0| -10.694|   1|     0.0434|      0.0879|           0.109|   0.144|  0.172|119.98|             3|
+------------------+---------+-------------------+--------------------+--------------------+--------------------+-----+----+-----------+------------+------+---+--------+----+-----------+------------+----------------+--------+-------+------+--------------+
```
### Verificar o total de linhas com duplicacao da coluna 'spotify_id'
```
from pyspark.sql.functions import count

# Group the DataFrame by 'spotify_id' and count the occurrences
duplicate_counts = music_info_raw.groupBy('spotify_id').agg(count('*').alias('count'))

# Filter the DataFrame to keep only rows where count > 1
duplicate_rows = music_info_raw.join(duplicate_counts, on='spotify_id').filter('count > 1')

# Show the duplicate rows DataFrame
# duplicate_rows.sort('name').show(20, truncate=False)
```

### Para este projeto, mantive somente a primeira ocorrencia do 'spotify_id' por conta de representar um "erro" de digitacao/forma alternativa de nome para a mesma musica

```
music_info_unique = music_info_raw.dropDuplicates(subset=['spotify_id'])
```
```
music_info_unique.count()
```
Resultado esperado obtido: Retiramos as 9 linhas que encontravam-se duplicadas no dataset Basta olhar o dataframe abaixo e verificar que a musica 3 AM somente aparece uma vez.

### Amostra de dados
```
#Amostra
music_info_unique.filter("artist = 'Matchbox Twenty'").sort("name").show()
```

### Criando view Spark SQL
```
user_listening_history_raw.createOrReplaceTempView("user_listening_history_raw")
```

```
USER_ID_dim = spark.sql("""

                            SELECT DISTINCT(user_id) FROM user_listening_history_raw

                        """)

```
```
USER_ID_dim.count()
```
Registros 962037

### Tipagem Colunas (conversao de string para respectivos formatos)
Leitura USER_ID_dim
```
#Formato string
USER_ID_dim.printSchema()
```
Schema
```
 |-- user_id: string (nullable = true)
```
Leitura user_listening_history_raw
```
#Formato string
#track_id permanecera string
#user_id permanecera string
#playcount sera convertido para int
user_listening_history_raw.printSchema()
```
```
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
root
 |-- track_id: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- playcount: string (nullable = true)
```
### Tipagem
```
user_listening_history_raw = user_listening_history_raw \
                                .withColumn("track_id", col("track_id").cast(StringType())) \
                                .withColumn("user_id", col("user_id").cast(StringType())) \
                                .withColumn("playcount", col("playcount").cast(IntegerType()))

```
```
#Tipagem trocada com sucesso!
user_listening_history_raw.printSchema()
```
```
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
root
 |-- track_id: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- playcount: integer (nullable = true)

```
### Leitura e tipagem
```
#Music Info
music_info_unique.printSchema()
```
```
 |-- track_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- artist: string (nullable = true)
 |-- spotify_preview_url: string (nullable = true)
 |-- spotify_id: string (nullable = true)
 |-- tags: string (nullable = true)
 |-- genre: string (nullable = true)
 |-- year: string (nullable = true)
 |-- duration_ms: string (nullable = true)
 |-- danceability: string (nullable = true)
 |-- energy: string (nullable = true)
 |-- key: string (nullable = true)
 |-- loudness: string (nullable = true)
 |-- mode: string (nullable = true)
 |-- speechiness: string (nullable = true)
 |-- acousticness: string (nullable = true)
 |-- instrumentalness: string (nullable = true)
 |-- liveness: string (nullable = true)
 |-- valence: string (nullable = true)
 |-- tempo: string (nullable = true)
 |-- time_signature: string (nullable = true)
 ```
 ### Tipagem
 ```
music_info_dim = music_info_unique \
                                .withColumn("track_id", col("track_id").cast(StringType())) \
                                .withColumn("name", col("name").cast(StringType())) \
                                .withColumn("artist", col("artist").cast(StringType())) \
                                .withColumn("spotify_preview_url", col("spotify_preview_url").cast(StringType())) \
                                .withColumn("spotify_id", col("spotify_id").cast(StringType())) \
                                .withColumn("tags", col("tags").cast(StringType())) \
                                .withColumn("genre", col("genre").cast(StringType())) \
                                .withColumn("year", col("year").cast(IntegerType())) \
                                .withColumn("duration_ms", col("duration_ms").cast(IntegerType())) \
                                .withColumn("danceability", col("danceability").cast(DoubleType())) \
                                .withColumn("energy", col("energy").cast(DoubleType())) \
                                .withColumn("key", col("key").cast(IntegerType())) \
                                .withColumn("loudness", col("loudness").cast(DoubleType())) \
                                .withColumn("mode", col("mode").cast(IntegerType())) \
                                .withColumn("speechiness", col("speechiness").cast(DoubleType())) \
                                .withColumn("acousticness", col("acousticness").cast(DoubleType())) \
                                .withColumn("instrumentalness", col("instrumentalness").cast(DoubleType())) \
                                .withColumn("liveness", col("liveness").cast(DoubleType())) \
                                .withColumn("valence", col("valence").cast(DoubleType())) \
                                .withColumn("tempo", col("tempo").cast(DoubleType())) \
                                .withColumn("time_signature", col("time_signature").cast(IntegerType())) 
 ```

### Códigos de tipagem de dados são utilizados devidamente em outros scripts desse projeto.
