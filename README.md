# Desafio Spark

## Qual o objetivo do comando cache em Spark?

Carrega e persiste o RDD na memória. No Spark há dois tipos de operações: transformações e ações. As transformações criam um novo RDD a partir de um existente, exemplo a função map. Ações agregam os elementos do RDD usando alguma função e retorna ao driver o resultado final. As transformações têm a característica de ser "lazy", isso significa que serão computadas somente quando ação for aplicada no RDD. 

Por padrão todas transformações são recomputadas a cada vez que uma ação é executada. Desse modo, numa operação como o map/reduce, os dados são lidos do disco e carregados na memória. Em seguida a transformação map é processada, depois a ação reduce é realizada retornando o resultado para o driver e liberando a memória utilizada. Se a mesma operação for executada novamente, todo processo anterior será repetido. Assim, usando o comando cache o Spark manterá o RDD na memória, tornando as operações mais rápidas.

## O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

Sim, o objetivo principal durante a concepção do Spark foi de criar um framework de processamento de dados em larga escala de forma paralela e distribuída que fosse mais rápido que o MapReduce. Esse objetivo é atingido pois no Spark o processamento é realizado na memória principal dos nós de trabalho, assim evitando operações de disco desnecessárias. 

Além disso, o Spark consegue encadear as tarefas a nível de programação da aplicação usando um grafo acíclico direcionado (DAG) como engine de processamento de dados. Assim, para cada job um DAG de diferentes estágios de tarefas é criado para ser executado em um cluster. Por outro lado, O MapReduce cria um DAG com apenas dois estágios (map e reduce), já o Spark permite a criação de DAGs com qualquer número de estágios. Desse modo, os jobs no Spark podem ser completos em apenas um único estágio ou em um único job com muitos estágios para operação mais complexas, que no MapReduce necessitariam ser divididas em múltiplos jobs. Logo, os jobs no Spark podem ser completos de forma mais rápida do que o MapReduce.

## Qual é a função do SparkContext?

O SparkContext é um objeto que representa o ponto de partida da aplicação Spark. Ele é responsável por configurar o ambiente de execução e estabelecer a conexão com o ambiente de execução do Spark. Ele também pode criar RDDs, variáveis de broadcast e acumuladores.

## Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

RDD é uma coleção imutável de elementos dos dados que podem ser particionada através dos nós de um cluster e pode executar operações como transformações e ações de paralelamente.

Por exemplo, seja os dados `` ["maçã","banana","maçã","maçã","laranja","maçã","maçã","laranja","morango"]`` podemos obter um RDD com as seguintes 3 partições ``[['maçã', 'banana','maçã'], ['maçã','laranja', 'maçã'], ['maçã', 'laranja', 'morango']]``, onde cada partição pode ser distribuída para diferentes nós de um cluster e ser processada paralelamente.

## GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

Com o reduceByKey o Spark pode combinar os resultados com uma mesma chave em cada partição antes arrumar os dados. 

Ex: seja o RDD 
``[[('maçã',1), ('banana',1),('maçã',1)], [('maçã',1),('laranja',1), ('maçã',1)], [('maçã',1), ('laranja',1), ('morango',1)]]``

com reduceByKey primeiro os elementos com mesma chaves são combinados de acordo com uma função passada por parâmetro em cada partição
``[[('maçã',2), ('banana',1)], [('maçã',2),('laranja',1)], [('maçã',1), ('laranja',1), ('morango',1)]]``

em seguida arrumados
``[[('maçã',2),('maçã',2),('maçã',1),('banana',1)], [('laranja',1), ('laranja',1), ('morango',1)]]``

por fim combinados
``[[('maçã',5),('banana',1)], [('laranja',2), ('morango',1)]]``

Já com GroupByKey os elementos de mesma chave são apenas arrumados e posteriormente combinados.

Ex: seja o RDD
``[[('maçã',1), ('banana',1),('maçã',1)], [('maçã',1),('laranja',1), ('maçã',1)], [('maçã',1), ('laranja',1), ('morango',1)]]``

primeiramente os elementos serão combinados
``[[('maçã',1), ('maçã',1),('maçã',1),('maçã',1),('maçã',1)], [('laranja',1),('laranja',1), ], [('morango',1)],[('banana',1)]]``

por fim combinados
``[[('maçã',5)], [('laranja',2)], [('banana',1)], [('morango',1)]]``

Como podemos observar nos exemplos acima, a quantidade de dados que são arrumados com o GroupByKey é bem maior que com reduceByKey. E essa diferença pode escalar muito se considerarmos um grande dataset. Com o groupByKey há uma grande quantidade de dados para serem transferidos pela rede desnecessariamente. Além de ocasionar erros de falta de memória, caso um executor não tenha memória suficiente para armazenar os dados arrumados

## Explique o que o código Scala abaixo faz.

```
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
```

A linha ``val textFile = sc.textFile("hdfs://...")`` cria um RDD a partir de um arquivo de texto armazenado no hdfs.
Exemplo: A partir de arquivo de texto contendo ``maçã banana maçã maçã laranja maçã maçã laranja morango`` teríamos como resultado o seguinte RDD ``[['maçã banana maçã maçã laranja maçã maçã laranja morango']]``

A linha ``val counts = textFile.flatMap(line => line.split(" "))`` transforma o RDD textFile aplicando a função flatMap em um novo RDD com as palavras separadas 
``[['maçã','banana','maçã','maçã','laranja','maçã','maçã','laranja','morango']]``

A linha ``.map(word => (word, 1))`` aplica a função passada como parâmetro da transformação map em cada elemento do RDD resultante do flatMap, produzindo um novo RDD
``[[('maçã', 1), ('banana', 1), ('maçã', 1), ('maçã', 1), ('laranja', 1), ('maçã', 1), ('maçã', 1), ('laranja', 1),('morango', 1)]]``

A linha ``.reduceByKey(_ + _)`` aplica uma ação no RDD acima, a qual combina os elementos com a mesma palavra chave somando os valores e por fim retornando a variável counts um novo RDD como mostrado abaixo que representa a quantidade ocorrências de cada palavra no texto original.
``[[('banana', 1)], [('maçã', 5), ('laranja', 2), ('morango', 1)]]``

Finalmente, a linha ``counts.saveAsTextFile("hdfs://...")`` salva o RDD counts como um arquivo de texto no hdfs.

## HTTP requests to the NASA Kennedy Space Center WWW server

O código usando python/pyspark e respostas para as questões encontram-se no arquivo ``desafio.ipynb``. Para execução desse teste foi utilizado um cluster Spark/YARN do serviço DataProc da nuvem google. Os dois arquivos de log foram carregados para pasta ``/tmp`` do hdfs desse cluster e portanto lidos pelo Spark diretamente do hdfs.


