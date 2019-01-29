# Desafio Spark

## Qual o objetivo do comando cache em Spark?

Carrega e persiste o RDD na memória. No Spark há dois tipos de operações: transformações e ações. As transformações criam um novo conjunto de dados a partir de um existente, exemplo a função map. Ações agregam os elementos do conjunto de dados usando alguma função e retorna ao driver o resultado final. As transformações têm a característica de ser "lazy", isso significa que serão computadas somente quando ação for aplicada no RDD. 

Por padrão todas transformações são recomputadas a cada vez que uma ação é executada. Desse modo, em uma operação como o map/reduce, os dados são lidos do disco e carregados na memória. Em seguida a transformação map é processada, depois a ação reduce é realizada retornando o resultado para o driver e liberando a memória utilizada. Se a mesma operação for executada novamente, todo processo anterior será repetido. Assim, usando o comando cache o Spark manterá o RDD na memória, tornando as operações mais rápidas.

## O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

Sim, o objetivo principal durante a concepção do Spark foi de criar um framework de processamento de dados em larga escala de forma paralela e distribuída que fosse mais rápido que o MapReduce. Esse objetivo é atingido pois no Spark o processamento é realizado na memória principal dos nós de trabalho, assim evitando operações de disco desnecessárias. 

Além disso, o Spark consegue encadear as tarefas a nível de programação da aplicação usando um grafo acíclico direcionado (DAG) como engine de processamento de dados. Assim, cada para cada job um DAG de diferentes estágios de tarefas é criado para ser executado em um cluster. Por outro lado, O MapReduce cria um DAG com apenas dois estágios (map e reduce), já o Spark permite a criação de DAGs com qualquer número de estágios. Desse modo, os jobs no Spark podem ser completos em apenas um único estágio ou em um único job com muitos estágios para operação mais complexas, que no MapReduce necessitariam ser divididas em múltiplos jobs. Logo, os jobs no Spark podem ser completos de forma mais rápida do que o MapReduce.

## Qual é a função do SparkContext?

O SparkContext é um objeto que representa o ponto de partida da aplicação Spark. Ele é responsável por configurar o ambiente de execução e estabelecer a conexão com o ambiente de execução do Spark. Ele também pode criar RDDs, variáveis de broadcast e acumuladores.

## Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

RDD é uma coleção imutável de elementos dos dados que podem ser particionada atráves dos nós de um cluster e pode executar operações como transformações e ações de paralelamente.

Por exemplo, sejá os dados `` ["maçã","banana","laranja","maçã","maçã","laranja","morango"]`` podemos obter um RDD com as seguintes 3 partições ``[['maçã', 'banana'], ['laranja', 'maçã'], ['maçã', 'laranja', 'morango']]``.




