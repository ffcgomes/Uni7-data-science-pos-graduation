--------------------------------------------------------------------------- 
# ARQUITETURA - DATA STREAM
---------------------------------------------------------------------------
 
# Usaremos: Kafka, Python, MongoDB


# ETAPAS DO PIPELINE
# - Código Python lê dataset JSON e o publica em um tópico KAFKA 
# - Código Python consumirá dados deste mesmo tópico e realizará a transformação (agregação) desses dados
# - Código Python Insere os dados já agregados no MongoDB 


# Interessante ilustrar que neste exemplo, os dados são agregados na linguagem PYTHON (usando Python DataFrames, e não no BD), e já são inseridos de forma agregada (sumarizada) no MongoDB 


--------------------------------------------------------------------------------------------------------


# INICIAR ZOOKEEPER
# Novo Terminal:

sudo bash
cd /usr/local/kafka/
bin/zookeeper-server-start.sh config/zookeeper.properties


--------------------------------------------------------------------------------------------------------


# INICIAR SERVIDOR KAFKA
# Novo Terminal:

sudo bash
cd /usr/local/kafka/
bin/kafka-server-start.sh config/server.properties


--------------------------------------------------------------------------------------------------------


# REMOVE TOPICOS, CASO EXISTAM
# abrir nova janela


sudo bash

cd /usr/local/kafka
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic topicoTwiteer



# Caso não exista, irá disparar o erro:
Error while executing topic command : Topics in [] does not exist



--------------------------------------------------------------------------------------------------------


# CRIAR TOPICO
# Novo Terminal:

sudo bash
cd /usr/local/kafka/
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topicoTwitter


--------------------------------------------------------------------------------------------------------


# Verificar se MongoDB está em execução (e se não estiver, inicializá-lo)
systemctl status mongod


# Se o serviço estiver em execução, o console mostrará uma mensagem semelhante ao texto abaixo:
systemctl status mongodb
● mongodb.service - An object/document-oriented database
   Loaded: loaded (/lib/systemd/system/mongodb.service; enabled; vendor preset: 
   Active: active (running) since Fri 2019-05-24 12:10:08 -03; 1h 8min ago
     Docs: man:mongod(1)
 Main PID: 14563 (mongod)
    Tasks: 23 (limit: 2924)
   CGroup: /system.slice/mongodb.service
           └─14563 /usr/bin/mongod --unixSocketPrefix=/run/mongodb --config /etc


 		
--------------------------------------------------------------------------------------------------------


# ANTES DE RODAR VERIFICAR SE O BANCO DE DADOS ALVO ESTÁ VAZIO
# SE JÁ EXISTIR, ELIMINAR O BANCO NO MONGODB

# nova janela

sudo bash

cd /usr/local/bin/robo3t/bin
./robo3t 



# Dropar banco KAFKASTREAMING, caso exista



--------------------------------------------------------------------------------------------------------



# Antes de carregar o tópico, acompanhar no console a produção de dados
# Abrir novo Terminal, para verificar inserção

sudo bash
cd /usr/local/kafka/


# Tópico para publicação inicial dos dados
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topicoDataStreaming --from-beginning
 

apt install -y build-essential libssl-dev python3-dev
apt install -y python3-venv
python3.6 -n venv my_env

source my_env/bin/activate

# Testar o Python
pythonprint("Hello, world")
quit()


pip3 install confluent-kafka
pip3 install pymongo
pip3 install pandas

--------------------------------------------------------------------------------------------------------


# Iniciar Producer
# Novo terminal
# Acessar pasta do arquivo

sudo bash

cd Fontes
python3 pub.py



--------------------------------------------------------------------------------------------------------



# Dando continuidade a implementação do pipeline:
# Iniciar código python que agrega dados e publica diretamente no MongoDB
# COM ISSO O AGREGADOR FICARÁ FUNCIONANDO ININTERRUPTAMENTE


# Abrir Novo terminal
# kafkastreaming é o nome do BD no mongo
sudo bash
cd Fontes/
python3 sub_agg_mongo.py localhost 27017 twitterStreaming



# Após esta execução, os dados serão consumidos do tópico e inseridos no mongodb de forma agregada (sumarizada)



--------------------------------------------------------------------------------------------------------


# Verificar Dados no MongoDB e Robo3T e no Console
# Verificar inserção após finalização do código


# Verificar no Mongo (ROBO3T) dados sendo inseridos no BD
db.getCollection('agg_dados').count()


--------------------------------------------------------------------------------------------------------


# Abrir novo terminal 
# Rodar novo publisher e acompanhar resultados no terminal do "sub_agg_mongo.py", para verificar o pipeline em funcionamento


# Iniciar Producer
# Novo terminal
# Acessar pasta do arquivo

sudo bash

cd Fontes
python3 pub.py


--------------------------------------------------------------------------------------------------------


# Verificar Dados no MongoDB e Robo3T e no Console
# Verificar inserção após finalização do código


# Verificar no Mongo (ROBO3T) dados sendo inseridos no BD
db.getCollection('agg_dados').count()


--------------------------------------------------------------------------------------------------------


