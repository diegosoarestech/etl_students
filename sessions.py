import findspark
from configparser import ConfigParser

findspark.init()
config = ConfigParser()
config.read("config.ini")

driver = config['MYSQL']['MYSQL_JDBC_DRIVER']
host = config['MYSQL']['MYSQL_HOST']
dataBase = config['MYSQL']['MYSQL_DBNAME']
table = 'stg_sessions'
user = config['MYSQL']['MYSQL_USERNAME']
passWord = config['MYSQL']['MYSQL_PASSWORD']

def write_data(_data):
    
    URL = f"jdbc:mysql://{host}/{dataBase}"
    
    _data.write.format("jdbc").options(
        url = URL,
        driver = driver,
        dbtable = table,
        user = user,
        password = passWord).mode('append').save()
    
    
if __name__ == '__main__':
    
    # importando bibliotecas necessarias
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql import SQLContext
    
    # criando spark context 
    sc = SparkContext()
    
    
    # criando sql context 
    sqlContext = SQLContext(sc)
    
    # criando spark session
    spSession = SparkSession \
        .builder \
        .master("local") \
        .appName("casePasseiDireto") \
        .getOrCreate()
        
    
    # carregando o arquivo
    sessions = spSession.read.json("Datasets/BASE A/sessions.json")
    
    # criando tabela temporária para pré transformação dos dados
    sessions.registerTempTable("sessionsTB")
    
    
    # aplicando transformações nos dados
    sessions = spSession.sql("""
        select
            to_timestamp(sessionstarttime) as session_start_at,
            upper(studentclient)           as student_cli,
            upper(studentid)               as student_id
        from
            sessionstb
    """)
    
    # gravando informações
    write_data(sessions)