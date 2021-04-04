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
    subjects = spSession.read.json("Datasets/BASE A/subjects.json")
    
    # criando tabela temporária para pré transformação dos dados
    subjects.registerTempTable("subjectsTB")
    
    # carregando o arquivo
    student_follow_subject = spSession.read.json("Datasets/BASE A/student_follow_subject.json")
    
    # criando tabela temporária para pré transformação dos dados
    student_follow_subject.registerTempTable("student_subjectTB")
    
    # aplicando transformações nos dados
    student_subject = spSession.sql("""
        select 
            a.id                as subject_id,
            a.name              as subject_nm,
            b.followdate        as subject_follow_dt,
            upper(b.studentid)  as student_id
        from
            subjectstb as a inner join student_subjecttb as b on a.id = b.subjectid
    """)
    
    # gravando informações
    write_data(student_subject)