
import findspark
from configparser import ConfigParser

findspark.init()
config = ConfigParser()
config.read("config.ini")

driver = config['MYSQL']['MYSQL_JDBC_DRIVER']
host = config['MYSQL']['MYSQL_HOST']
dataBase = config['MYSQL']['MYSQL_DBNAME']
table = 'stg_events'
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
    
    # carregando os arquivos
    events = spSession.read.json("Datasets/BASE B/*.json")
    
    # renomeando os campos
    events = events \
        .withColumnRenamed("Page Category", "Page_Category") \
        .withColumnRenamed("Page Category 1", "Page_Category_1") \
        .withColumnRenamed("Page Category 2", "Page_Category_2") \
        .withColumnRenamed("Page Category 3", "Page_Category_3") \
        .withColumnRenamed("Page Name", "Page_Name") \
        .withColumnRenamed("at", "event_at") \
        .withColumnRenamed("first-accessed-page", "first_accessed_page") \
        .withColumnRenamed("language", "event_language") \
        .withColumnRenamed("name", "event_name") \
        .withColumnRenamed("type", "event_type") \
        .withColumnRenamed("at", "event_at") \
        .withColumnRenamed("custom_1", "university") \
        .withColumnRenamed("custom_2", "course") 

    # criando tabela temporária para pré transformação dos dados
    events.registerTempTable("eventsTb")
    
    # transformando dados
    events = spSession.sql("""
        select 
            upper(uuid)                 as event_id,
            to_timestamp(event_at)      as event_at,
            to_date(event_at)           as event_at_dt,
            upper(replace(left(last_accessed_url, instr(last_accessed_url, '?') -1), '/disciplina/', '')) as last_accessed_url,
            upper(page_category)        as page_category,
            upper(replace(left(page_name, instr(page_name, '?') -1), '/disciplina/', '')) as page_name,
            upper(browser)              as browser,
            upper(carrier)              as carrier,
            coalesce(clv_total, 0)      as clv_total,
            upper(user_frequency)       as user_frequency,
            device_new,
            event_language,
            coalesce(coalesce(marketing_campaign, marketing_medium), marketing_source) as marketing,
            upper(left(studentid_clienttype, instr(studentid_clienttype, '@') -1)) as student_id,
            upper(user_type)            as user_type,
            upper(platform)             as platform
        from 
            eventstb
        where
            studentid_clienttype is not null
    """)
    
    # criando tabela temporária para pré transformação dos dados
    events.registerTempTable("eventsTb")
    
    # transformando dados
    events = spSession.sql("""

        with cte as (
            
            select 
                row_number() over(partition by student_id, event_at_dt order by event_at asc) as rw,
                a.*
            from 
                eventstb as a
        
        )
        
        select 
            a.event_id,
            a.event_at,
            a.event_at_dt,
            a.last_accessed_url, 
            a.page_category, 
            a.page_name, 
            a.browser, 
            a.carrier, 
            a.clv_total, 
            a.user_frequency, 
            a.device_new, 
            a.event_language, 
            a.marketing, 
            a.user_type, 
            a.platform,
            cast(unix_timestamp(b.event_at) - unix_timestamp(a.event_at) as bigint) as permanence,
            a.student_id
        from 
            cte as a left join cte as b on a.student_id = b.student_id and a.event_at_dt = b.event_at_dt
            and a.rw = (b.rw - 1)
        order
            by a.student_id,
            a.event_at_dt

    """)
    
    
    # gravando informações
    write_data(events)