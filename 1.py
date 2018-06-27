import time ,pyspark ,socket ,jaydebeapi

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from os import listdir
from os.path import isfile, join


conf = pyspark.SparkConf()
spark = SparkSession.builder.getOrCreate()

days = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
tdays =['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
months = ['01','02','03','04','05','06','07','08','09','10','11','12']
years = ['1391','1392','1393','1394','1395','1396','1397']

DateFrom = '13960101'
DateTo = '13970301'


def StartThriftserver():
    server_address = ('10.100.136.60', 5555)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)

    except socket.error:
        if 'Connection refused' in socket.error:
            print('Connection refused StartThriftserver')
            return 'Connection refused StartThriftserver'
    try:
        message = ('startthriftserver').encode()
        sock.sendall(message)
        try:
            response = sock.recv(4096)
        except:
            print('Oh noes! %s' % sys.exc_info()[0])
            return False
    finally:
        sock.close()
    return True
def StopThriftserver():

    server_address = ('10.100.136.60', 5555)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)

    except socket.error:
        print(socket.error)
        if 'Connection refused' in str(socket.error):
            print('Connection refused StopThriftserver')
            return 'Connection refused StopThriftserver'
    try:
        message = ('stopthriftserver').encode()
        sock.sendall(message)
        try:
            response = sock.recv(4096)
        except:
            print('Oh noes! %s' % sys.exc_info()[0])
            return False
    finally:
        sock.close()
    return True

def Servat_insert(date1) :
    #StopThriftserver()
    # read first Table
    lastbal97_01_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/pqLastbal9697")
    lastbal97_01_DF.repartition(6).createOrReplaceTempView("lastbal97_01")

    # read second Table
    custinfo97_01_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/pqCust9701")
    custinfo97_01_DF.repartition(6).createOrReplaceTempView("custinfo97_01")

    # STEP 1 : create mandeh sepordeh gharzol hasaneh
    mvGharz_DF = spark.sql("select CUSTNO, SUM(SUBSTRING(REMAININGAMOUNTEFFECTIVE, 0, length(REMAININGAMOUNTEFFECTIVE)-1)) as MandehGharz,HISDATE from lastbal97_01 where (SUBSTRING(ACNO, 0,2)='01' or SUBSTRING(ACNO, 1,2)='03') and HISDATE = "+date1+" group by CUSTNO,HISDATE  ")
    mvGharz_DF.repartition(6).createOrReplaceTempView("mvGharz")
    print('   %%%%%% mvGharz size of ', date1, ' is : ', mvGharz_DF.count())
    
    # STEP 2 : create mandeh sepordeh kootah modat
    mvKootah_DF = spark.sql("select CUSTNO, SUM(SUBSTRING(REMAININGAMOUNTEFFECTIVE, 0, length(REMAININGAMOUNTEFFECTIVE)-1)) as MandehKootah,HISDATE from lastbal97_01 where SUBSTRING(ACNO, 0,2)='02' and HISDATE = "+date1+" group by CUSTNO,HISDATE ")
    mvKootah_DF.repartition(6).createOrReplaceTempView("mvKootah")
    print('   %%%%%% mvKootah size of ', date1, ' is : ', mvKootah_DF.count())
    
    # STEP 3 : create mandeh sepordeh modatdar
    mvModatdar_DF = spark.sql("select CUSTNO, SUM(SUBSTRING(REMAININGAMOUNTEFFECTIVE, 0, length(REMAININGAMOUNTEFFECTIVE)-1)) as MandehModatdar,HISDATE from lastbal97_01 where (SUBSTRING(ACNO, 0,2)='04' or SUBSTRING(ACNO, 1,2)='08') and HISDATE = "+date1+" group by CUSTNO ,HISDATE")
    mvModatdar_DF.repartition(6).createOrReplaceTempView("mvModatdar")
    print('   %%%%%% mvModatdar size of ', date1, ' is : ', mvModatdar_DF.count())

    # STEP 4 : create ghedmat moshtari
    mvGhedmat_DF = spark.sql("select CUSTNO,   int(((substring(current_date(),1,4) *365) +(substring(current_date(),6,2) *30.42) + substring(current_date(),9,2))- 226746.26 - ((substring(concat('13',DATEOPN),1,4) *365 ) + (substring(DATEOPN,3,2)*30.42 ) +  substring(DATEOPN,5,2))) as ghedmat from custinfo97_01")
    mvGhedmat_DF.repartition(6).createOrReplaceTempView("mvGhedmat")
    print('   %%%%%% mvGhedmat size of ', date1, ' is : ', mvGhedmat_DF.count())

    today_DF = spark.sql(" select cast(a.CUSTNO as string) , cast(d.MandehGharz as string) , cast(b.MandehKootah as string) , cast(c.MandehModatdar as string) , cast(a.ghedmat as string) ,cast ( nvl(b.HISDATE,nvl(c.HISDATE,d.HISDATE)) as string ) as  tarikh from mvGhedmat  a LEFT join mvKootah b on a.CUSTNO = b.CUSTNO  LEFT join mvModatdar c on a.CUSTNO = c.CUSTNO LEFT join mvGharz d on a.CUSTNO = d.CUSTNO where cast ( nvl(b.HISDATE,nvl(c.HISDATE,d.HISDATE)) as string ) is not null and  (d.MandehGharz +b.MandehKootah+c.MandehModatdar ) >= 10000000000 ")
    today_DF.repartition(6).createOrReplaceTempView("today")
    print(today_DF.count())
    # print(today_DF.head(10))
    # print('\n\n\n\n\n\n\n')



    # conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", "jdbc:hive2://10.100.136.60:10000", ["hduser", ""],
    #                           jars=["/home/hduser/hadi/ser/libdep"+f for f in listdir("/home/hduser/hadi/ser/libdep") if isfile(join("/home/hduser/hadi/ser/libdep", f))])
    # conn.autocommit = True
    # curs = conn.cursor()
    # curs.execute("CREATE TABLE  IF NOT EXISTS mvServat_D1 (CUSTNO STRING, MandehGharz STRING, MandehKootah STRING, MandehModatdar STRING, ghedmat STRING,tarikh STRING)	using parquet options (path 'hdfs://10.100.136.60:9000/user/hduser/pqServat1')")
    # curs.close()
    # conn.close()

    append_DF = spark.sql("select CUSTNO , MandehGharz , MandehKootah , MandehModatdar , ghedmat, tarikh from today  ")
    append_DF.write.mode("Append").format("parquet").save('hdfs://10.100.136.60:9000/user/hduser/pqServat1' )
    append_DF.createOrReplaceTempView("mvservat_d1")
    spark.catalog.refreshTable("mvservat_d1")
    #spark.sql("refresh table pqServat1")
    #
    # append_DF = spark.sql("select CUSTNO , MandehGharz , MandehKootah , MandehModatdar , ghedmat, nvl(today1,nvl(today2,today3),'Ignore')  as tarikh from today where nvl(today1,nvl(today2,today3)) is not null ")
    # append_DF.write.mode("Ignore").format("parquet").save('hdfs://10.100.136.60:9000/user/hduser/pqServat1')
    #
    # append_DF = spark.sql("select CUSTNO , MandehGharz , MandehKootah , MandehModatdar , ghedmat, nvl(today1,nvl(today2,today3),'Overwrite')  as tarikh from today where nvl(today1,nvl(today2,today3)) is not null ")
    # append_DF.write.mode("Overwrite").format("parquet").save('hdfs://10.100.136.60:9000/user/hduser/pqServat1')
    #spark.sql(" INSERT into TABLE mvServat_D1 select CUSTNO , MandehGharz , MandehKootah , MandehModatdar , ghedmat from today" )
    # today_DF.write.mode('append').insertInto('mvServat_D1')
    # all_DF = spark.sql(" select * from mvServat_D1 ")
    # today_DF.repartition(6).createOrReplaceTempView("all")
    # print(all_DF.head(10))

    #StartThriftserver()

for y in years :
    if DateTo[:4] < y or DateFrom[:4] > y:continue
    for m in months :
        tdays = days
        if DateTo[:6] < y+m or DateFrom[:6] > y+m: continue
        if int(m) > 6 : tdays = days[:-1]
        if m == '12':tdays = days[:-2]
        for d in tdays:
            if (DateTo < y + m +d or DateFrom > y + m + d) or (d != '01'): continue
            today = y + m + d
            print( today, ':', time.ctime())
            Servat_insert(today)