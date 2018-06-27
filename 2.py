import time, pyspark, socket, jaydebeapi

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from os import listdir
from os.path import isfile, join

conf = pyspark.SparkConf()
spark = SparkSession.builder.getOrCreate()

days = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
        '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
tdays = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
         '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
years = ['1391', '1392', '1393', '1394', '1395', '1396', '1397']

DateFrom = '13960101'
DateTo = '13960101'


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


def Servat_insert(date1):
    # StopThriftserver()
    # read first Table
    lastbal97_01_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/pqLastbal9697")
    lastbal97_01_DF.repartition(6).createOrReplaceTempView("lastbal9697")

    # read second Table
    custinfo97_01_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/pqCust9701")
    custinfo97_01_DF.repartition(6).createOrReplaceTempView("custinfo9701")

    mvCus1_DF = spark.sql("""
                select sum(substr(b.REMAININGAMOUNTEFFECTIVE, 0, length(b.REMAININGAMOUNTEFFECTIVE)-1)) as jam,
                        b.custno
                from lastbal9697 b  
                left join custinfo9701 c on b.custno = c.custno 
                where   b.HISDATE = "+date1+" and   c.custype != '02' 
                group by b.custno""")
    mvCus1_DF.repartition(6).createOrReplaceTempView("mvCus1_DF")
    print('*')
    mvCus2_DF = spark.sql("""
                select a.custno ,
                        sum(substr(a.REMAININGAMOUNTEFFECTIVE, 0, length(a.REMAININGAMOUNTEFFECTIVE)-1)) as mandeh  ,
                        SUBSTRING(a.ACNO, 0,2) as NoeHesab
                from lastbal9697 a
                left join mvCus1_DF b on a.custno = b.custno
                where  a.HISDATE  = "+date1+"  and b.jam >9999999999
                group by a.custno,SUBSTRING(a.ACNO, 0,2)
              """)
    mvCus2_DF.repartition(6).createOrReplaceTempView("mvCus2_DF")
    print('   %%%%%% mvCus1_DF size of ', date1, ' is : ', mvCus1_DF.count())

    mvCus3_DF = spark.sql("""
                select count(DISTINCT custno)
                from mvCus2_DF 
              """)
    mvCus3_DF.repartition(6).createOrReplaceTempView("mvCus3_DF")
    print('   %%%%%% mvCus3_DF size of ', date1, ' is : ', mvCus3_DF.count())




for y in years:
    if DateTo[:4] < y or DateFrom[:4] > y: continue
    for m in months:
        tdays = days
        if DateTo[:6] < y + m or DateFrom[:6] > y + m: continue
        if int(m) > 6: tdays = days[:-1]
        if m == '12': tdays = days[:-2]
        for d in tdays:
            if (DateTo < y + m + d or DateFrom > y + m + d) or (d != '01'): continue
            today = y + m + d
            print(today, ':', time.ctime())
            Servat_insert(today)