from pyspark.sql.types import *
DateFrom = '13960601'
DateTo = '13970231' # tarikh emrooz ke ba today brabar ast.
days = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
months = ['01','02','03','04','05','06','07','08','09','10','11','12']
years = ['1391','1392','1393','1394','1395','1396','1397']
tdays = days
#list_GH = [];   list_K = [];   list_M = [];   KotahModat_main = [];    GH_main = [];    ModatDar_main = []
list = [];   Modiriat_Main = []



for y in years:
    if DateTo[:4] < y or DateFrom[:4] > y: continue
    for m in months:
        if DateTo[:6] < y + m or DateFrom[:6] > y + m: continue
        if int(m)<= 6: tdays = days
        for d in tdays:
            if DateTo < y + m + d or DateFrom > y + m + d: continue
            today = y + m + d
        if int(m) > 6: tdays = days[:-1]
        for d in tdays:
            if DateTo < y + m + d or DateFrom > y + m + d: continue
            today = y + m + d
        if m == '12': tdays = days[:-2]
        for d in tdays:
            if DateTo < y + m + d or DateFrom > y + m + d: continue
            today = y + m + d

#Mohasebe baze zamani 3 mah gozashteh
year =today[:4]
month = today[4:6]
day = today[-2:]
if int(month)-3 <=0:
    mm = int(month)+9
    yy = int(year)-1
    FDate = str(yy)+str(mm)+day
else:
    mm = int(month)-3
    FDate = year + str(mm) + day

#Mohasebe MandehHesab gharzolhasane, KotahModat, BolandModat
LastBalDF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/pqLastbal9606to9702")
LastBalDF.createOrReplaceTempView("LastBalView")
#GH
GharzDF = spark.sql("select CUSTNO,SUM(SUBSTRING(REMAININGAMOUNTEFFECTIVE, 1, length(REMAININGAMOUNTEFFECTIVE)-1))as MANDE_GH from LastBalView where SUBSTRING(ACNO, 1,2)='01' or SUBSTRING(ACNO, 1,2)='03' and HISDATE between "+FDate+" and "+today+" group by CUSTNO")
GharzDF.createOrReplaceTempView("GharzolhasaneView")
CUSTNO_GH = spark.sql("select CUSTNO from GharzolhasaneView")
MANDE_GH = spark.sql("select MANDE_GH from GharzolhasaneView")
#KM
KotahModatDF = spark.sql("select CUSTNO,SUM(SUBSTRING(REMAININGAMOUNTEFFECTIVE, 1, length(REMAININGAMOUNTEFFECTIVE)-1))as MANDE_KotahModat from LastBalView where SUBSTRING(ACNO, 1,2)='02' and HISDATE between "+FDate+" and "+today+" group by CUSTNO")
KotahModatDF.createOrReplaceTempView("KotahModatView")
CUSTNO_KotahModat = spark.sql("select CUSTNO from KotahModatView")
MANDE_KotahModat =  spark.sql("select MANDE_KotahModat from KotahModatView")
#MD
ModatDarDF = spark.sql("select CUSTNO,SUM(SUBSTRING(REMAININGAMOUNTEFFECTIVE, 1, length(REMAININGAMOUNTEFFECTIVE)-1))as MANDE_ModatDar from LastBalView where SUBSTRING(ACNO, 1,2)='04' or SUBSTRING(ACNO, 1,2)='08' and HISDATE between "+FDate+" and "+today+" group by CUSTNO")
ModatDarDF.createOrReplaceTempView("ModatDarView")
CUSTNO_ModatDar = spark.sql("select CUSTNO from ModatDarView")
MANDE_ModatDar =  spark.sql("select MANDE_ModatDar from ModatDarView")
########

list.append([today, [CUSTNO_GH, MANDE_GH, CUSTNO_KotahModat, MANDE_KotahModat, CUSTNO_ModatDar, MANDE_ModatDar]])
Modiriat_Main.append([today, str(CUSTNO_GH), str(MANDE_GH), str(CUSTNO_KotahModat), str(MANDE_KotahModat),str(CUSTNO_ModatDar), str(MANDE_ModatDar)])


