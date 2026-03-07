# 스파크 SQL과 데이터 프레임: 내장 데이터 소스 소개
## 스파크 SQL의 특징

상위 수준의 정형화 API가 엔진으로 제공된다.
다양한 정형 데이터를 읽거나 쓸 수 있다.(예: json, parquet, avro, orc, csv,하이브 테이블)
태블로, 파워BI, 탈렌드와 같은 외부 비즈니스 인텔리전스의 데이터 소스나 MySQL 및 PostgreSQL과 같은 RDBMS의 데이터를 JDBC/ODBC 커넥터를 사용하여 쿼리할 수 있다.
스파크 애플리케이션에서 데이트베이스 안에 테이블 또는 뷰로 저장되어 있는 정형 데이터와 소통할 수 있도록 프로그래밍 인터페이스를 제공한다.
SQL 쿼리를 정형 데이터에 대해 실행할 수 있는 대화형 셸을 제공한다.
ANSI SQL: 2003 호환 명령 및 HiveQL을 지원한다.
스파크 애플리케이션에서 스파크 SQL 사용하기



스칼라 예제)
```scala
import org.apache.spark.sql.SparkSession
val spark = SparkSession
	.builder
    .appName("SparkSQLExampleApp")
    .getOrCreate()
    
var csvFile = "csv파일 경로"

// 스키마 추론(더 큰 파일의 경우 스키마를 지정해주자)
var df = spark.read.format("csv")
	.option("inferSchema", "true")
    .option("header", "true")
    .load(csvFile)
    
df.createOrReplaceTempView("sample_data")
```


파이썬 예제)
```python
from pyspark.sql import SparkSession

spark = (SparkSession
	.builder 
    .appName("SparkSQLExampleApp")
    .getOrCreate())

csv_file = "파일 경로"

df = (spark.read.format("csv")
	.option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file))
df.createOrReplaceTempView("example_tbl")
```

## SQL 테이블과 뷰

스파크는 각 테이블과 해당 데이터에 관련된 정보인 스키마, 설명, 테이블명, 데이터베이스명, 칼럼명, 파티션, 실제 데이터의 물리적 위치 등의 메타데이터를 가지고 있다. 이 모든 정보는 중앙 메타스토어에 저장된다.
스파크는 스파크 테이블만을 위한 별도 메타스토어를 생성하지 않고 기본적으로는 /user/hive/warehouse 에 있는 아파치 하이브 메타스토어를 사용하여 테이블에 대한 모든 메타데이터를 유지한다.

### 관리형 테이블과 비관리형 테이블
관리형 테이블의 경우 메타데이터와 파일 저장소의 데이터를 모두 관리한다.
로컬 파일 시스템, HDFS, AWS S3, Azure Blob과 같은 저장소에 저장할 수 있다.

비관리형 테이블의 경우에는 오직 메타데이터만 관리하고 카산드라와 같은 외부 데이터 소스에서 데이터를 직접 관리한다.



## SQL 데이터베이스와 테이블 생성하기
### 관리형 테이블 생성하기

파이썬 예제)
```python
spark.sql("CREATE TABLE example_tbl (date STRING, delay INT, distance INT,
origin STRING, destination STRING)")
```

### 비관리형 테이블 생성하기
```python
spark.sql("""
	CREATE TABLE example_tbl (
    date STRING, delay INT, distance INT,
	origin STRING, destination STRING
    )
USING csv OPTIONS (PATH 'csv_파일_경로')
""")
```

## 뷰 생성하기

뷰는 스파크 애플리케이션이 종료되면 사라진다.
```sql
CREATE OR REPLACE GLOBAL TEMP VIEW 테이블명 AS SELECT ~ ;
```

뷰를 사용할땐?
global_temp라는 전역 임시 데이터베이스에 전역 임시 뷰를 생성하였으므로... global_temp.<view_name>을 입력한다.
```sql
SELECT * FROM global_temp.테이블명
```

뷰를 없애고 싶다면?
```sql
DROP VIEW IF EXISTS 뷰_이름
```



## 임시 뷰 vs 전역 임시 뷰

임시뷰와 전역 임시뷰는 큰 차이가 없다.
임시 뷰는 단일 SparkSession에 연결되고 전역 임시 뷰는 여러 SparkSession에서 볼 수 있다.

## 메타데이터 보기

메타데이터는 스파크 SQL의 상위 추상화 모듈인 카탈로그에 저장된다.
```python
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("테이블이름")
```

## 테이블을 데이터프레임으로 읽기
```python
example_df = spark.sql(~)
example_df2 = spark.table(테이블이름)
```



## 데이터프레임 및 SQL 테이블을 위한 데이터 소스

스파크 sql은 다양한 데이터 소스에 대한 인터페이스를 제공한다.
또한 데이터 소스 API를 사용하여 이러한 데이터 소스로부터 데이터를 읽고 쓸 수 있도록 일반적인 함수들을 제공한다.
서로다른 데이터 소스 간에 의사소통하는 방법을 제공하는 상위 수준 데이터 소스 API인 DataFrameReader와 DataFramseWriter가 있다.





### DataFrameReader

DataFrameReader는 데이터 소스에서 데이터 프레임으로 데이터를 읽기 위한 핵심구조이다.
정의된 형식과 권장되는 사용 패턴이 있다.
```python
DataFrameReader.format(args).option("key", "value").schema(args).load()
```

오직 SparkSession 인스턴스를 통해서만 이 DataFrameReader에 액세스할 수 있다.

즉, DataFrameReader의 인스턴스를 개별적으로 만들 수는 없다.
```python
SparkSession.read
// or
SparkSession.readStream
```

스칼라 예제)
```scala
val df = spark.read.format("parquet").load(file)
val df2 = spark.read.load(file)
val df3 = spark.read.format("csv")
	.option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .load("~파일경로")
val df4 = spark.read.format("json")
	.load("파일경로")

```


### DataFrameWriter

데이터 소스에 데이터를 저장하거나 쓰는 작업을 수행한다.
```python
DataFrameWriter.format(args)
	.option(args)
    .bucketBy(args)
    .partitionBy(args)
    .save(path)

DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)
```

인스턴스 핸들가져오기
```python
DataFrame.write
// or
DataFrame.writeStream
```

## 여러가지 데이터 소스를 SQL 테이블로 읽기
```sql
CREATE OR REPLACE TEMPORARY VIEW example.tbl
USING //csv //parquet //json //txt //avro //orc
 OPTIONS (
 	path "파일경로"
 )
```

뿐만 아니라 이미지 파일도 읽을 수 있다.



파이썬 예제)
```python
from pyspark.ml import image

image_dir = '파일경로'
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()

images_df.select("image.컬럼명")
```



## 이진 파일

스파크 3.0 은 이진 파일에 대한 지원을 데이터 소스로 추가했다.

DataFrameReader는 각 이진 파일을 파일의 원본 내용과 메타데이터를 포함하는 단일 데이터 프레임 행으로 변환한다.

이진 파일 데이터 소스는 다음과 같은 열이 있는 데이터 프레임을 생성한다.

경로: StringType
수정시간: TimestampType
길이: LongType
내용: BinaryType

```python
path = '파일경로'
binary_files_df = (spark.read.format("binaryFile")
	.option("pathGlobFilter", "*.jpg")
    .load(path))
binary_files_df.show(5)
```

디렉터리에서 파티션 데이터 검색을 무시하려면 recursiveFileLookup을 "true"로 설정할 수 있다.
```python
path = '파일경로'
binary_files_df = (spark.read.format("binaryFile")
	.option("pathGlobFilter", "*.jpg")
    .option("recursiveFileLookup", "true")
    .load(path))
binary_files_df.show(5)
```

recursiveFileLookup 옵션이 "true"로 설정된 경우 label 칼럼이 존재하지 않는다.
현재는 이진 파일 데이터 소스가 데이터 프레임에서 다시 원래 파일 형식으로 쓰는 것을 지원하지 않는다.
