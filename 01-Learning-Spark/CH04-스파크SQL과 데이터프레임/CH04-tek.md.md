# Chapter 4. 스파크 SQL과 데이터프레임: 내장 데이터 소스 소개

![spark sql](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FcQ8EvM%2FbtsJnXdzXg0%2FAAAAAAAAAAAAAAAAAAAAALxV1IKyLznt_aeaazCPeEqsjw8LPE1uxRGfdCf9JS-Z%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1772290799%26allow_ip%3D%26allow_referer%3D%26signature%3DM8m7FRf8MMfvD%252Fc7l1HPU%252F3PpnY%253D)

**스파크 SQL 특징**
- 상위 수준의 정형화 API가 엔진으로 제공
- 다양한 정형 데이터를 읽고 쓰기 가능(Json, 하이브 테이블, Parquet, Avro, ORC, CSV)
- 태블로(Tableau), 파워BI(Power BI), 탈렌드(Talend)와 같은 외부 비즈니스 인텔리전스의 데이터 소스나 MySQL및 PostgreSQL과 같은 RDBMS의 데이터를 JDBC/ODBC 커넥터를 사용하여 가능
- 스파크 애플리케이션에서 DB 안에 테이블/뷰로 저장된 정형 데이터와 소통할 수 있도록 프로그래밍 인터페이스 제공
- SQL 쿼리를 정형 데이터에 실행할 수 있는 대화형 셸 제공
- ANSI SQL : 2003 호환 명령 및 HiveQL 지원

>💡 ANSI SQL
>- DBMS의 종류마다 각기 다른 SQL을 사용하기 때문에, 미국 표준 협회(Amerian National Standards Institute)에서 이를 표준화하여 표준 SQL문을 정립시켜 놓은 것 

## 스파크 애플리케이션에서 스파크 SQL 사용하기
- `SparkSession`은 정형화 API로 스파크를 프로그래밍 하기 위한 통합된 진입점 (Unified Entry Point) 제공
	- 이를 통해, 쉽게 클래스 가져오고 코드에서 인스턴스 생성 가능
```sql
-- SQL 쿼리 실행
-- SparkSession 인스턴스에서 아래 SQL 함수 사용
-- 이렇게 실행된 spark.sql 쿼리는 추가 스파크 작업 수행 가능하도록 데이터프레임 반환
spark.sql("SELECT * FROM TableName")
```
```python
from pyspark.sql import SparkSession

# Create SparkSession 
spark = (SparkSession.builder.appName("SparkSQLExampleAPP").getOrCreate())

csv_file = "데이터 경로"

# 읽고 임시뷰 생성
# 스키마 추론
df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .load(csv_file))

df.createOrRepalceTempView("us_delay_filghts_tbl")
```
```python
# 만약 스키마 지정 원한다면, DDL 형태의 문자열 사용
schema = "'date' STRING, 'delay' INT, 'distance' INT, 'origin' STRING, 'destination' STRING"
```
- `spark.sql` 인터페이스를 사용하면 `DataFrame` 및 `Dataset` API와 마찬가지로 일반적인 데이터 분석 작업 수행 가능
- `spark.sql` 인터페이스 사용하여 데이터 쿼리하는 것은 관계형 데이터베이스 테이블에 일반 SQL 쿼리 작성하는 것과 유사
	- 쿼리는 SQL로 되어 있음에도 가독성과 의미론에서 데이터프레임 API 작업과 유사하다
- 정형 데이터 쿼리할 수 있도록 스파크는 메모리와 디스크상에서 뷰와 테이블 생성 및 관리하는 복잡한 작업 관리
## SQL 테이블과 뷰
> 테이블은 데이터를 갖는다.
> 스파크는 각 테이블과 해당 데이터에 관련된 정보인 스키마, 설명, 테이블명, 데이터베이스명, 칼럼명, 파티션, 실제 데이터의 물리적 위치 등의 메타데이터 갖고 있다.
> 이 모든 정보는 중앙 메타스토어에 저장된다.


- 스파크는 스파크 테이블만을 위한 별도 메타스토어를 생성하지 않는다
	- `/user/hive/warehouse`에 있는 Apache Hive Metastore를 사용하여 테이블에 대한 모든 메타데이터 유지한다.
	- 스파크 구성 변수 `spark.sql.warehouse.dir`을 로컬 또는 외부 분산 저장소로 솔정하여 다른 위치로 기본 경로 변경 가능
### 관리형 테이블과 비관리형 테이블
#### 관리형 테이블 (managed)
- 스파크가 메타데이터와 파일 저장소의 데이터 모두 관리
	- 파일 저장소는 로컬 파일 시스템 또는 HDFS 또는 Amazon S3, Azure Blob 등 객체 저장소일 수 있다.
- 스파크가 모든 것을 관리하기 때문에 `DROP TABLE <테이블명>` 같은 SQL 명령은 메타데이터와 실제 데이터 모두 삭제
#### 비관리형 테이블 (unmanaged)
- 스파크는 오직 메타데이터만 관리하고, 카산드라 같은 외부 데이터 소스에서 데이터 직접 관리
-  `DROP TABLE <테이블명>` 명령이 실제 데이터는 그대로 두고 메타데이터만 삭제
### SQL 데이터베이스와 테이블 생성하기
- 테이블은 데이터베이스 내부 존재
	- 기본적으로 스파크는 `default` 데이터베이스 안에 테이블을 생성
- 사용자 데이터베이스 새로 생성하고 싶다면, 스파크 애플리케이션이나 노트북에서 SQL 명령어 실행 가능
```python
# 데이터 베이스 생성 
spark.sql("CREATE DATABASE learn_spark_db") 
spark.sql("USE learn_spark_db")
```
#### 관리형 테이블 생성
```python
# 관리형 테이블 생성 
flights_df = spark.read.csv("departuredelays.csv", schema=schema) flights_df.write.saveAsTable("managed_us_delay_flights_tbl")
```
#### 비관리형 테이블 생성
- 스파크 애플리케이션에서 접근 가능한 파일 저장소에 있는 `parquer`, `csv`, `JSON` 파일 포맷의 데이터 소스로부터 비관리형 테이블 생성 가능
```python
# 1. sql
spark.sql(""" 
	CREATE TABLE us_delay_flights_tbl ( 
	date STRING, 
	delay INT, 
	distance INT, 
	origin STRING, 
	destination STRING 
	) 
	USING csv OPTIONS (PATH 'departuredelays.csv') 
	""")

# 2. 데이터 프레임 API 
(flights_df
	.write 
	.option("path", "/tmp/data/us_flights_delay")
	.saveAsTable("us_delay_flights_tbl2"))
```
### 뷰 생성하기
- 스파크는 기존 테이블 토대로 뷰 만들 수있다.
	- 전역 (해당 클러스터의 모든 `SparkSession`에서 볼 수 있음)
	- 세션 범위 (단일 `SparkSession`에서만 볼 수 있음)
	- 일시적으로 스파크 애플리케이션이 종료되면 뷰는 사라진다.
- 뷰 생성은 데이터베이스 내에서 테이블을 생성할 때와 유사한 구문 사용
	- 뷰 생성 이후에는 테이블처럼 쿼리 가능
	- 뷰는 테이블과 달리 실제 데이터를 소유하지 않기 때문에 스파크 애플리케이션 종료되면 테이블은 유지되지만 뷰는 사라진다
```python
# 항공사 데이터에서 전역/일반 임시 뷰 생성 
df_sfo = spark.sql("""SELECT date, delay, origin, destination 
					  FROM us_delay_flights_tbl 
					  WHERE origin = 'SFO'""") 
df_jfk = spark.sql("""SELECT date, delay, origin, destination 
					  FROM us_delay_flights_tbl 
					  WHERE origin = 'JFK'""") 

# create a temporary and global temporary view 
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view") 
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")
```

#### 임시 뷰 vs 전역 임시 뷰
**임시 뷰와 전역 임시 뷰는 크게 차이가 없다**
- 임시 뷰
	- 스파크 애플리케이션 내의 단일 `SparkSession`에 연결된다
- 전역 임시 뷰
	- 스파크 애플리케이션 내의 여러 `SparkSession`에서 볼 수 있다
- 사용자는 단일 스파크 애플리케이션에서 여러 `SparkSession` 만들 수 있다.
	- 동일한 Hive Metastore 구성을 공유하지 않는 두 개의 서로 다른 `SparkSession`에서 같은 데이터에 액세스하고 결합하고자 할 때 이 점을 유용하게 사용 가능
### 메타데이터 보기
- 스파크는 각 관리형 및 비관리형 테이블에 대한 메타데이터 관리
	- 메타데이터 저장을 위한 스파크 SQL의 상위 추상화 모듈인 **카탈로그**에 저장
	- 카탈로그는 데이터베이스, 테이블 및 뷰와 관련된 메타데이터 검사
### SQL 테이블 캐싱하기
- 캐싱
	- 데이터를 캐시 메모리상에 올려두는 것. 
	- 캐싱 이후 해당 데이터에 대한 요청이 있을 경우 데이터의 기본 스토리지 위치에 액세스할 때보다 더 빠르게 요청c 처리 가능. 
- `DataFrame`처럼 SQL 테이블과 뷰 또한 캐시 및 언캐싱 가능
	- Spark 3.0에서는 테이블을 `LAZY`로 지정 가능 
		→ 테이블 바로 캐싱하지 않고 처음 사용하는 시점에서 캐싱한다
```sql
--SQL 예제
CACHE [LAZY] TABLE <table-name>
UNCACHE TABLE <table-name>
```
## 데이터프레임 및 SQL 테이블을 위한 데이터 소스
### DataFrameReader
- `DataFrameReader`은 데이터 소스에서 `DataFrame`으로 데이터를 읽기 위한 핵심 구조
```python
# 정의된 형식과 권장되는 사용 패턴
# 함수를 함께 연결하는 이 패턴은 스파크에서 일반적으로 사용하며 가독성 높다
DataFrameReader.format(args).option("key", "value").schema(args).load
```
```python
# 오직 SparkSession 인스턴스 통해서만 DataFrameReader에 액세스 가능
# 즉, DataFrameReader의 인스턴스 개별적으로 만들 수 없다

# 정적 데이터 소스로 부터 데이터를 읽어올 때 
SparkSession.read 
# 스트리밍 소스에서 데이터를 읽어올 때 
SparkSession.readStream
```

### DataFrameWriter
- 지정된 내장 데이터 소스에 데이터 저장하거나 쓰는 작업 수행
	- `DataFrameReader`와 달리 `SparkSession`이 아닌 저장하려는 `DataFrame`에서 인스턴스에 액세스 가능
```python
# 경로 기반 저장
# 지정된 디렉토리 내 파일에 저장
DataFrameWriter.format(args)
	.option(args)
	.bucketBy(args)
	.partitionBy(args)
	.save(path)

# 카탈로그 중심: 데이터를 저장함과 동시에 Spark Catalog 또는 HMS에 테이블 정보 등록
# 파일 + 메타스토어 내 테이블 정보
# 추상화: 사용자는 물리적인 파일 경로를 몰라도 `spark.table("tableName")`이나 SQL 문(`SELECT * FROM tableName`)으로 데이터 조회 가능
DataFrameWriter.format(args)
	.option(args)
	.sortBy(args)
	.saveAsTable(table)
```
```python
# 정적 데이터 소스로 부터 데이터를 읽어올 때 
SparkSession.write 
# 스트리밍 소스에서 데이터를 읽어올 때 
SparkSession.writeStream
```
### Parquet
- 스파크의 기본 데이터 소스는 `parquet`
	- 다양한 I/O 최적화를 제공하는 오픈소스 칼럼 기반 파일 형식
- 최적화와 효율성이 장점
	- 데이터 변환, 정리 후 `parquet` 형식으로 `DataFrame` 저장하여 다운스틍림에서 사용하는 것이 좋다
 - 일반적으로 정적 `parquet` 데이터 소스에서 읽을 때는 스키마 필요 없다.
	- `parquet` 메타데이터는 보통 스키마를 포함하기 때문에, 스파크에서 스키마 파악 가능
	- 스트리밍 데이터 소스의 경우, 스키마 제공 필요
- `parquet`는 효율적이고 컬럼 기반 스토리지 사용하여 빠른 압축 알고리즘 사용 → 스파크의 기본이자 선호하는 데이터 소스
- `parquet` 파일은 데이터파일, 메타데이터, 여러 압축 파일 및 일부 상태파일 포함된 디렉토리 구조에 저장
	- footer의 메타데이터에는 파일 형식의 버전, 스키마, 경로 등의 컬럼 데이터 포함
```python
# 데이터프레임으로 읽기
df = spark.read.format("parquet").load("경로")
```
```sql
-- SQL로 읽어 뷰로 만들기
CREATE OR REPLACE TEMPORARY VIEW "table name"
			USING parquet
			OPTIONS (
				path "경로"
			)
```
```python
# 데이터프레임을 parquet로 쓰기
(df.write.format("parquet")
	.mode("overwrite")
	.option("compression", "snappy")
	.save("경로"))
```
```sql
-- 스파크 SQL 테이블에 데이터프레임 쓰기
(df.write
		.mode("overwrite")
		.saveAsTable("저장할 테이블 명")
```
### JSON
- JSON (JavaScript Object Notation)도 널리 사용된다.
	- XML에 비해 읽기 쉽고, 구문을 분석하기 쉽다
	- 단일 라인 모드, 다중 라인 모드로 표현
### CSV
- 일반 텍스트파일로 널리 사용
	- 쉼표로 각 데이터 또는 필드 구분
	- 쉼표로 구분된 각 줄은 레코드를 나타낸다
- 데이터 및 비즈니스 분석가들 사이에서 널리 사용되는 형식
### Avro
- 특히 Apache Kafka에서 메시지 직렬화 및 역직렬화할 때 사용
	- JSON에 대한 직접 매핑, 속도와 효율성, 많은 프로그래밍 언어에 사용할 수 있는 바인딩을 포함한 많은 이점 제공
### ORC
- 또 다른 최적화된 컬럼 기반 파일 형식
	- 2가지 spark 설정으로 어떤 ORC 구현체 사용할지 지정 가능
- 벡터화된 ORC 리더
	- 한 번에 한 행을 읽는 것이 아니라 행 블록 (블록당 1,024개)를 읽어 작업 간소화
	- 검색, 필터, 집계 및 조인과 같은 집중적인 작업에 대한 CPU 사용량 줄인다.












