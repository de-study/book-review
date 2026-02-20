> 외부의 물리적 데이터를 스파크 엔진이 계산할 수 있는 논리적 객체인 DataFrame으로 변환(Ingest)하고 관리하는 것

```
데이터 소스(csv,parquet...) → 데이터프레임(DataFrame)
```
# 1. 스파크SQL

### (1) 배경
- 기존 RDD의 한계
	- 스키마를 Spark가 알수 없어서 자동 최적화 할 수 없음
- DataFrame API 출시 → SQL에 익숙한 분석가/엔지니어
- Spark SQL은 이 두 문제를 동시에 해결 → **SQL 문법 지원 + Catalyst Optimizer로 자동 최적화**
```
스키마를 모를 때 (RDD)

- 데이터가 그냥 바이트 덩어리로 취급됨
- Spark 입장에서는 컬럼이 뭔지, 타입이 뭔지 모름
- 결국 전체 데이터를 다 읽고, 처리는 개발자 코드에 맡김
- 최적화 여지 없음


스키마를 알 때 가능한 최적화 3가지
1. Column Pruning (컬럼 가지치기)
    - SELECT,WHERE절 등에서 사용된 컬럼만 읽어들임
    - Parquet 컬럼별로 저장되므로 컬럼만 읽어들이기 적합 → I/O 자체를 대폭 줄임
2. Predicate Pushdown (조건 밀어내기)
    - WHERE age > 30 조건이 있으면 데이터 소스(파일/DB) 단계에서 미리 필터링
    - 스키마를 모르면: 1억 건 전부 executor 메모리로 올린 뒤 
      거기서 country = 'KR' 필터링
    - 네트워크와 메모리로 올라오는 데이터 양 자체를 줄임
    - executor 간 shuffle 데이터량 감소 → 네트워크 병목 제거
3. Join 전략 최적화
    - 각 테이블의 컬럼 타입과 크기를 알기 때문에 
      작은 테이블은 Broadcast Join으로 자동 전환
    - 스키마/통계를 모르면: 두 테이블 모두 shuffle (전체 데이터를 네트워크로 재분배)
	- 스키마/통계를 알면: countries가 10KB라는 걸 앎 → 전체 executor에 통째로 복사(broadcast)
    - 큰 테이블끼리면 Shuffle Hash Join 또는 Sort Merge Join 선택
    - 한쪽이 메모리에 들어올 수 있는 크기 → Shuffle Hash Join
	- 둘 다 크면 → Sort Merge Join
```
### (2) 핵심 구성요소 
- SparkSession - Spark SQL의 진입점. SQL 쿼리 실행, DataFrame 생성 등 모든 작업의 시작점
- Catalyst Optimizer - SQL 쿼리를 파싱해서 최적 실행 계획으로 자동 변환하는 쿼리 최적화 엔진
- Temporary View - DataFrame을 SQL에서 테이블처럼 쓸 수 있게 등록하는 논리적 뷰

### (3) Catalyst Optimizer
- SQL이나 DataFrame 코드를 작성하면 Catalyst가 내부적으로 실행 계획을 분석
- 불필요한 컬럼 제거, 조건 pushdown(필터를 가능한 한 앞단에서 처리), 조인 순서 최적화 등을 자동 수행
- 개발자가 수동으로 최적화하지 않아도 됨 → RDD 대비 생산성 대폭 향상
- Catalyst Optimizer의 실행 계획 단계(Parsed → Analyzed → Optimized → Physical Plan)

### (4) 커넥터 

**배경**
- 기존에는 각 데이터 소스마다 다른 라이브러리, 다른 API 사용
- Spark SQL은 Data Source API를 통해 어디서 읽든 DataFrame/SQL로 통일

**지원하는 데이터 소스**
- 파일 : Parquet, ORC, JSON, CSV, Avro
- 데이터베이스 : MySQL, PostgreSQL, Oracle (JDBC 커넥터)
- 클라우드 스토리지 : S3, GCS, ADLS
- 데이터 레이크 : Delta Lake, Iceberg, Hudi
- 스트리밍 : Kafka (Structured Streaming과 결합)

### (5) 스파크SQL vs DataFrame
- Spark SQL과 DataFrame은 둘 다 Spark의 상위 레이어(High-Level API)에 속한다.
```
┌─────────────────────────────────────────────┐
│           High-Level API Layer              │
│  Spark SQL  │  DataFrame  │  Dataset        │
│  MLlib      │  GraphX     │  Structured     │
│             │             │  Streaming      │
├─────────────────────────────────────────────┤
│         Catalyst Optimizer                  │
│     (공통 최적화 엔진 - 여기서 통합됨)          │
├─────────────────────────────────────────────┤
│              Spark Core                     │
│         (RDD, 스케줄링, 메모리관리)            │
├─────────────────────────────────────────────┤
│           Cluster Manager                   │
│    YARN │ Kubernetes │ Standalone │ Mesos   │
└─────────────────────────────────────────────┘
Spark SQL 또는 DataFrame 코드 작성 → Catalyst가 최적화 
→ Spark Core가 RDD로 실행 → Cluster Manager가 리소스 배분
```

스파크SQL vs DataFrame
- DataFrame: 동적으로 컬럼명/조건을 코드로 조합해야 할 때, Python 로직과 섞어야 할 때
- 스파크 SQL: 분석가와 협업, 복잡한 집계/윈도우 함수, 가독성이 중요할 때

성능차이? 
- DataFrame API로 작성하든, SQL 문자열로 작성하든 결국 같은 Logical Plan으로 변환됨
- 최종 실행 계획, 성능 차이 없음
- 그냥 개발자가 어떤 문법을 선호하냐의 차이

실무팁
- 복잡한 전처리는 DataFrame으로 처리
- 결과를 View로 등록
- 집계/분석 쿼리는 SQL로 작성
- 이게 가장 흔한 ETL 패턴

# 2. 스파크SQL 사용

```
1. SparkSession 생성
2. 데이터 읽기 (파일/DB/S3)
3. DataFrame → Temp View 등록
4. SQL 쿼리 실행
5. 결과 저장
```

실무 예시 - 주문 데이터에서 국가별 매출 집계
```python 
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# 1. 세션 생성
spark = SparkSession.builder.appName("sales_etl").getOrCreate()
#`appName` : Spark UI에서 보이는 앱 이름
#`getOrCreate` : 이미 세션이 있으면 재사용, 없으면 새로 생성
  
# 2. 스키마 선언
orders_schema = StructType([
    StructField("order_id",        StringType(),  nullable=False),
    StructField("customer_id",     StringType(),  nullable=False),
    StructField("purchase_amount", DoubleType(),  nullable=True),
    StructField("order_date",      DateType(),    nullable=True),
])

customers_schema = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("country",     StringType(), nullable=True),
])

# 3. 데이터 읽기 - 스키마 적용
orders_df    = spark.read.schema(orders_schema).parquet("s3://bucket/orders/")
customers_df = spark.read.schema(customers_schema).parquet("s3://bucket/customers/")

# 4. View 등록
orders_df.createOrReplaceTempView("orders")
customers_df.createOrReplaceTempView("customers")

# 5. SQL 실행
result_df = spark.sql("""
    SELECT 
        c.country,
        COUNT(o.order_id)      AS order_count,
        SUM(o.purchase_amount) AS total_revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_date >= '2024-01-01'
    GROUP BY c.country
    ORDER BY total_revenue DESC
""")

# 6. 결과 저장
result_df.write.mode("overwrite").parquet("s3://bucket/output/country_sales/")
```


실무에서 자주 쓰는 패턴 추가
- 복잡한 전처리는 DataFrame으로 하고 SQL로 마무리하는 혼용 패턴
```
from pyspark.sql.functions import col, to_date

# 전처리는 DataFrame으로 (동적 처리에 유리)
cleaned_df = orders_df \
    .filter(col("status") == "COMPLETED") \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# 전처리 결과를 View로 등록
cleaned_df.createOrReplaceTempView("cleaned_orders")

# 집계는 SQL로 (가독성)
result_df = spark.sql("""
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        SUM(purchase_amount)            AS monthly_revenue
    FROM cleaned_orders
    GROUP BY 1
    ORDER BY 1
""")
```



# 3. SQL테이블과 뷰

## (1) 메타데이터 관리
> 테이블, 스키마는 어떻게 관리할까? 저장소가 있어야 할텐데...   
> 초기에는 HDFS + Hive Metastore로 관리했고, 클라우드 전환과 멀티 엔진 환경이 되면서 한계가 드러났다.
### 발생한 문제점들 
**1세대 - HDFS + Hive Metastore (2008~)**

- 데이터는 HDFS(분산 파일시스템)에 저장
- Hive Metastore가 테이블 스키마, 위치, 파티션 정보 관리
- Hive가 SQL 실행, MapReduce로 처리
- 이때는 Hive가 데이터도 관리하는 Managed Table이 기본
- 문제 없었던 이유: 어차피 Hive 하나만 쓰니까 데이터 소유권이 Hive에 있어도 괜찮았음

**문제 1 - 클라우드 전환 (2013~)**

- 온프레미스 HDFS → S3/GCS로 이동
- S3는 HDFS와 달리 누구나 접근 가능한 오브젝트 스토리지
- Managed Table로 Spark이 데이터 소유하면 S3 경로가 Spark warehouse 하위로 고정
- 다른 팀이 같은 데이터 접근하려면 Spark 통해야 하는 구조적 문제 발생

**문제 2 - 멀티 엔진 환경 (2015~)**

- Spark, Presto, Athena, Flink 등 여러 엔진이 동시에 등장
- 같은 데이터를 여러 엔진이 읽어야 하는 상황
- Managed Table이면 Spark warehouse에 종속, 다른 엔진 접근 어려움
- External Table + S3 고정 경로로 전환 → 어떤 엔진이든 같은 S3 경로 참조 가능

**문제 3 - Hive Metastore 자체의 한계 (2018~)**

- Hive Metastore는 MySQL/PostgreSQL에 메타데이터 저장 → 단일 장애점
- 스키마 변경(컬럼 추가/삭제) 이력 관리 안됨
- 누가 언제 스키마 바꿨는지 추적 불가
- 테이블 접근 권한 관리가 엔진별로 따로 → 일관성 없음
- 이 문제를 해결하려고 AWS Glue Catalog, Databricks Unity Catalog 등장

**현재 실무 표준**
- 데이터: S3 고정 경로 (External Table)
- 메타스토어: AWS Glue Catalog 또는 Unity Catalog
- 컴퓨팅: Spark, Athena, Presto 등 필요한 엔진 선택
- 스토리지와 컴퓨팅 완전 분리


### 관리형/비관리형 테이블
#### 관리형 테이블 (managed)
- Spark이 지정한 warehouse 경로에 데이터를 저장 (기본: `/user/hive/warehouse/`)
- 데이터 소유권이 Spark/Hive에 있음
- DROP TABLE 하면 S3/HDFS 데이터까지 날아감
```
# 데이터를 Spark warehouse 경로에 저장
# path 지정 없음 → Spark warehouse 경로에 데이터 저장
orders_df.write \
    .mode("overwrite") \
    .saveAsTable("orders")
```
#### 비관리형 테이블 (External)
- 데이터는 내가 지정한 경로(S3 등)에 있고, 메타스토어에 위치만 등록
- 데이터 소유권이 나에게 있음
- DROP TABLE 해도 S3 데이터는 안 날아감
```
# 데이터 위치를 직접 지정
# path 직접 지정 → 해당 경로에 데이터 저장, 메타스토어엔 위치만 등록
orders_df.write \
    .mode("overwrite") \
    .option("path", "s3://bucket/orders/") \
    .saveAsTable("orders")
```
#### 차이점 
- Managed: 테이블 DROP 시 메타데이터 + 실제 데이터 파일 모두 삭제
- External: 테이블 DROP 시 메타데이터만 삭제, 실제 데이터 파일은 그대로 유지

**실무에서 어떤걸 쓰나**
- 실무에서는 거의 무조건 External Table 사용
- 이유: 데이터는 S3에 따로 관리, 메타스토어(Hive/Glue)는 그 위치만 등록
- Spark 클러스터가 날아가도 S3 데이터는 안전
- 여러 팀/시스템이 같은 S3 데이터를 다른 메타스토어로 참조 가능

#### DDL로 선언하는방법
```
-- Managed
CREATE TABLE orders (
    order_id        STRING,
    customer_id     STRING,
    purchase_amount DOUBLE,
    order_date      DATE
);


-- External
spark.sql(""" 
CREATE EXTERNAL TABLE IF NOT EXISTS orders ( 
	order_id STRING, 
	customer_id STRING, 
	purchase_amount DOUBLE, 
	order_date DATE 
	) 
	STORED AS PARQUET 
	LOCATION 's3://bucket/orders/' 
""")

# 해당 테이블이 Managed인지 External인지 확인 
spark.sql("DESCRIBE EXTENDED orders").show(truncate=False)
```

문법차이
- Hive 스타일(`STORED AS`) , Spark SQL 스타일(`USING`) 
- Spark 2.x 이후 도입된 문법, 더 다양한 포맷 지원
- `STORED AS PARQUET` = `USING parquet` 동일한 의미
- `USING parquet` 이 현재 표준 문법
```
-- Hive 스타일 (구버전, Hive 호환)
STORED AS PARQUET
LOCATION 's3://bucket/orders/'

-- Spark SQL 스타일 (현재)
USING parquet
OPTIONS (path 's3://bucket/orders/')
```

## (2) 메타스토어 등록 

**Hive Metastore**
- 온프레미스 또는 자체 구축 Hadoop 환경에서 사용
- 메타데이터를 MySQL/PostgreSQL 같은 RDB에 저장
- Spark 설정에서 Hive Metastore 주소를 직접 지정해서 연동

```
spark = SparkSession.builder \
    .appName("etl") \
    .config("spark.sql.warehouse.dir", "s3://bucket/warehouse/") \
    .config("hive.metastore.uris", "thrift://metastore-host:9083") \
    .enableHiveSupport() \
    .getOrCreate()
```

- `enableHiveSupport()` : Hive Metastore 연동 활성화
- `hive.metastore.uris` : Hive Metastore 서버 주소
- `spark.sql.warehouse.dir` : Managed Table 저장 기본 경로


**AWS Glue Catalog**
- AWS 환경에서 Hive Metastore 대체
- 별도 서버 없이 AWS가 관리, 장애 걱정 없음
- EMR, Athena, Spark 모두 같은 Glue Catalog 참조 가능
```
spark = SparkSession.builder \
    .appName("etl") \
    .config("spark.sql.warehouse.dir", "s3://bucket/warehouse/") \
    .config("hive.metastore.client.factory.class",
"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .enableHiveSupport() \
    .getOrCreate()
```
- `hive.metastore.client.factory.class` : Glue Catalog를 Hive Metastore처럼 사용하게 해주는 클래스


**실무 흐름 - Glue Catalog 기준**
- S3에 Parquet 파일 저장
- Glue Catalog에 External Table 등록
- Spark에서 `spark.sql("SELECT * FROM orders")` 로 바로 접근
- 동시에 Athena에서도 같은 테이블 SQL 가능


**Hive Metastore vs Glue Catalog 선택 기준**
- AWS 환경이면 Glue Catalog
- 온프레미스 또는 멀티 클라우드면 Hive Metastore
- Databricks 환경이면 Unity Catalog


## (3) 뷰

### 개념 
- 뷰는 실제 데이터를 저장하지 않고 SQL 쿼리를 저장해두는 논리적 객체

**왜 필요한가**
- 복잡한 SQL을 매번 다시 작성하지 않고 재사용
- 특정 컬럼/데이터만 노출해서 접근 제어
- 중간 변환 결과를 이름 붙여서 다음 쿼리에서 참조

### 종류
- Temp View: 현재 SparkSession 내에서만 유효, 세션 종료 시 사라짐
- Global Temp View: 같은 Spark 앱 내 모든 SparkSession에서 접근 가능, 앱 종료 시 사라짐
- Permanent View: 메타스토어에 저장, 세션/앱 종료해도 유지

### 선언방법
```
# Temp View
orders_df.createOrReplaceTempView("orders_view")


# Global Temp View
orders_df.createOrReplaceGlobalTempView("orders_global_view")


# Permanent View (메타스토어에 저장)
spark.sql("""
    CREATE OR REPLACE VIEW orders_summary AS
    SELECT country, SUM(purchase_amount) AS total_revenue
    FROM orders
    GROUP BY country
""")
```

- 뷰 선언 후 SQL문으로 활용
```
# Temp View
spark.sql("SELECT * FROM orders_view")

# Global Temp View - 반드시 global_temp prefix 필요
spark.sql("SELECT * FROM global_temp.orders_global_view")

# Permanent View - 일반 테이블처럼 접근
spark.sql("SELECT * FROM orders_summary")
```

**실무 사용 패턴**
- ETL 중간 단계 결과 → Temp View
- 여러 Job에서 공통으로 쓰는 집계 → Permanent View
- Global Temp View는 실무에서 거의 안 씀
#### (예시1) Temp View - ETL 중간 단계
- 복잡한 전처리 결과를 다음 단계에서 재사용할 때
```
# 원본 데이터 로드
raw_orders_df = spark.read.parquet("s3://bucket/orders/")
raw_customers_df = spark.read.parquet("s3://bucket/customers/")

# 전처리 후 Temp View 등록
raw_orders_df \
    .filter("status = 'COMPLETED'") \
    .createOrReplaceTempView("cleaned_orders")

raw_customers_df \
    .filter("is_active = true") \
    .createOrReplaceTempView("active_customers")

# 이후 SQL에서 전처리 결과 재사용
joined_df = spark.sql("""
    SELECT o.order_id, o.purchase_amount, c.country, c.tier
    FROM cleaned_orders o
    JOIN active_customers c ON o.customer_id = c.customer_id
""")

joined_df.createOrReplaceTempView("enriched_orders")

# 또 다음 단계에서 enriched_orders 재사용
daily_summary = spark.sql("""
    SELECT 
        order_date,
        country,
        tier,
        SUM(purchase_amount) AS revenue
    FROM enriched_orders
    GROUP BY order_date, country, tier
""")
```

#### (예시2) Permanent View - 여러 Job 공통 집계

매일 여러 리포트 Job이 공통으로 참조하는 집계가 있을 때
```
-- 한번만 선언해두면 영구적으로 사용 가능
CREATE OR REPLACE VIEW monthly_revenue_by_country AS
SELECT
    DATE_TRUNC('month', order_date) AS month,
    country,
    SUM(purchase_amount)            AS total_revenue,
    COUNT(order_id)                 AS order_count
FROM orders
GROUP BY 1, 2;


# 마케팅 팀 Job
marketing_df = spark.sql("""
    SELECT * FROM monthly_revenue_by_country
    WHERE country = 'KR'
""")

# 재무 팀 Job
finance_df = spark.sql("""
    SELECT month, SUM(total_revenue) 
    FROM monthly_revenue_by_country
    GROUP BY month
""")

# 경영진 리포트 Job
exec_df = spark.sql("""
    SELECT country, SUM(total_revenue)
    FROM monthly_revenue_by_country
    WHERE month >= '2024-01-01'
    GROUP BY country
""")
```

**실무에서 Permanent View가 특히 중요한 이유**
- 마케팅/재무/경영진이 각자 SQL 짜면 집계 기준이 달라져서 숫자가 안 맞는 상황 발생
- Permanent View로 집계 로직을 중앙화하면 이 문제 해결
- 실무에서 이걸 "Single Source of Truth" 라고 부름


## (4) SQL테이블 캐싱

> 테이블/뷰 데이터를 executor 메모리에 올려두고 반복 접근 시 디스크/S3 읽기를 생략하는 것


**왜 필요한가**
- 같은 테이블을 여러 쿼리에서 반복 읽으면 매번 S3 I/O 발생
- 캐싱하면 첫 번째 읽기 이후 메모리에서 바로 접근
- 반복 집계, 여러 Join에서 공통으로 쓰는 테이블에 효과적

**캐싱 방법 2가지**
```python
# SQL 방식
spark.sql("CACHE TABLE orders")

# DataFrame 방식
orders_df.cache()

# 캐시 여부 확인 
spark.catalog.isCached("orders")
```

Lazy vs Eager 캐싱
- Spark 3.0에서는 테이블을 `LAZY`로 지정 가능
- 테이블 바로 캐싱하지 않고 처음 사용하는 시점에서 캐싱
- Eager 캐싱: 선언하는 즉시 S3에서 데이터를 읽어서 메모리에 올림
- Lazy 캐싱 : 선언 시점에 아무 작업 안 함
- storageLevel 옵션
	- 캐시를 어디에 저장할지 지정
	- `MEMORY_ONLY` : 메모리에만 저장, 메모리 부족 시 캐시 날아감
	- `MEMORY_AND_DISK` : 메모리 부족 시 디스크로 넘침, 캐시 유지됨
	- `DISK_ONLY` : 디스크에만 저장
```
# Lazy 캐싱 (기본값) - 실제 쿼리 실행 시점에 캐시
spark.sql("CACHE TABLE orders")

# Eager 캐싱 - 선언 즉시 캐시 (시간이 걸림)
spark.sql("CACHE TABLE orders 
OPTIONS ('storageLevel' 'MEMORY_AND_DISK')")

# DataFrame Eager 캐싱
orders_df.persist()
```


캐시 해제
```
# 특정 테이블 캐시 해제
spark.sql("UNCACHE TABLE orders")

# 전체 캐시 해제
spark.sql("CLEAR CACHE")

# DataFrame 캐시 해제
orders_df.unpersist()
```


**실무 사용 패턴**
- 하나의 Job 안에서 같은 테이블을 3번 이상 Join/집계할 때 캐싱
- 작은 dimension 테이블(국가코드, 카테고리 등) 캐싱
- Job 끝나면 반드시 `unpersist()` 호출해서 메모리 반환
- 너무 큰 테이블 캐싱하면 OOM 발생 → 테이블 크기 확인 후 결정


# 4. DataFrame과 데이터소스 
> Spark SQL만으로 읽기/쓰기를 완결할 수도 있다. 하지만 Dataframe이 더 관리가 쉬움   
> SQL 문자열은 런타임에 에러가 나고, DataFrame은 코드 작성 시점에 에러를 잡을 수 있다.  

## (1) DataFrame 함수

### 배경
- Spark 초기에는 포맷별로 읽는 방법이 달랐음
- CSV 읽는 API, JSON 읽는 API, Parquet 읽는 API가 제각각
- DataFrameReader/Writer로 통일된 인터페이스 제공

### DataFrameReader - 읽기
- `spark.read` 로 접근
- `Spark.read`: 정적 데이터 소스 읽기
- `Spark.readStream`: 스트리밍 소스 읽기
```python
df = spark.read \
    .format("parquet") \        # 포맷 지정
    .option("mergeSchema", "true") \  # 옵션 지정
    .schema(order_schema) \     # 스키마 지정
    .load("s3://bucket/orders/")  # 경로
```

단축 문법도 동일한 동작

```python
df = spark.read.parquet("s3://bucket/orders/")
```


### DataFrameWriter - 쓰기
- `df.write` 로 접근
```python
df.write \
    .format("parquet") \
    .mode("overwrite") \   # overwrite / append / ignore / errorIfExists
    .option("compression", "snappy") \
    .partitionBy("country") \   # 파티션 컬럼 지정
    .save("s3://bucket/output/")
```

**mode 옵션 4가지**
- `overwrite` : 기존 데이터 삭제 후 저장
- `append` : 기존 데이터에 추가
- `ignore` : 이미 있으면 아무것도 안 함
- `errorIfExists` : 이미 있으면 에러 (기본값)
```
overwrite - 매일 전체 재생성하는 집계 테이블
- 일별 전체 매출 집계, 월별 리포트처럼 매번 전체를 새로 계산
- 어제 데이터가 오늘 데이터로 완전히 교체됨
- 실수로 중복 실행해도 결과가 동일 (멱등성 보장)

append - 매시간 새로 발생하는 이벤트 로그
- 클릭 로그, 주문 이벤트처럼 계속 쌓이는 데이터
- 기존 데이터는 유지하고 새 데이터만 추가
- 중복 실행 시 데이터 중복 발생 주의
  
ignore - 초기 데이터 세팅, 이미 있으면 스킵
- 마이그레이션이나 초기 데이터 로드 시
- 이미 데이터가 있으면 덮어쓰지 않고 그냥 넘어감
- 파이프라인 재실행 시 안전하게 
  
errorIfExists - 실수로 덮어쓰면 안 되는 중요 테이블
- 한번만 생성해야 하는 기준 테이블, 마스터 데이터
- 이미 존재하면 에러를 내서 파이프라인 중단
- 실수로 재실행했을 때 데이터 손실 방지용 안전장치
```


**실무에서 왜 중요한가**
- 포맷이 CSV에서 Parquet으로 바뀌어도 `.format()` 만 바꾸면 됨
- 읽기/쓰기 옵션이 코드에 명시적으로 보여서 유지보수 편함
- ETL 파이프라인에서 소스/타겟 포맷이 자주 바뀌는데 변경 포인트가 한 곳으로 집중됨


## (2) 데이터 소스

**1. Parquet - 실무 표준**
- 컬럼 기반 저장 포맷, Predicate Pushdown/Column Pruning 최적화에 최적
- 스키마 내장, 압축률 높음
- 대용량 분석 쿼리에 가장 적합

```python
df = spark.read \
    .schema(schema) \
    .parquet("s3://bucket/orders/")
```


**2. JSON - API 연동, 로그 수집**
- 반정형 데이터, 중첩 구조(nested) 지원
- 스키마 추론 가능하지만 느림 → 직접 선언 권장
- 텍스트 기반이라 파일 크기 큼

```python
df = spark.read \
    .schema(schema) \
    .option("multiLine", "true") \  # 여러 줄에 걸친 JSON 객체 처리
    .json("s3://bucket/events/")
```


**3. CSV - 레거시 시스템, 엑셀 데이터**
- 가장 범용적이지만 스키마 없음, 타입 추론 부정확
- 대용량에 부적합, 분석용보다 수집/전달용
- 실무에서 소스 데이터로 받을 때만 사용, 저장은 Parquet으로 변환

```python
df = spark.read \
    .schema(schema) \
    .option("header", "true") \     # 첫 행을 컬럼명으로
    .option("delimiter", ",") \     # 구분자
    .option("nullValue", "NULL") \  # null 처리
    .csv("s3://bucket/legacy/")
```


**4. Avro - Kafka 메시지, 스트리밍**
- 행 기반 저장 포맷, 스키마 내장
- Kafka와 궁합이 좋아서 스트리밍 파이프라인에서 자주 사용
- Schema Registry와 연동해서 스키마 버전 관리
```python
df = spark.read \
    .format("avro") \
    .load("s3://bucket/kafka-output/")
```



**5. ORC - Hive 환경**
- 컬럼 기반, Parquet과 유사하지만 Hive에 최적화
- Hive 기반 레거시 환경에서 주로 사용
- 신규 프로젝트에서는 Parquet 선호
```python
df = spark.read \
    .schema(schema) \
    .orc("s3://bucket/hive-data/")
```


**6. Binary - 이미지, 오디오, ML 데이터**
- 바이너리 파일을 그대로 읽어서 DataFrame으로
- ML 파이프라인에서 이미지/오디오 처리할 때 사용
- 컬럼: path, modificationTime, length, content(바이트 배열)
```python
df = spark.read \
    .format("binaryFile") \
    .option("pathGlobFilter", "*.png") \  # 특정 확장자만 필터
    .load("s3://bucket/images/")
```


**실무 포맷 선택 기준**
- 배치 ETL 저장/분석 → Parquet
- Kafka 스트리밍 → Avro
- 외부 데이터 수신 → CSV/JSON으로 받아서 Parquet으로 변환
- Hive 레거시 환경 → ORC
- ML 데이터 → Binary

### JSON 멀티라인
**기본값 - 한 줄에 하나의 JSON 객체 (JSONL 형식)**
```json
{"order_id": "001", "amount": 100}
{"order_id": "002", "amount": 200}
{"order_id": "003", "amount": 300}
```
- Spark 기본값으로 읽힘
- 각 줄이 독립적인 레코드
- 로그 수집, Kafka 메시지 저장할 때 이 형식

**multiLine - 여러 줄에 걸친 하나의 JSON 객체**
```json
{
    "order_id": "001",
    "amount": 100,
    "customer": {
        "name": "eddie",
        "country": "KR"
    }
}
```
- 사람이 읽기 좋게 들여쓰기된 형식
- REST API 응답, 설정 파일이 이 형식
- `multiLine=true` 없으면 파싱 에러 발생


**JSON 읽기 option**
```python
# 기본값 - JSONL
df = spark.read.json("s3://bucket/logs/")

# multiLine - 들여쓰기된 JSON
df = spark.read \
    .option("multiLine", "true") \
    .json("s3://bucket/api-response/")
```
