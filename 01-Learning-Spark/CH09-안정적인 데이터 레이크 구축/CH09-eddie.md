
# 01.스토리지
### (1)확장성 및 성능
- 저장공간 확장이 가능해야함
- 읽기/처리 및 지연시간 제공가능해야함
### (2)트랜잭션 지원
- 데이터 품질 보장을 위한 ACID 트랜잭선에 지원 ex) delta, iceberg
### (3)다양한 데이터 형식 지원
- 정형/반정형/비정형 포맷 지원
### (4)다양한 워크로드 지원
- SQL워크로드 지원
- ETL 워크로드 지원
- 모니터링 워크로드 지원
- ML/AL 워크로드 지원
### (5) 개방성
- 오픈포맷

# 02. 데이터베이스
### (1) OLTP
- 높은 동시성, 짧은 지연시간
### (2) OLAP
- 높은 처리량 스캔
- 복잡한 쿼리

### (3)spark로 DB 연동
- JDBC 드라이버 jar 파일로 액세스 가능(Ch05참조)

### (4)데이터베이스 한계
- 데이터 크기 증가: GB단위 → TB,PB 단위로 증가 (사용자행동데이터 수집)
- 분석 데이터 포맷의 다양성 증가 : 머신러닝,딥러닝 도입으로 인한 활용의 한계(비정형, 반정형)
	- 구조화된 데이터만 처리 가능
- 데이터베이스를 확장성의 한계 - 비용
	- 분산처리 불가능
	- 스케일 비용이 매우 높음
- 비SQL기반 분석 지원 못함 - SQL에 기반한 처리방식

# 03.데이터 레이크
> 스파크가 데이터레이크와 통합해서 워크로드를 확장해 나가는 방식을 살펴봐야함
### (1) 데이터 베이스의 한계 극복
- 데이터베이스의 한계를 극복하기 위해 발생
- S3/HDFS에 원시 데이터를 그냥 던져놓는 방식
- 처리엔진 다각화
	- 배치처리 - Spark
	- 스트림처리 - Spark Structured Streaming, Flink
	- 머신러닝 - MLlib, R
- 유연성 확장

### (2)한계
- 데이터 품질 보장 불가능
	- 롤백 불가능
	- 일관성 없음
- 문제: ACID 없음, 스키마 없음, 데이터 품질 보장 없음

# 04.데이터 레이크하우스
### (1) 트랜잭션 지원
- ACID 트랜잭션 지원
- 스키마적용 및 거버넌스 
	- 잘못된 스키마가 있는 데이터 삽입 방지 가능
- 오픈 포맷 형식
	- 포맷 통일로 다양한 툴 적용 가능
- 다양한 워크로드 지원
	- SQL, 스트리밍분석, ML분석 가능
- UPSERT, DELETE wldnjs
	- CDC, SCD작업 가능
	- SCD (Slowly Changing Dimension)

### (2)SCD 잠깐 
데이터 웨어하우스에서 dimension 테이블(고객, 상품, 직원 등)은 시간이 지나면서 값이 바뀜. 근데 그 변경 이력을 어떻게 관리하느냐에 따라 분석 결과가 완전히 달라짐. 

"2023년 3월에 VIP였던 고객의 구매 금액"을 분석하려면 그 시점의 등급 기준으로 조인해야 함. Type 1이면 현재 등급으로만 조인돼서 과거 분석이 틀려짐.

Type 2 테이블 구조:

```
customer_sk  | customer_id | grade | start_date | end_date   | is_current
-------------|-------------|-------|------------|------------|------------
1            | C001        | BASIC | 2022-01-01 | 2023-02-28 | False
2            | C001        | VIP   | 2023-03-01 | NULL       | True
```

> "Type 2를 기본으로 쓰고, 이력이 필요 없는 단순 참조 테이블은 Type 1"이라고 말하면 충분

**타입별 정리**
- **Type 0** — 변경 무시. 최초 값 고정. 거의 안 씀
- **Type 1** — 덮어쓰기. 이력 없음. 가장 단순
- **Type 2** — 이력 전체 보존. 행을 새로 추가. 실무에서 가장 많이 씀
- **Type 3** — 현재값 + 이전값 컬럼 추가. 이력이 1단계만 필요할 때
- **Type 4** — 현재 테이블 따로 + 이력 테이블 따로 분리
- **Type 6** — Type 1 + 2 + 3 혼합. current_flag + 이력 행 + prev 컬럼 동시에

**실제 파이프라인에서 어디서 처리하냐**
- Staging 레이어에서 source 데이터 수신
- Silver/DW 레이어 진입 시 SCD 로직 적용
- 보통 Spark나 dbt로 구현

Spark 기준 Type 2 upsert 흐름:

```
existing = spark.read.table("dim_customer")
incoming = spark.read.table("stg_customer")

# 변경된 레코드 감지
changed = incoming.join(existing.filter("is_current = true"), "customer_id")
                  .filter(incoming.grade != existing.grade)

# 기존 행 close (end_date 업데이트, is_current = False)
# 신규 행 insert (start_date = today, end_date = NULL, is_current = True)
```
**등장인물**

```
운영 DB        → 쇼핑몰이 실제로 쓰는 MySQL
stg_customer  → 오늘 운영 DB에서 뽑은 스냅샷 (매일 덮어써짐)
dim_customer  → 이력이 누적되는 DW 테이블
```

---

**STEP 1 — 운영 DB 원본**

```
customer_id | grade
------------|------
C001        | BASIC
C002        | VIP
```

---

**STEP 2 — 오늘 배치 실행. C001이 VIP로 바뀐 상태로 stg에 적재**

```
customer_id | grade
------------|------
C001        | VIP     ← 바뀜
C002        | VIP
```

---

**STEP 3 — 어제까지 dim_customer 상태**

```
customer_sk | customer_id | grade | start_date | end_date | is_current
------------|-------------|-------|------------|----------|------------
1           | C001        | BASIC | 2024-01-01 | NULL     | True
2           | C002        | VIP   | 2024-01-01 | NULL     | True
```

---

**STEP 4 — 비교. C001 grade 바뀐 거 감지**

```
inc.customer_id | inc.grade | cur.grade
----------------|-----------|----------
C001            | VIP       | BASIC     ← 잡힘
```

---

**STEP 5 — C001 기존 행 Close**

```
customer_sk | customer_id | grade | start_date | end_date   | is_current
------------|-------------|-------|------------|------------|------------
1           | C001        | BASIC | 2024-01-01 | 2024-06-01 | False  ← 닫힘
```

---

**STEP 6 — C001 신규 행 Insert**

```
customer_sk | customer_id | grade | start_date | end_date | is_current
------------|-------------|-------|------------|----------|------------
3           | C001        | VIP   | 2024-06-01 | NULL     | True  ← 추가
```

---

**STEP 7 — 3개 합쳐서 저장**

```
customer_sk | customer_id | grade | start_date | end_date   | is_current
------------|-------------|-------|------------|------------|------------
1           | C001        | BASIC | 2024-01-01 | 2024-06-01 | False
2           | C002        | VIP   | 2024-01-01 | NULL       | True
3           | C001        | VIP   | 2024-06-01 | NULL       | True
```

S3에 parquet로 저장. 내일 배치 돌면 이 테이블이 existing이 됨.

---

**활용**(SCD Type 2)

```
-- 2024년 3월 기준 C001 등급 조회
select grade
from dim_customer
where customer_id = 'C001'
and start_date <= '2024-03-01'
and (end_date > '2024-03-01' or end_date is null)
-- 결과 → BASIC
```


### (3)레이크하우스 선택지
- Hudi (2016, Uber)
    - Uber의 실시간 데이터 수정 필요 때문에 탄생
    - 드라이버 위치, 요금 데이터를 S3에서 upsert 해야 했음
    - 기존 Parquet은 immutable이라 수정 불가 → Hudi가 해결
- Iceberg (2017, Netflix)
    - Netflix의 페타바이트급 테이블 관리 문제 때문에 탄생
    - Hive 메타스토어가 대형 파티션 테이블에서 느리고 불안정했음
    - 메타데이터 관리 자체를 재설계한 것이 핵심
- Delta Lake (2019, Databricks)
    - Databricks가 Spark 기반 레이크하우스 표준을 만들기 위해 탄생
    - 셋 중 가장 늦게 나왔지만 Spark와의 통합이 가장 자연스러움

|항목|Hudi|Iceberg|Delta Lake|
|---|---|---|---|
|탄생 목적|Upsert/CDC|대형 테이블 메타 관리|Spark 통합 레이크하우스|
|강점|실시간 upsert|멀티엔진 호환성|Spark 친화성|
|Time Travel|있음|있음|있음|
|엔진 호환|보통|최고 (Spark/Flink/Trino)|Spark 중심|
|채택 주체|Uber, 핀테크|Netflix, Apple|Databricks 고객사|



# 04. Delta Lake 기반 Spark 운영

## 살펴볼 것들

- 아파치 스파크를 사용하여 델타 레이크 테이블 읽기 및 쓰기
- 델타 레이크에서 ACID 보장으로 동시 배치 처리 및 스트리밍 쓰기를 허용하는 방법
- 델타 레이크가 모든 쓰기에 스키마를 적용하고 명시적 스키마 진화를 허용하여 더 나은 데이터 품질을 보장하는 방법
- 업데이트, 삭제 및 병합 작업을 사용하여 복잡한 데이터 파이프라인 구축 (모든 작업은 ACID 보장)
- 델타 레이크 테이블을 수정한 작업 기록을 감사하고, 이전 버전의 테이블을 쿼리하여 과거로 여행

---

## (1) 델타레이크와 스파크 구성

### 왜 이걸 하는가

Delta Lake는 Parquet 파일 위에 트랜잭션 로그(`_delta_log`)를 얹은 구조다. Spark가 이 로그를 읽고 쓰려면 delta 패키지가 classpath에 있어야 하고, SparkSession에 Delta 전용 확장과 카탈로그를 등록해야 한다. 이걸 안 하면 이후 모든 Delta 작업이 동작하지 않는다.

### 전체 흐름

패키지 의존성 추가 → SparkSession에 Delta 확장 등록 → 카탈로그를 Delta로 설정 → 이후 모든 Delta 작업의 기반이 됨

> 엔지니어 독백: SparkSession 만들 때 delta extension 안 넣으면 `df.write.format('delta')` 호출 자체가 터진다. 나중에 에러 보고 왜 안 되지 헤매지 말고 제일 먼저 세팅하고 시작해야 하는 부분.

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("DeltaLakeOps")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

### 코드 흐름 설명

- `configure_spark_with_delta_pip`: 로컬 환경에서 delta-spark JAR을 자동으로 classpath에 주입해주는 헬퍼 함수. 수동으로 `--packages` 옵션 안 써도 됨
- `spark.sql.extensions`: Delta가 제공하는 SQL 문법 확장(MERGE, RESTORE, DESCRIBE HISTORY 등)을 Spark SQL 엔진에 등록. 이걸 안 하면 MERGE 문 자체가 파싱 에러남
- `spark.sql.catalog.spark_catalog`: 기본 카탈로그를 Delta용으로 교체. 이걸 안 하면 `CREATE TABLE USING DELTA` 같은 DDL이 동작 안 함
- `.getOrCreate()`: 이미 세션이 있으면 재사용, 없으면 새로 생성. 멱등하게 호출 가능

---

## (2) 델타레이크 테이블에 데이터 로드

### 왜 이걸 하는가

실무에서 e-commerce 플랫폼의 사용자 테이블을 Delta로 초기 적재하는 상황을 생각해봐. S3나 HDFS에 CSV/Parquet으로 쌓여있던 데이터를 Delta 포맷으로 변환하면 `_delta_log`가 생기면서 이후 모든 변경이 버전으로 추적된다. 초기 적재가 version 0이 되고 이후 INSERT, UPDATE, DELETE가 전부 version 1, 2, 3...으로 쌓인다.

### 전체 흐름

DataFrame 생성 → Delta 포맷으로 write → 경로 기반 또는 카탈로그 테이블명으로 read → 결과 확인

> 엔지니어 독백: 처음 write할 때 overwrite로 시작하는 게 맞다. 이미 경로에 파일이 있으면 `_delta_log`가 깨끗하게 초기화되고, append로 시작하면 이전 찌꺼기 로그랑 섞여서 나중에 history가 꼬인다.

```python
from pyspark.sql import Row
from datetime import date

# 실무 상황: e-commerce 플랫폼 사용자 테이블 초기 적재
data = [
    Row(user_id=1, name="Alice", age=30, signup_date=date(2024, 1, 10), plan="premium"),
    Row(user_id=2, name="Bob",   age=25, signup_date=date(2024, 2, 5),  plan="free"),
    Row(user_id=3, name="Carol", age=28, signup_date=date(2024, 3, 1),  plan="premium"),
]

df = spark.createDataFrame(data)

DELTA_PATH = "/tmp/delta/users"

df.write \
  .format("delta") \
  .mode("overwrite") \
  .save(DELTA_PATH)

delta_df = spark.read.format("delta").load(DELTA_PATH)
delta_df.show()
```

출력:

```
+-------+-----+---+-----------+-------+
|user_id| name|age|signup_date|   plan|
+-------+-----+---+-----------+-------+
|      1|Alice| 30| 2024-01-10|premium|
|      2|  Bob| 25| 2024-02-05|   free|
|      3|Carol| 28| 2024-03-01|premium|
+-------+-----+---+-----------+-------+
```

### 코드 흐름 설명

- `Row(...)`: 스키마를 별도로 정의하지 않고 Row 객체로 데이터를 정의. Spark가 타입을 자동 추론함. 실무에서는 StructType으로 명시하는 게 더 안전
- `.format("delta")`: Parquet이 아닌 Delta 포맷으로 저장 지시. 이 옵션 하나로 데이터 파일과 함께 `_delta_log/` 디렉토리가 생성됨
- `.mode("overwrite")`: 경로에 기존 데이터가 있으면 전부 덮어씀. 최초 적재 시 항상 overwrite가 안전
- `.save(DELTA_PATH)`: 실제 디스크에 write 실행. 이 시점에 트랜잭션 로그 version 0 파일 생성

```python
# 카탈로그에 테이블로 등록해서 SQL로 조회하는 방식
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS users
    USING DELTA
    LOCATION '{DELTA_PATH}'
""")

spark.sql("SELECT * FROM users WHERE plan = 'premium'").show()
```

출력:

```
+-------+-----+---+-----------+-------+
|user_id| name|age|signup_date|   plan|
+-------+-----+---+-----------+-------+
|      1|Alice| 30| 2024-01-10|premium|
|      3|Carol| 28| 2024-03-01|premium|
+-------+-----+---+-----------+-------+
```

- `CREATE TABLE IF NOT EXISTS`: 이미 테이블이 있으면 skip. 파이프라인 재실행 시 에러 없이 멱등하게 동작
- `LOCATION`: 이미 데이터가 있는 경로를 카탈로그에 연결. 데이터를 새로 만드는 게 아니라 기존 Delta 파일을 테이블명으로 참조하는 것
- 이후 `spark.sql("SELECT * FROM users")`처럼 절대 경로 대신 테이블명으로 쿼리 가능

---

## (3) 델타레이크 테이블에 스트림 로드

### 왜 이걸 하는가

실무에서 Kafka나 Kinesis에서 실시간으로 들어오는 사용자 이벤트 로그(클릭, 구매, 로그인 등)를 Delta 테이블에 적재하는 상황을 생각해봐. 일반 Parquet sink는 동시에 배치와 스트리밍이 같은 경로에 쓰면 파일 충돌이 발생하지만, Delta는 낙관적 동시성 제어(Optimistic Concurrency Control)로 이걸 원자적으로 처리한다.

### 전체 흐름

소스 스트림 정의 → 스키마 명시 → Delta sink로 writeStream → checkpoint 경로 설정 → 실행 → 동시에 배치 read 가능

> 엔지니어 독백: `checkpointLocation` 빠뜨리면 재시작할 때마다 오프셋을 처음부터 다시 읽어서 중복 적재가 터진다. checkpoint는 Spark가 "어디까지 처리했는지"를 파일로 기록하는 장치고, exactly-once 보장의 핵심이다.

```python
# 실무 상황: 사용자 이벤트 로그가 JSON 파일로 특정 경로에 계속 떨어지는 상황
# (Kafka Connect가 S3에 JSON 파일로 내려쓰는 패턴)

STREAM_SOURCE = "/tmp/stream/user_events"
CHECKPOINT    = "/tmp/delta/users_stream_checkpoint"

streaming_df = (
    spark.readStream
    .format("json")
    .schema(delta_df.schema)    # 스트리밍은 schema inference 불가 → 명시 필수
    .load(STREAM_SOURCE)
)

query = (
    streaming_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .start(DELTA_PATH)
)

query.awaitTermination()
```

### 코드 흐름 설명

- `spark.readStream`: 배치 read가 아닌 스트리밍 read. 소스 경로를 지속적으로 감시하면서 신규 파일이 생기면 micro-batch로 처리
- `.schema(delta_df.schema)`: 스트리밍은 schema inference를 지원하지 않아서 반드시 명시해야 함. 앞서 만든 delta_df의 스키마를 재사용해서 일관성 유지
- `.format("json")`: 소스 파일 포맷. 신규 JSON 파일이 STREAM_SOURCE 경로에 생길 때마다 트리거됨
- `.outputMode("append")`: 새로 들어온 행만 Delta 테이블에 추가. 집계 없이 raw 이벤트를 쌓을 때 사용하는 모드
- `.option("checkpointLocation", CHECKPOINT)`: 처리 완료된 파일/오프셋 정보를 이 경로에 저장. 장애 후 재시작 시 여기서 마지막 처리 위치를 읽어서 중복 없이 이어서 처리
- `.start(DELTA_PATH)`: 스트리밍 쿼리 비동기 실행 시작. 바로 리턴하고 백그라운드에서 계속 동작
- `query.awaitTermination()`: 메인 스레드가 종료되지 않도록 블로킹

스트리밍 진행 중 배치 read로 실시간 확인:

```python
# 스트리밍이 돌아가는 동안 별도 배치 쿼리로 현재 적재 상태 확인 가능
spark.read.format("delta").load(DELTA_PATH).count()
```

출력:

```
# 스트리밍 시작 직후
3

# 이벤트 파일 5개 더 들어온 후
8
```

---

## (4) 데이터 손상을 방지하기 위해 Write 시 스키마 적용

### 왜 이걸 하는가

실무에서 상류 팀이 API 응답 구조를 바꾸거나 ETL 코드를 수정하다가 실수로 컬럼을 제거하거나 타입을 바꾸는 일이 생긴다. 이게 그대로 Delta 테이블에 들어오면 하류 분석 쿼리가 전부 깨진다. Schema Enforcement는 이걸 write 시점에 차단해서 데이터 손상을 막아주는 첫 번째 방어선이다.

### 전체 흐름

기존 스키마와 다른 DataFrame 준비 → write 시도 → AnalysisException 발생 → 에러 로그에서 불일치 컬럼 확인 → 상류 수정 or Evolution 처리로 분기

> 엔지니어 독백: 상류 팀이 `age` 컬럼 지우고 `email` 추가한 채로 파이프라인 돌렸다. Delta가 여기서 막아주지 않으면 테이블에 `age`가 null로 가득 차고 `email`은 사라진 채로 하류까지 퍼진다. 에러로 튕겨줘야 상류 팀이 인지하고 수정한다.

```python
# 실무 상황: 상류 팀이 users 테이블 스키마를 변경한 채로 파이프라인 실행
# 기존: user_id, name, age, signup_date, plan
# 변경: user_id, name, email, signup_date, plan (age 제거, email 추가)

bad_schema_data = [
    Row(user_id=4, name="Dave", email="dave@test.com",
        signup_date=date(2024, 4, 1), plan="free")
]
bad_df = spark.createDataFrame(bad_schema_data)

try:
    bad_df.write \
        .format("delta") \
        .mode("append") \
        .save(DELTA_PATH)
except Exception as e:
    print(f"[Schema Enforcement 차단] {e}")
```

에러 로그 원본:

```
AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: ...)

Table schema:
root
 |-- user_id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)         <-- 기존 컬럼, 신규 데이터에서 사라짐
 |-- signup_date: date (nullable = true)
 |-- plan: string (nullable = true)

Data schema:
root
 |-- user_id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- email: string (nullable = true)        <-- 신규 컬럼, 테이블에 없음
 |-- signup_date: date (nullable = true)
 |-- plan: string (nullable = true)

To enable schema migration using DataFrameWriter or DataStreamWriter,
please set: .option("mergeSchema", "true")
```

### 에러 로그 해석

Table schema와 Data schema 두 블록을 비교하는 게 핵심이다. `age`가 Data schema에서 사라졌고 `email`이 새로 생겼다. Delta는 이 불일치를 감지하고 write를 차단한 것이다. 마지막 줄에 `mergeSchema` 옵션 힌트까지 준다. 이 에러를 보면 판단 기준은 두 가지다.

- `age` 제거가 의도적이었는가? → 상류 팀 확인 후 테이블 스키마 재정의 필요
- `email` 추가가 의도적이었는가? → Section 5의 Schema Evolution으로 처리

---

## (5) 변화하는 데이터를 수용하기 위해 Evolution 처리

### 왜 이걸 하는가

비즈니스 요구사항이 변하면서 사용자 테이블에 `email`, `phone`, `referral_code` 같은 컬럼이 점진적으로 추가된다. Schema Evolution은 명시적으로 옵션을 켜서 허용하는 구조인데, 실수로 스키마가 바뀌는 걸 방지하기 위해 default는 off다.

### 전체 흐름

신규 컬럼 포함 DataFrame 준비 → `mergeSchema` 옵션 켜고 write → 기존 row는 신규 컬럼이 null로 채워짐 → 필요하면 UPDATE로 backfill

> 엔지니어 독백: 신규 컬럼 추가는 `mergeSchema`로 허용하지만 기존 컬럼 타입 변경(int → string)은 `mergeSchema`로도 안 된다. 타입 변경은 테이블 재생성 수준의 작업이다. 그리고 기존 row에 null이 깔리는 거 당연한 건데, 하류 분석팀한테 미리 공지 안 하면 갑자기 null 보고 파이프라인 깨진 줄 알고 연락 온다.

```python
# 실무 상황: 마케팅 팀 요청으로 사용자 이메일 컬럼 추가
# 신규 가입자부터 email 수집 시작, 기존 사용자는 null로 유지 후 별도 수집 캠페인 진행 예정

from pyspark.sql import Row

evolved_data = [
    Row(user_id=4, name="Dave", age=32,
        signup_date=date(2024, 4, 1), plan="free",
        email="dave@test.com"),
]
evolved_df = spark.createDataFrame(evolved_data)

evolved_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(DELTA_PATH)

spark.read.format("delta").load(DELTA_PATH).show()
```

출력:

```
+-------+-----+---+-----------+-------+--------------+
|user_id| name|age|signup_date|   plan|         email|
+-------+-----+---+-----------+-------+--------------+
|      1|Alice| 30| 2024-01-10|premium|          null|
|      2|  Bob| 25| 2024-02-05|   free|          null|
|      3|Carol| 28| 2024-03-01|premium|          null|
|      4| Dave| 32| 2024-04-01|   free|dave@test.com|
+-------+-----+---+-----------+-------+--------------+
```

### 코드 흐름 설명

- `evolved_data`: 기존 스키마에 `email` 컬럼이 추가된 데이터. 기존 컬럼은 그대로 유지
- `.option("mergeSchema", "true")`: 이 옵션 하나가 Schema Evolution을 허용하는 스위치. Delta가 `_delta_log`에 새 스키마를 등록하면서 `email` 컬럼을 테이블에 추가
- 기존 row(user_id 1, 2, 3)의 `email` 값은 자동으로 null. 이후 아래처럼 backfill 처리

```python
# 기존 사용자 이메일 backfill
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, DELTA_PATH)

dt.update(
    condition = "user_id = 1",
    set = {"email": "'alice@test.com'"}
)

spark.read.format("delta").load(DELTA_PATH).show()
```

출력:

```
+-------+-----+---+-----------+-------+---------------+
|user_id| name|age|signup_date|   plan|          email|
+-------+-----+---+-----------+-------+---------------+
|      1|Alice| 30| 2024-01-10|premium|alice@test.com|
|      2|  Bob| 25| 2024-02-05|   free|           null|
|      3|Carol| 28| 2024-03-01|premium|           null|
|      4| Dave| 32| 2024-04-01|   free| dave@test.com|
+-------+-----+---+-----------+-------+---------------+
```

전역 설정 방법:

```python
# 매번 write마다 옵션 붙이기 번거로울 때 SparkSession 전역으로 설정
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

단, 전역으로 켜면 의도치 않은 스키마 변경도 통과될 수 있어서 특정 파이프라인 실행 구간에서만 켜고 끄는 방식이 더 안전하다.

---

## (6) 기존 데이터 변환

Delta Lake는 복잡한 데이터 파이프라인을 구축할 수 있는 UPDATE, DELETE 및 MERGE를 지원한다. 실제 사용 사례의 몇 가지 예를 통해 살펴본다.

---

### a. 오류 수정을 위한 데이터 업데이트

#### 왜 이걸 하는가

실무에서 데이터 입력 오류, API 버그, 마이그레이션 오류로 특정 row의 값이 잘못 들어오는 일이 생긴다. 일반 Parquet이었으면 해당 파티션 전체를 재적재해야 하지만 Delta는 UPDATE 한 줄로 특정 조건의 row만 수정할 수 있다. 수정 내역은 트랜잭션 로그에 자동 기록돼서 감사 추적도 가능하다.

> 엔지니어 독백: 가입 시 나이 입력 UI 버그로 user_id=2의 age가 25가 아니라 250으로 들어왔다는 제보가 왔다. Parquet이었으면 해당 파티션 전체 다시 뽑아야 하는데 Delta는 UPDATE 한 줄로 끝난다. 이 수정이 history에 기록되니까 "언제 누가 왜 고쳤는지"도 남는다.

```python
# 실무 상황: 가입 UI 버그로 user_id=2의 age가 250으로 잘못 입력됨
# 정상값 25로 수정 필요

from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, DELTA_PATH)

# 수정 전 확인
spark.read.format("delta").load(DELTA_PATH) \
    .filter("user_id = 2").show()
```

출력:

```
+-------+----+---+-----------+----+-----+
|user_id|name|age|signup_date|plan|email|
+-------+----+---+-----------+----+-----+
|      2| Bob|250| 2024-02-05|free| null|
+-------+----+---+-----------+----+-----+
```

```python
dt.update(
    condition = "user_id = 2",
    set = {"age": "25"}
)

# 수정 후 확인
spark.read.format("delta").load(DELTA_PATH) \
    .filter("user_id = 2").show()
```

출력:

```
+-------+----+---+-----------+----+-----+
|user_id|name|age|signup_date|plan|email|
+-------+----+---+-----------+----+-----+
|      2| Bob| 25| 2024-02-05|free| null|
+-------+----+---+-----------+----+-----+
```

#### 코드 흐름 설명

- `DeltaTable.forPath(spark, DELTA_PATH)`: Delta 테이블 객체를 Python API로 로딩. 이 객체를 통해 UPDATE, DELETE, MERGE 등 DML 실행
- `condition = "user_id = 2"`: SQL WHERE 절과 동일. 이 조건에 해당하는 row만 업데이트 대상
- `set = {"age": "25"}`: 딕셔너리로 컬럼명과 새 값을 지정. 값은 문자열로 표현된 SQL 표현식. 리터럴 숫자는 문자열로 써도 자동 변환

---

### b. 사용자 관련 데이터 삭제

#### 왜 이걸 하는가

GDPR, CCPA 등 개인정보보호법에 따라 사용자가 탈퇴하거나 데이터 삭제를 요청하면 해당 데이터를 삭제해야 할 의무가 있다. Delta의 DELETE는 조건에 맞는 row를 트랜잭션으로 삭제하고 로그에 기록한다. 단, 물리적 파일 삭제는 별도로 VACUUM을 실행해야 한다.

> 엔지니어 독백: user_id=1에서 GDPR Right to Erasure 요청이 들어왔다. DELETE 치면 트랜잭션 로그에 삭제 기록이 남고 read 시 안 보이게 되는데, 실제 Parquet 파일은 아직 디스크에 있다. 법적 완전 삭제를 위해선 이후 VACUUM까지 실행해야 한다. 이 두 단계를 구분 안 하면 감사 받을 때 문제 된다.

```python
# 실무 상황: user_id=1 (Alice) 서비스 탈퇴 + 개인정보 삭제 요청

# 삭제 전 확인
spark.read.format("delta").load(DELTA_PATH) \
    .filter("user_id = 1").show()
```

출력:

```
+-------+-----+---+-----------+-------+---------------+
|user_id| name|age|signup_date|   plan|          email|
+-------+-----+---+-----------+-------+---------------+
|      1|Alice| 30| 2024-01-10|premium|alice@test.com|
+-------+-----+---+-----------+-------+---------------+
```

```python
dt.delete(condition = "user_id = 1")

# 삭제 후 전체 테이블 확인
spark.read.format("delta").load(DELTA_PATH).show()
```

출력:

```
+-------+-----+---+-----------+-------+--------------+
|user_id| name|age|signup_date|   plan|         email|
+-------+-----+---+-----------+-------+--------------+
|      2|  Bob| 25| 2024-02-05|   free|          null|
|      3|Carol| 28| 2024-03-01|premium|          null|
|      4| Dave| 32| 2024-04-01|   free|dave@test.com|
+-------+-----+---+-----------+-------+--------------+
```

```python
# 물리적 파일까지 완전 삭제 (GDPR 완전 준수)
# 기본 retention 7일, 여기선 즉시 삭제를 위해 0으로 설정
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM delta.`{DELTA_PATH}` RETAIN 0 HOURS")
```

#### 코드 흐름 설명

- `dt.delete(condition = "user_id = 1")`: 조건에 맞는 row를 논리적으로 삭제. `_delta_log`에 DELETE 트랜잭션 기록
- 삭제 직후 read 시 user_id=1이 안 보이는 건 Delta가 로그 기반으로 해당 row를 필터링하기 때문
- `RETAIN 0 HOURS`: 보존 기간 없이 즉시 물리 파일 삭제. 운영에서는 최소 7일 유지 권장
- `retentionDurationCheck.enabled false`: 안전장치 해제. 0 HOURS로 VACUUM 치려면 이 설정 필요

---

### c. merge()를 사용해서 테이블에 변경 데이터 UPSERT

#### 왜 이걸 하는가

실무에서 CDC(Change Data Capture) 파이프라인이 MySQL의 변경 데이터를 Delta 테이블로 동기화하는 상황을 생각해봐. 소스에서 기존 row가 수정되면 Delta 테이블도 업데이트해야 하고, 새 row면 삽입해야 한다. 이게 UPSERT 패턴이고 Delta의 merge가 이걸 원자적으로 처리한다.

> 엔지니어 독백: MySQL binlog에서 뽑은 CDC 데이터가 들어왔다. user_id=2는 plan이 free → premium으로 바뀌었고 user_id=5는 신규 가입이다. INSERT/UPDATE를 별도로 처리하면 레이스 컨디션이 생길 수 있는데 merge 하나로 원자적으로 처리 가능하다.

```python
# 실무 상황: MySQL CDC 데이터로 Delta 사용자 테이블 동기화
# user_id=2: plan 변경 (free → premium), user_id=5: 신규 가입

cdc_data = [
    Row(user_id=2, name="Bob",   age=25, signup_date=date(2024, 2, 5),
        plan="premium", email=None),
    Row(user_id=5, name="Eve",   age=22, signup_date=date(2024, 5, 1),
        plan="free",    email="eve@test.com"),
]
cdc_df = spark.createDataFrame(cdc_data)

dt.alias("target") \
  .merge(
      cdc_df.alias("source"),
      "target.user_id = source.user_id"
  ) \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

spark.read.format("delta").load(DELTA_PATH).orderBy("user_id").show()
```

출력:

```
+-------+-----+---+-----------+-------+---------------+
|user_id| name|age|signup_date|   plan|          email|
+-------+-----+---+-----------+-------+---------------+
|      2|  Bob| 25| 2024-02-05|premium|           null|
|      3|Carol| 28| 2024-03-01|premium|           null|
|      4| Dave| 32| 2024-04-01|   free|  dave@test.com|
|      5|  Eve| 22| 2024-05-01|   free|   eve@test.com|
+-------+-----+---+-----------+-------+---------------+
```

#### 코드 흐름 설명

- `.alias("target")`: merge의 기준이 되는 Delta 테이블. 조건 표현식에서 `target.컬럼명`으로 참조
- `.merge(cdc_df.alias("source"), "target.user_id = source.user_id")`: source DataFrame과 user_id로 매칭. 이 조건이 JOIN 조건
- `.whenMatchedUpdateAll()`: 매칭된 row의 모든 컬럼을 source 값으로 업데이트. `set = {"*": "*"}`와 동일
- `.whenNotMatchedInsertAll()`: 매칭 안 된 row를 전체 컬럼 그대로 삽입. `values = {"*": "*"}`와 동일
- `.execute()`: 위 조건들을 하나의 원자적 트랜잭션으로 실행

---

### d. 데이터 삽입 전용 병합을 사용하여 삽입하는 동안 데이터 중복 제거

#### 왜 이걸 하는가

실무에서 Kafka consumer가 at-least-once로 동작하면 같은 이벤트가 두 번 들어올 수 있다. 또는 파이프라인 장애 후 재실행 시 이미 처리된 데이터가 다시 들어오는 경우도 있다. insert-only merge로 이미 있는 row는 skip하면 멱등성이 보장된다.

> 엔지니어 독백: Kafka consumer가 재시작되면서 user_id=3, 4 이벤트가 다시 들어왔다. 그냥 append로 쓰면 중복 row가 생기는데 insert-only merge로 이미 있는 건 skip하고 진짜 신규인 user_id=6만 넣을 수 있다.

```python
# 실무 상황: Kafka consumer 재시작으로 이미 처리된 이벤트 재유입
# user_id=3, 4는 이미 존재 / user_id=6은 진짜 신규

dedup_data = [
    Row(user_id=3, name="Carol", age=28, signup_date=date(2024, 3, 1),
        plan="premium", email=None),
    Row(user_id=4, name="Dave",  age=32, signup_date=date(2024, 4, 1),
        plan="free",    email="dave@test.com"),
    Row(user_id=6, name="Frank", age=35, signup_date=date(2024, 6, 1),
        plan="premium", email="frank@test.com"),
]
dedup_df = spark.createDataFrame(dedup_data)

dt.alias("target") \
  .merge(
      dedup_df.alias("source"),
      "target.user_id = source.user_id"
  ) \
  .whenNotMatchedInsertAll() \
  .execute()

spark.read.format("delta").load(DELTA_PATH).orderBy("user_id").show()
```

출력:

```
+-------+-----+---+-----------+-------+----------------+
|user_id| name|age|signup_date|   plan|           email|
+-------+-----+---+-----------+-------+----------------+
|      2|  Bob| 25| 2024-02-05|premium|            null|
|      3|Carol| 28| 2024-03-01|premium|            null|
|      4| Dave| 32| 2024-04-01|   free|  dave@test.com|
|      5|  Eve| 22| 2024-05-01|   free|   eve@test.com|
|      6|Frank| 35| 2024-06-01|premium| frank@test.com|
+-------+-----+---+-----------+-------+----------------+
```

#### 코드 흐름 설명

- `whenNotMatchedInsertAll()`만 사용: `whenMatchedUpdate`가 없으니 매칭된 row는 아무 작업도 안 함. 이미 있는 user_id=3, 4는 건드리지 않고 통과
- user_id=6만 매칭이 안 되므로 insert 실행
- 이 패턴이 멱등성 보장의 핵심. 몇 번 재실행해도 결과가 동일함

#### 삭제/조건/선택적/star 구문 정리

```python
# 1. 삭제 수행: 매칭된 row 삭제
dt.alias("target") \
  .merge(source_df.alias("source"), "target.user_id = source.user_id") \
  .whenMatchedDelete() \
  .execute()

# 2. 조건 구문: 특정 조건일 때만 update
dt.alias("target") \
  .merge(source_df.alias("source"), "target.user_id = source.user_id") \
  .whenMatchedUpdate(
      condition = "source.plan != target.plan",   # plan이 바뀐 경우만 update
      set = {"plan": "source.plan"}
  ) \
  .execute()

# 3. 선택적 수행: 특정 컬럼만 업데이트
dt.alias("target") \
  .merge(source_df.alias("source"), "target.user_id = source.user_id") \
  .whenMatchedUpdate(set = {
      "plan":  "source.plan",
      "email": "source.email"
      # age, signup_date는 건드리지 않음
  }) \
  .whenNotMatchedInsertAll() \
  .execute()

# 4. star 구문: 전체 컬럼 자동 매핑
# .whenMatchedUpdateAll()     → set = {"*": "*"} 와 동일, 모든 컬럼 update
# .whenNotMatchedInsertAll()  → values = {"*": "*"} 와 동일, 모든 컬럼 insert
```

---

### e. 작업 내역으로 데이터 변경 검사

#### 왜 이걸 하는가

실무에서 "이 데이터가 언제 어떻게 바뀌었는가"를 추적해야 하는 상황이 자주 생긴다. 데이터 품질 이슈 발생 시 원인 파악, 규정 준수 감사, 파이프라인 장애 분석 등에 필요하다. Delta의 `history()`는 `_delta_log`의 JSON을 파싱해서 모든 작업의 타임라인을 보여준다.

> 엔지니어 독백: 갑자기 분석팀에서 "어제 오후부터 user count가 이상하다"는 제보가 왔다. history() 찍으면 어제 오후에 DELETE가 실행됐고 어떤 조건이었는지까지 바로 보인다. 이게 있으면 장애 원인 파악이 10분 안에 끝난다.

```python
# 실무 상황: 데이터 이상 제보 → 변경 이력 감사

dt.history().select(
    "version",
    "timestamp",
    "operation",
    "operationParameters",
    "operationMetrics"
).show(truncate=False)
```

출력:

```
+-------+-------------------+------------------+------------------------------------------------------+----------------------------------------+
|version|timestamp          |operation         |operationParameters                                   |operationMetrics                        |
+-------+-------------------+------------------+------------------------------------------------------+----------------------------------------+
|7      |2024-06-01 15:30:00|MERGE             |{predicate: target.user_id = source.user_id}          |{numTargetRowsInserted: 1,              |
|       |                   |                  |                                                      | numTargetRowsUpdated: 0,               |
|       |                   |                  |                                                      | numTargetRowsDeleted: 0}               |
|6      |2024-06-01 14:20:00|MERGE             |{predicate: target.user_id = source.user_id}          |{numTargetRowsInserted: 1,              |
|       |                   |                  |                                                      | numTargetRowsUpdated: 1,               |
|       |                   |                  |                                                      | numTargetRowsDeleted: 0}               |
|4      |2024-05-20 09:30:00|DELETE            |{predicate: user_id = 1}                              |{numDeletedRows: 1}                     |
|3      |2024-05-15 08:00:00|UPDATE            |{predicate: user_id = 2, set: {age: 25}}              |{numUpdatedRows: 1}                     |
|0      |2024-01-10 07:00:00|WRITE             |{mode: Overwrite}                                     |{numFiles: 1, numOutputRows: 3}         |
+-------+-------------------+------------------+------------------------------------------------------+----------------------------------------+
```

#### 에러 로그 해석

version이 곧 타임라인이다. version 0이 최초 write고 이후 작업마다 증가한다. version 4를 보면 `2024-05-20 09:30`에 `user_id=1` 조건으로 DELETE가 실행됐고 1건이 삭제됐다. 분석팀이 제보한 "어제 오후 이상"이 바로 이 version 4다. `operationMetrics`의 `numDeletedRows`까지 보면 영향받은 row 수도 바로 확인 가능하다.

---

### f. 시간 탐색을 사용하여 테이블의 이전 스냅샷 쿼리

#### 왜 이걸 하는가

실무에서 잘못된 DELETE나 UPDATE 이후 "삭제 전 데이터를 다시 보고 싶다"거나 "특정 시점의 테이블 상태로 복구해야 한다"는 상황이 생긴다. Delta는 `_delta_log`에 이전 버전 정보가 모두 있어서 version 번호나 timestamp로 과거 시점의 테이블을 그대로 조회하거나 복원할 수 있다.

> 엔지니어 독백: 아까 user_id=1 DELETE 쳤는데 알고 보니 잘못된 요청이었다. Alice 데이터를 복구해야 한다. time travel로 삭제 전 version 조회해서 해당 row만 꺼내 다시 insert하면 복구 완료다. 전체 테이블 재적재 안 해도 된다.

```python
# 실무 상황: 잘못된 DELETE 이후 데이터 복구
# version 4가 DELETE 실행 버전 → version 3 시점으로 조회하면 삭제 전 데이터 확인

# version 기준 조회
df_before_delete = spark.read \
    .format("delta") \
    .option("versionAsOf", 3) \
    .load(DELTA_PATH)

df_before_delete.show()
```

출력:

```
+-------+-----+---+-----------+-------+---------------+
|user_id| name|age|signup_date|   plan|          email|
+-------+-----+---+-----------+-------+---------------+
|      1|Alice| 30| 2024-01-10|premium|alice@test.com|
|      2|  Bob| 25| 2024-02-05|   free|           null|
|      3|Carol| 28| 2024-03-01|premium|           null|
+-------+-----+---+-----------+-------+---------------+
```

```python
# timestamp 기준 조회 (정확한 version 모를 때)
df_ts = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-05-19 23:59:59") \
    .load(DELTA_PATH)

df_ts.filter("user_id = 1").show()
```

출력:

```
+-------+-----+---+-----------+-------+---------------+
|user_id| name|age|signup_date|   plan|          email|
+-------+-----+---+-----------+-------+---------------+
|      1|Alice| 30| 2024-01-10|premium|alice@test.com|
+-------+-----+---+-----------+-------+---------------+
```

```python
# 삭제된 Alice 데이터 복구: 이전 버전에서 row 꺼내서 현재 테이블에 재삽입
alice_df = df_before_delete.filter("user_id = 1")

alice_df.write \
    .format("delta") \
    .mode("append") \
    .save(DELTA_PATH)

spark.read.format("delta").load(DELTA_PATH).orderBy("user_id").show()
```

출력:

```
+-------+-----+---+-----------+-------+---------------+
|user_id| name|age|signup_date|   plan|          email|
+-------+-----+---+-----------+-------+---------------+
|      1|Alice| 30| 2024-01-10|premium|alice@test.com|
|      2|  Bob| 25| 2024-02-05|premium|           null|
|      3|Carol| 28| 2024-03-01|premium|           null|
|      4| Dave| 32| 2024-04-01|   free|  dave@test.com|
|      5|  Eve| 22| 2024-05-01|   free|   eve@test.com|
|      6|Frank| 35| 2024-06-01|premium| frank@test.com|
+-------+-----+---+-----------+-------+---------------+
```

```python
# 테이블 자체를 특정 version으로 되돌리기 (전체 롤백)
spark.sql(f"""
    RESTORE TABLE delta.`{DELTA_PATH}` TO VERSION AS OF 3
""")
```

#### 코드 흐름 설명

- `versionAsOf`: history()에서 확인한 version 번호로 해당 시점의 스냅샷을 read. 현재 테이블은 건드리지 않음
- `timestampAsOf`: 정확한 version을 모를 때 특정 시각 기준으로 조회. Delta가 그 시각에 가장 가까운 version을 찾아줌
- `RESTORE TABLE TO VERSION AS OF 3`: 현재 테이블 상태를 version 3으로 되돌림. version 4 이후 작업이 전부 무효화되고 RESTORE 자체가 새 version으로 기록됨
- time travel이 가능한 이유는 Delta가 오래된 Parquet 파일을 자동 삭제하지 않기 때문. VACUUM을 실행하면 해당 retention 기간 이전 파일은 물리 삭제되고 그 이전 version으로는 time travel 불가

```python
# VACUUM 실행 시 time travel 가능 범위가 줄어듦
spark.sql(f"VACUUM delta.`{DELTA_PATH}` RETAIN 168 HOURS")
# 이후 7일(168시간) 이전 version은 time travel 불가
```
