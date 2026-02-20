## 1. 사용자 정의 함수 (UDF) 최적화

### UDF의 역할 및 주의사항
- **역할**: 내장 함수로 구현 불가능한 로직을 Python/Scala 함수로 구현하여 SQL 내에서 재사용
- **평가 순서와 Null 처리**: 
    - Spark SQL은 하위 표현식의 평가 순서를 보장하지 않음
    - `WHERE s IS NOT NULL AND strlen(s) > 1` 실행 시 `strlen`이 먼저 실행되어 에러 발생 가능
    - **해결**: UDF 내부에서 직접 Null 체크 수행 또는 SQL의 `CASE WHEN` 문 사용 권장

### Pandas UDF (Vectorized UDF)
- **기존 문제**: JVM-Python 프로세스 간 Pickle 직렬화 비용으로 인해 성능 저하 발생
- **혁신**: **Apache Arrow** 포맷을 통한 데이터 전송으로 직렬화 비용 제거
- **동작 방식**: 행(Row) 단위 처리가 아닌 **Pandas Series/DataFrame 단위(벡터화된 실행)**로 작업 수행
- **Spark 3.0+ 변화**: Python 타입 힌트 기반 타입 추론 및 `Pandas UDF`, `Pandas 함수 API`로 기능 분화



```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Spark 3.0+ 타입 힌트 방식의 Pandas UDF
@pandas_udf("double")
def predict_score_udf(mid: pd.Series, final: pd.Series) -> pd.Series:
    # 벡터화된 연산으로 성능 극대화
    return mid * 0.4 + final * 0.6

df.withColumn("total", predict_score_udf(df["mid_score"], df["final_score"]))
```

## 2. 데이터 쿼리 인터페이스 및 도구

### Spark SQL Shell (spark-sql)
- **특징**: 로컬 모드에서 Hive Metastore와 직접 통신하며 쿼리를 실행하는 CLI
- **사용처**: 간단한 스키마 확인 및 Ad-hoc 쿼리 실행 시 가장 빠름

### 비라인 (Beeline)
- **특징**: SQLLINE 기반 JDBC 클라이언트로 **Spark Thrift 서버(STS)**에 접속하여 쿼리 실행
- **환경**: 엔터프라이즈 환경에서 보안 및 동시 접속 관리가 필요할 때 사용

### 외부 BI 도구 연동
- JDBC/ODBC 프로토콜을 통해 Tableau, Power BI 등 외부 시각화 도구와 Spark SQL 연결 가능



## 3. 외부 데이터베이스 연결 (JDBC) 최적화

### 병렬 처리를 위한 파티셔닝 전략
데이터 소스를 분할하지 않으면 단일 드라이버 연결을 통해 모든 데이터가 처리되어 성능이 급격히 저하됩니다.

- **numPartitions**: 동시 JDBC 연결 수 (Spark Worker 수의 배수 설정 권장)
- **partitionColumn**: 파티션을 나눌 기준 컬럼 (인덱싱된 숫자, 날짜 타입만 가능)
- **lowerBound / upperBound**: 파티션 구간을 결정하기 위한 기준 컬럼의 최솟값/최댓값



### DB별 특화 옵션
- **MySQL**: `rewriteBatchedStatements=true` (실제 배치 Insert 활성화)
- **Oracle**: `fetchsize`를 10,000 이상으로 상향 조정 (기본값 10은 매우 느림)
- **PostgreSQL**: `reWriteBatchedInserts=true` 및 `stringtype=unspecified` 활용

```python
# 파티셔닝 기반 병렬 데이터 로드 예시
jdbc_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://db_host:3306/prod_db") \
    .option("dbtable", "orders") \
    .option("partitionColumn", "order_id") \
    .option("lowerBound", "1") \
    .option("upperBound", "1000000") \
    .option("numPartitions", "10") \
    .load()
```

## 4. 고차 함수 및 실전 데이터 변환

### 고차 함수 (Higher-Order Functions)
배열(Array) 내 각 요소에 직접 연산을 수행하여 `explode`를 방지하고 성능을 최적화합니다.
- **transform()**: 모든 요소 가공 (새 배열 생성)
- **filter()**: 조건 만족 요소만 추출
- **reduce()**: 요소를 하나의 값으로 집계

### 실전 관계형 연산 (PySpark)
- **데이터 정제**: `withColumn()`과 `expr()`을 사용하여 String → Int 타입 변환
- **윈도우 함수**: `partitionBy`, `orderBy`를 정의하여 그룹 내 순위(`rank`) 계산
- **결합**: `union()`을 통한 데이터 수직 결합

```python
from pyspark.sql.functions import col, expr
from pyspark.sql.window import Window

# 1. 타입 변환 및 분석용 뷰 등록
df = departureDelays \
    .withColumn("delay", expr("CAST(delay AS INT)")) \
    .withColumn("distance", col("distance").cast("int"))

# 2. 윈도우 함수: 각 출발지별 지연 시간 상위 3개 비행편 추출
windowSpec = Window.partitionBy("origin").orderBy(col("delay").desc())
df.withColumn("rank", expr("rank()").over(windowSpec)) \
  .filter("rank <= 3").show()

```
