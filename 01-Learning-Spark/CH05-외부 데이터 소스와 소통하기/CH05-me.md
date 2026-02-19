### 1. Spark SQL의 개요와 배경

- **Spark SQL의 정의**: Apache Spark의 핵심 컴포넌트로, 관계형 처리와 Spark의 함수형 프로그래밍 API를 통합한 엔진
- **기원 및 발전**: Spark 상에서 Hive 코드 기반으로 구축되었던 Shark 프로젝트에서 시작되었으며, 현재는 Hive 코드에 의존하지 않는 독립적인 엔진으로 진화
- **주요 장점**: 선언적 쿼리와 최적화된 스토리지를 통한 성능 향상 및 머신러닝 등 복잡한 분석 라이브러리와의 연동 가능

### 2. 사용자 정의 함수 (UDF) 활용

- **UDF의 역할**: 복잡한 비즈니스 로직이나 ML 모델을 함수로 래핑하여 SQL 쿼리 내에서 재사용 가능하게 함
- **등록 및 사용**: Scala나 Python으로 함수를 작성한 후 `spark.udf.register()`를 통해 등록하며, 세션 내에서만 유효함
- **평가 순서와 Null 처리**: Spark SQL은 하위 표현식의 평가 순서를 보장하지 않으므로, UDF 내부에서 Null 체크를 수행하거나 SQL의 `IF`, `CASE WHEN` 문 사용을 권장

### 3. Pandas UDF (Vectorized UDF)를 통한 최적화

- **기존 문제점**: JVM과 Python 프로세스 사이의 데이터 이동 및 직렬화 비용으로 인해 Scala 대비 성능이 낮음
- **해결책**: **Apache Arrow**를 사용하여 데이터를 전송하고 Pandas를 통해 벡터화된 연산을 수행함으로써 성능을 획기적으로 개선
- **Spark 3.0 변화**: Python 타입 힌트(Type Hints)를 사용하여 UDF 타입을 추론하며, Pandas UDF와 Pandas Function API로 세분화됨

### 4. 데이터 쿼리 인터페이스 및 도구

- **Spark SQL Shell (spark-sql)**: 로컬 모드에서 Hive 메타스토어와 통신하며 대화형으로 SQL 쿼리를 실행하는 CLI 도구
- **Beeline**: HiveServer2와 호환되는 JDBC 클라이언트로, Spark Thrift 서버(STS)에 접속하여 쿼리 실행 가능
- **외부 BI 도구 연동**: Tableau, Power BI 등 비즈니스 인텔리전스 도구를 JDBC/ODBC 프로토콜을 통해 연결 가능


### 5. 외부 데이터베이스 연결 (JDBC)

- **JDBC 연결 개요**: 데이터 소스 API를 통해 외부 DB의 데이터를 DataFrame으로 로드하며, 내부 데이터와의 조인 및 최적화 기능 활용 가능
- **주요 연결 속성**:
    - `url`: JDBC 접속 URL (예: `jdbc:postgresql://localhost/test`)
    - `dbtable`: 읽거나 쓸 대상 테이블 이름
    - `query`: 데이터를 읽어올 때 사용할 SQL 쿼리 (dbtable과 동시 사용 불가)
    - `user / password`: 데이터베이스 접속 계정 정보
    - `driver`: 사용할 JDBC 드라이버의 클래스 이름
- **병렬 처리를 위한 파티셔닝 전략**:
    - `numPartitions`: 읽기/쓰기에 사용할 최대 파티션 수이자 동시 JDBC 연결 수로, 보통 워커 수의 배수로 설정함
    - `partitionColumn`: 파티션을 나눌 기준 컬럼 (숫자, 날짜, 타임스탬프 타입만 가능)
    - `lowerBound / upperBound`: 파티션 스트라이드(보폭) 결정을 위한 기준 컬럼의 최소/최대값

### 6. 데이터베이스별 연결 사례 및 소스

- **PostgreSQL 및 MySQL**: 가장 보편적인 방식이며 드라이버 JAR 파일을 클래스패스에 추가하여 `format("jdbc")`로 연동
- **Azure Cosmos DB**: `azure-cosmosdb-spark` 커넥터를 사용하며 `query_custom` 설정을 통해 내부 인덱스 활용 가능
- **MS SQL Server**: `mssql-jdbc` 드라이버를 사용하며 `jdbcUrl`에 데이터베이스 이름을 포함하여 접속
- **기타 외부 소스**: Apache Cassandra, Snowflake, MongoDB 등 다양한 에코시스템 연동 지원


### 7. 복잡한 데이터 구조 조작 및 내장 함수

- **전통적인 방식의 한계**:
    - `Explode` 및 `Collect`: 중첩 구조를 펼친 뒤 연산하고 다시 합치는 방식으로, 셔플 발생 및 순서 보장 불가 등의 단점 존재
    - 사용자 정의 함수(UDF): 로직 작성이 자유로우나 직렬화 비용 발생으로 성능 최적화가 어려움
- **Array(배열) 전용 내장 함수**: `array_distinct`(중복 제거), `flatten`(평탄화), `array_sort`(정렬), `array_zip`(구조체 병합) 등
- **Map(맵) 전용 내장 함수**: `map_from_arrays`(맵 생성), `map_concat`(맵 통합), `element_at`(키 값 추출) 등

### 8. 고차 함수 (Higher-Order Functions) 적용

- **작동 원리**: 익명 함수(Lambda)를 인자로 받아 배열 내 각 요소에 직접 연산을 수행하며 SQL 내에서 효율적으로 동작
- **핵심 함수**:
    - `transform()`: 배열의 모든 요소에 함수를 적용하여 새로운 배열 생성
    - `filter()`: 조건을 만족하는 요소만 남긴 배열 생성
    - `exists()`: 조건을 만족하는 요소의 존재 여부 확인 (Boolean)
    - `reduce()`: 배열의 요소들을 하나의 값으로 집계


### 9. 실전 관계형 연산 및 데이터 변환 실습

- **데이터 로드 및 초기 설정**

```python
9. 실전 관계형 연산 및 데이터 변환 실습 (코드 정제)
데이터 로드 및 초기 설정

Python
# 공항 정보 및 비행 지연 데이터 로드
airportsna = spark.read.csv("airports-na.txt", header=True, sep="\t")
departureDelays = spark.read.csv("departuredelays.csv", header=True)

from pyspark.sql.functions import expr

# 데이터 타입 변환 (String -> Int) 및 컬럼 업데이트
departureDelays = departureDelays \
    .withColumn("delay", expr("CAST(delay AS INT)")) \
    .withColumn("distance", expr("CAST(distance AS INT)"))

# SQL 쿼리를 위한 임시 뷰 생성
departureDelays.createOrReplaceTempView("departureDelays")
특정 경로 데이터 추출 (SEA -> SFO)

Python
# 특정 조건(출발지, 목적지, 날짜)에 맞는 데이터 필터링 및 테이블 생성
foo = spark.sql("""
    SELECT date, delay, distance, origin, destination 
    FROM departureDelays 
    WHERE origin = 'SEA' 
      AND destination = 'SFO' 
      AND date LIKE '01010%'
""")

# 분석용 임시 뷰 등록
foo.createOrReplaceTempView("foo")
```

- **특정 경로 데이터 추출 (SEA -> SFO)**

```python
# 특정 조건(출발지, 목적지, 날짜)에 맞는 데이터 필터링 및 테이블 생성
foo = spark.sql("""
    SELECT date, delay, distance, origin, destination 
    FROM departureDelays 
    WHERE origin = 'SEA' 
      AND destination = 'SFO' 
      AND date LIKE '01010%'
""")

# 분석용 임시 뷰 등록
foo.createOrReplaceTempView("foo")
```

- **결합 연산 (Unions & Joins)**:
    - `Union`: 스키마가 동일한 두 DataFrame을 수직으로 합침 (`bar = foo.union(foo)`)
    - `Join`: 특정 키를 기준으로 테이블 결합 (기본값은 Inner Join)
- **윈도우 함수 (Windowing)**:
    - 행 간 관계 정의를 위해 `partitionBy`, `orderBy` 등을 사용함
    - 예시: `Window.partitionBy("origin").orderBy(desc("delay"))`를 정의하여 각 출발지별 지연 시간 상위 비행편 추출
- **데이터 수정 및 변환 (Modifications)**:
    - `withColumn()`: 신규 컬럼 생성 및 데이터 가공
    - `drop()`: 불필요한 컬럼 제거
    - `withColumnRenamed()`: 컬럼 명칭 정제
