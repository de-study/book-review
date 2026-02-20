# CH05-외부 데이터 소스와 소통하기
## 스파크 SQL과 아파치 하이브
- 스파크 SQL은 관계형 처리와 함수형 프로그래밍 API 통합하는 아파치 스파크의 기본 구성 요소
	- 샤크 이전에 했던 작업이 기원
	- 샤크는 아파치 스파크 위에 하이브 코드베이스 기반으로 구축 → 하둡 시스템에서 최초의 대화형 SQL 쿼리 엔진 중 하나
- 엔터프라이즈 데이터 웨어하우스만큼 빠르고, Hive / MapReduce 만큼 확장 가능
### 사용자 정의 함수
#### 스파크 SQL UDF
- 사용자만의 고유한 파이스파크 또는 스칼라 UDF 생성 이점
	- 다른 사용자도 스파크 SQL 안에서 이를 사용할 수 있다
#### 스파크 SQL에서 평가 순서 및 null 검사
- 스파크 SQL은 하위 표현식의 평가 순서 보장 X
	- 아래 쿼리는 `s is NOT NULL` 절이 `strlen(s)` 이전에 실행된다는 것 보장하지 않는다
	```python
	spark.sql("SELECT s FROM test1 WHERE s IS NOT NULL AND strlen(s) > 1")
	```
- 적절한 null 검사 수행 방법
	1. UDF 자체가 null 인식하게 만들고 UDF 내부에서 null 검사 수행
	2. IF 또는 CASE WHEN 식 사용해서 null 검사 수행하고 조건 분기에서 UDF 호출
#### 판다스 UDF로 pyspark UDF 향상 및 배포
- `pyspark` UDF 사용 관련 일반적 문제는 스칼라 UDF보다 성능 느리다는 것
	- `pyspark` UDF가 JVM, 파이썬 사이 데이터 이동 필요해서 비용이 많이 들었기 때문
- 이 문제 해결 위해 판다스 UDF가 아파치 스파크 2.3 일부로 도입
	- 판다스 UDF는 `Apache Arrow` 사용해서 데이터 전송 → 판다스는 해당 데이터로 작업
	- `pandas_udf` 키워드를 데코레이터로 사용해서 판다스 UDF 정의하거나 함수 자체를 래핑 가능
- `Apache Arrow` 형식에 포함된 데이터라면 이미 파이썬 프로세스에서 사용 가능 
  → 더 이상 데이터를 직렬화하거나 피클할 필요가 없다
	- 행마다 개별 입력에 대해 작업하는 대신, `Pandas Series` 또는 `DataFrame`에서 작업한다 (벡터화된 실행)

**`Pandas UDF`는 아파치 스파크 3.0에서 `Pandas UDF` 및 판다스 함수 API로 분할** 

- 판다스 UDF
	- 스파크 3.0에서 판다스 UDF는 `pandas.Series`, `pandas.DataFrame`, `Tuple` 및 `Iterator`와 같은 파이썬 유형 힌트로 판다스 UDF 유형 유추
- 판다스 함수 API
	- 판다스 함수 API 사용하면 입력과 출력 모두 판다스 인스턴스인 `DataFrame`에 로컬 파이썬 함수 직접 적용
## 스파크 SQL, 셸, 비라인 및 태블로로 쿼리하기
### 스파크 SQL 셸 사용하기
- 스파크 SQL 쿼리 실행하는 가장 쉬운 방법: `spark-sql CLI`
	- 로컬 모드에서 Hive Metastore와 통신하는 대신 Thrift JDBC/ODBC 서버 (일명 Spark Thrift Server, STS) 와는 통신하지 않는다
```python
# $SPARK_HOME 폴더에서 아래 명령을 실행하면 스파크 SQL CLI 시작
# 셸을 시작한 이후, 스파크 SQL 쿼리를 대화 형식으로 수행할 수 있다
./bin/spark-sql
```
### 비라인 작업
- 비라인은 SQLLINE CLI 기반으로 하는 JDBD 클라이언트이다
	- 이 유틸리티 사용하여 STS에 대해 스파크 SQL 쿼리 실행
- 기본적으로 비라인은 비보안 모드
	- 사용자 이름은 로그인 계정이고, 비밀 번호는 비어있다
### 태블로로 작업하기
- 비라인, Spark SQL 셸과 유사하게 스리프트 JDBC/ODBC 서버 통해 선호하는 BI 도구 스파크 SQL에 연결 가능
## 외부 데이터 소스
### JDBC 및 SQL 데이터베이스
- 스파크 SQL에는 JDBC 사용하여 다른 데이터베이스에서 데이터 읽을 수 있는 데이터 소스 API 포함
	- 결과를 `DataFrame`으로 반환할 때 이러한 데이터 소스 쿼리 단순화하므로 스파크 SQL의 모든 이점 (성능 및 다른 데이터 소스와 조인할 수 있는 기능 포함) 제공
- 데이터 소스 API 사용해서 원격 데이터베이스 테이블을 `DataFrame` 또는 `Spark SQL` 임시 뷰로 로드할 수 있다
#### 파티셔닝의 중요성
- `Spark SQL`과 JDBC 외부 소스간에 많은 양의 데이터 전송할 때 데이터 소스 분할하는 것 중요
	- 모든 데이터가 하나의 드라이버 연결 통해 처리되는 것이 문제
		- 추출 성능을 포화 상태로 만든다
		- 성능 크게 저하
		- 소스 시스템의 리소스를 포화상태로 만들 수 있다
- 대규모 작업의 경우에는 파티셔닝 연결 속성을 사용하는 것이 좋다
```python
- numPartions: 10
- lowerBound: 1000
- upperBound: 10000
# 이렇게 되면 파티션 크기는 1,000이 되고 10개의 파티션 생성
# 이 것은 아래의 쿼리 10개를 실행하는 것과 동일
```
```sql
SELECT * FROM table WHERE partitionColumn BETWEEN 1000 and 2000
SELECT * FROM table WHERE partitionColumn BETWEEN 2000 and 3000
...
SELECT * FROM table WHERE partitionColumn BETWEEN 9000 and 10000
```
- `numPartitions`의 좋은 시작점: Spark Worker 수의 배수 사용
	- e.g. Spark Worker 노드가 4애 있는 경우, 파티션 4개 또는 8개로 시작
- 처음에는 최소 및 최대 `partitionColumn`의 실제 값 기준으로 `lowerBound` 및 `upperBound` 기반으로 계산한다
	- e.g. `numPartitions: 10`, `lowerBound: 1000`, `upperBound: 10000`로 선택했지만 모든 값이 2000 ~ 4000인 경우
		- 10개의 쿼리 중 2개 (각 파티션에 대해 하나씩) 만 모든 작업을 수행
		- 이 시나리오에서 더 나은 구성은 `numPartitions: 10`, `lowerBound: 2000`, `upperBound: 4000`
- 데이터 skew 방지하기 위해 균일하게 분산될 수 있는 `partitionColumn` 선택
	- e.g. `partitionColumn` 대부분의 2500, `numPartitions: 10`, `lowerBound: 1000`, `upperBound: 10000`인 경우, 대부분의 작업은 2000 ~ 3000 사이의 값 요청하는 작업에 의해 수행된다
		- 이러한 경우 기존 `partitionColumn` 대신 다른 `partitionColumn` 사용하거나 가능한 경우 파티션을 더 균등하게 분산하기 위해 새 항목 생성