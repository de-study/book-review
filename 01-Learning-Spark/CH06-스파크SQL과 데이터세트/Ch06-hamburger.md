## Spark SQL이란?

- 정형 데이터 처리를 위한 Spark 모듈
- SQL 문법 + DataFrame/Dataset API 제공
- 내부적으로 Catalyst Optimizer와 Tungsten 엔진을 사용

왜 Spark SQL을 쓰는가?
- RDD보다 표현력이 높고
- 자동 최적화(Catalyst)
- JVM 기반 물리 실행 최적화(Tungsten)
- 다양한 데이터 소스와 통합 가능 (Parquet, JSON, JDBC 등)

## 스파크 SQL의 구조
```python
사용자 코드 (SQL / DataFrame / Dataset)
            ↓
Logical Plan
            ↓
Catalyst Optimizer
            ↓
Physical Plan
            ↓
Tungsten 실행 엔진
```
## DataFrame 구조화된 분산 테이블
- 분산된 테이블 구조
- 스키마(컬럼명 + 타입) 존재
- 내부적으로는 Dataset[Row]
```python
DataFrame = Dataset[Row]
```
RDD는 단순한 객체 컬렉션이기 때문에 컬럼 개념이 없음, 스키마가 없음, 최적화하기 어려움
반면 DataFrame은 컬럼 기반 연산, SQL 최적화 가능, 다양한 데이터 소스 통합 가능 (Parquet, JSON, JDBC 등)

## Dataset 타입 안정성을 추가한 API
- 타입 안정성을 가지는 분산 컬렉션
- Scala / Java 전용
- Python에는 없음
```scala
case class Person(name: String, age: Int)
val ds = spark.read.json("people.json").as[Person]
```
Encoder?
Dataset은 JVM 객체 ↔ Spark 내부 바이너리 표현을 변환해야 한다.
이를 담당하는 것이 Encoder이다.

Encoder 덕분에 타입 안정성 유지, 메모리 효율적 저장, 빠른 직렬화가 가능하다.

## Catalyst Optimizer
Catalyst는 규칙 기반(rule-based) + 비용 기반(cost-based) 최적화 엔진이다.
Spark가 내부적으로 불필요한 컬럼 제거, filter 먼저 실행, join 순서 재배치 같은 걸 자동으로 해준다.
-> 최적화는 Spark가 알아서 한다.

### 4단계 최적화 과정
1️⃣ Unresolved Logical Plan
- SQL 파싱 직후 상태
- 컬럼이 실제로 존재하는지 아직 모름

2️⃣ Analyzed Logical Plan
- 스키마 확인
- 타입 확인
- 컬럼 해석 완료

3️⃣ Optimized Logical Plan
여기서 최적화 발생:
Predicate Pushdown
Projection Pruning
Constant Folding
Join Reordering

4️⃣ Physical Plan
실제 실행 전략 선택:
Broadcast Hash Join
Sort Merge Join
Shuffle Hash Join 등

예시
```sql
SELECT name FROM people WHERE age > 20
```
Spark는 내부적으로 필요한 컬럼만 읽고 filter를 최대한 빨리 적용하고 최소 데이터만 shuffle한다. 

## Tungsten? 물리 실행 최적화
Catalyst가 논리적 최적화라면 Tungsten은 “실행 최적화”다.
- JVM 객체 줄임
- 메모리 효율 개선
- CPU 친화적 실행
주요 기능:

1. Off-heap 메모리 사용
- JVM GC 부담 감소

2. Cache-friendly execution
- CPU 캐시 친화적 구조

3. Whole-stage Code Generation
- 쿼리 전체를 하나의 Java 코드로 생성

-> 함수 호출 오버헤드 감소
-> CPU pipeline 효율 증가
