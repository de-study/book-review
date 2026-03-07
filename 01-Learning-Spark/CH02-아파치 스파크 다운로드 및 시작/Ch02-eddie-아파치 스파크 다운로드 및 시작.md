# 01.스파크 애플리케이션 개념

### Spark session
> 여기서 중요한 개념이 스파크 session이다 session은 스파크 job과 그것을 실행할 클러스터의 연결지점이자 실행시 필요한 메타데이터를 지정해주는 역할을 함.
- 스파크 잡 실행하는 객체  
```
SparkSession 없으면:
- 어느 서버에서 실행할지 모름
- 메모리 얼마 쓸지 모름
- Executor 몇 개 띄울지 모름
- 코드를 누가 받아서 실행하나?

SparkSession이 이 모든 걸 세팅해주는 거였어."
```
<br><br>  


### 작업 흐름
**<흐름 정리>  
1.SparkSession 생성: 애플리케이션 시작  
2.Transformation 정의: Lazy 평가 (실행 안됨)  
3.Action 호출: Job 생성 트리거  
4.DAG 생성: Spark이 실행 계획 수립  
5.Stage 분리: Shuffle 경계로 나눔  
6.Task 생성: 각 Stage를 파티션별로 분해  
7.Task 실행: Executor에서 병렬 처리  
8.결과 수집: Driver로 리턴**  

```
# SparkSession 생성
spark = SparkSession.builder.appName("example").getOrCreate()

# Transformation (아직 실행 안됨)
df = spark.read.parquet("data.parquet")  
filtered = df.filter(col("age") > 20)  # Transformation
grouped = filtered.groupBy("city")      # Transformation (Shuffle 경계)

# Action 호출 → Job 시작
result = grouped.count().collect()      # Action
```
<br> 


실행 분석:
- Job 1개: collect() 호출 시 생성
- Stage 2개:
```
Stage 1: read + filter (Shuffle 없음)
Stage 2: groupBy + count (Shuffle 발생)
```

- Task 개수:
```
Stage 1: 파일 파티션 개수만큼 (예: 10개)
Stage 2: groupBy 후 파티션 개수만큼 (예: 200개)
```




### Job (작업 단위)
생성 시점:
- Action 호출 시 1개의 Job 생성
- Action 예: collect(), count(), save()

역할:
하나의 결과를 만들기 위한 전체 작업

왜 필요한가:
- Hadoop MapReduce 한계: 단순 Map-Reduce만 가능
- Spark 해결: 복잡한 연산을 하나의 Job으로 최적화

예시:
```
# 이 코드는 1개의 Job 생성
df.filter(...).groupBy(...).count()  # count()가 Action
```

### Stage (단계)
생성 기준:
- Job을 Shuffle 경계로 나눈 단위
- Shuffle 발생 시점마다 Stage 분리

Shuffle이란:
- 데이터를 네트워크를 통해 재분배하는 작업
- 파티션 간 데이터 교환 필요

왜 Stage로 나누나:
- Shuffle은 네트워크 I/O 발생 → 병목
- Shuffle 전까지는 파이프라인으로 연속 실행 가능
- Shuffle 후에는 모든 데이터가 준비되어야 다음 진행

Shuffle 발생 연산:
`groupBy()`, `join()`, `reduceByKey()`, `repartition()`

예시 흐름:
```
# Stage 1: filter, map (Shuffle 없음)
# Stage 2: groupBy에서 Shuffle 발생
# Stage 3: agg 계산
df.filter(...).map(...).groupBy(...).agg(...)
```


### Task (최소 실행 단위)
생성 기준:
- Stage 내에서 파티션 개수만큼 Task 생성
- 1 Task = 1 파티션 처리

역할:
- 실제로 Executor에서 실행되는 작업
- 각 Task는 독립적으로 병렬 실행

왜 파티션별로 나누나:
- 분산 처리의 핵심: 데이터를 쪼개서 병렬 처리
- 파티션 = 물리적 데이터 분할 단위
- Task = 파티션을 처리하는 논리적 작업 단위

예시:
- 데이터가 100개 파티션 → Stage당 100개 Task 생성
---

<br><br><br> 
# 02. 스파크 연산 종류
- Transformation(트랜스포메이션)
- Action(액션)

### 트랜스포메이션
- 원본 데이터의 값을 변형하지 않고 처리하는 연산들
- 결과적으로 DF -> (트랜스포메이션) -> DF'
- select(), filter() 등
- 특징
  - Lazy evaluation : 최적화 작업을 계획하기 위해 action에서 실행됨(shell환경일지라도)
  - lineage 형성 : 여러 트랜스포메이션 작업들의 순서를 DAG방식으로 기록해서(연산만 추가)
    - 장애 상황에도 대응가능
```
# Narrow Transformation (Shuffle 없음, 빠름)
df.select("name", "age")        # 컬럼 선택
df.filter(col("age") > 20)      # 필터링
df.map(lambda x: x * 2)         # 각 요소 변환
df.withColumn("new", col("old") * 2)  # 컬럼 추가

# Wide Transformation (Shuffle 발생, 느림)
df.groupBy("city")              # 그룹핑
df.join(other_df, "key")        # 조인
df.distinct()                   # 중복 제거
df.repartition(10)              # 파티션 재분배
df.orderBy("age")               # 정렬
```
- 종류

| 분류 | 연산 | 설명 | Shuffle 여부 | 성능 |
| :--- | :--- | :--- | :--- | :--- |
| **Narrow** | `select()` | 특정 컬럼 선택 | X | 빠름 |
| **Narrow** | `filter()` | 조건에 맞는 데이터 필터링 | X | 빠름 |
| **Narrow** | `map()` | 각 요소를 1:1로 변환 | X | 빠름 |
| **Narrow** | `flatMap()` | 각 요소를 분리하여 여러 개로 확장 | X | 빠름 |
| **Narrow** | `withColumn()` | 컬럼 추가 또는 기존 컬럼 수정 | X | 빠름 |
| **Narrow** | `drop()` | 특정 컬럼 삭제 | X | 빠름 |
| **Narrow** | `union()` | 두 DataFrame을 하나로 합침 | X | 빠름 |
| **Wide** | `groupBy()` | 지정된 키를 기준으로 그룹핑 | O | 느림 |
| **Wide** | `join()` | 두 데이터셋을 특정 키로 조인 | O | 느림 |
| **Wide** | `distinct()` | 중복 데이터 제거 | O | 느림 |
| **Wide** | `repartition()` | 파티션 개수 재분배 | O | 느림 |
| **Wide** | `orderBy()` | 데이터를 특정 기준으로 정렬 | O | 느림 |
| **Wide** | `reduceByKey()` | 키별로 데이터를 모아 집계 연산 | O | 느림 |

- Transformation 분류
  - Narrow Transformation
    - Shuffle 없음
    - 파티션 간 데이터 이동 불필요
    - 각 파티션 독립적 처리
    - 빠름
    예시: map, filter, select, union
  
  - Wide Transformation
    - Shuffle 발생
    - 파티션 간 데이터 교환 필요
    - 네트워크 I/O 발생
    - 느림
    예시: groupBy, join, distinct, repartition
   
<br> 

### 액션  
- Action은 Transformation으로 구성된 계보(Lineage)를 실제로 실행하며, 결과를 드라이버로 반환하거나 외부 저장소에 기록.
- 특징
  - Eager: 즉시 실행
  - Job 생성: Spark 작업 시작
  - 결과값 리턴: Driver로 데이터 반환 또는 저장
- 예시
```
# Driver로 데이터 수집
df.collect()                    # 전체 데이터 리스트로
df.take(10)                     # 상위 10개만
df.first()                      # 첫 번째 Row
df.head(5)                      # 상위 5개

# 집계 연산
df.count()                      # 개수 세기
df.reduce(lambda a,b: a+b)      # 누적 연산

# 출력
df.show(20)                     # 콘솔 출력
df.foreach(print)               # 각 Row 처리

# 저장
df.write.parquet("output/")     # 파일 저장
df.write.saveAsTable("table")   # 테이블 저장
```

- 종류
 
| 분류 | 연산 | 리턴 타입 | 용도 |
| :--- | :--- | :--- | :--- |
| **수집** | `collect()` | `List[Row]` | 전체 데이터를 드라이버 메모리로 수집 (대용량 시 위험) |
| **수집** | `take(n)` | `List[Row]` | 상위 n개의 데이터만 수집 |
| **수집** | `first()` | `Row` | 첫 번째 로우(Row) 반환 |
| **수집** | `head(n)` | `List[Row]` | 상위 n개의 로우 수집 |
| **집계** | `count()` | `Long` | 데이터셋의 전체 레코드 개수 계산 |
| **집계** | `reduce()` | 단일값 | 데이터를 하나로 합치는 누적 연산 수행 |
| **집계** | `aggregate()` | 단일값 | 초기값을 이용한 복잡한 다단계 집계 수행 |
| **출력** | `show(n)` | `None` | 데이터를 표 형태로 콘솔에 출력 (디버깅용) |
| **출력** | `foreach()` | `None` | 각 로우에 대해 사용자 정의 함수 실행 |
| **저장** | `write.parquet()` | `None` | 데이터를 Parquet 형식 파일로 저장 |
| **저장** | `write.csv()` | `None` | 데이터를 CSV 형식 파일로 저장 |
| **저장** | `saveAsTable()` | `None` | 데이터를 Hive 메타스토어 등에 테이블로 저장 |

<br><br><br>  

### 기타 
> 하둡에도 트랜스포메이션이 되나? 안됨. 하둡에서 보완하기 위해 나온게 트랜스포메이션/액션임 
```
Hadoop MapReduce의 한계
- 매 연산마다 디스크 I/O 발생
- 중간 결과를 HDFS에 저장 → 읽기 반복
- 최적화 불가능 (각 단계가 독립적)

Spark의 해결책: Lazy Evaluation

Transformation은 실행 계획만 기록
- Action 호출 시점에 전체 최적화
- 불필요한 연산 제거 가능
- 메모리 기반 파이프라인 실행

왜 이렇게 하나: 전체 흐름을 보고 최적화하려면 실행을 미뤄야 함
```
<br><br>


> RDD, DF 차이
### Spark 최적화 비교: RDD vs DataFrame

### 1. 핵심 요약
- **RDD**: "어떻게(How)" 처리할지 엔지니어가 절차를 직접 정의 (수동 최적화)
- **DataFrame**: "무엇을(What)" 할지 선언하면 Spark가 실행 계획 수립 (자동 최적화)

---

### 2. RDD (Resilient Distributed Dataset)
### [특징: 명령형 프로그래밍 및 수동 최적화]
- **데이터 구조 미인식**: Spark가 데이터 내부 구조를 알 수 없어 불투명한(Opaque) 연산 수행
- **최적화 책임**: 엔지니어가 데이터 로드, 필터링 순서, 메모리 관리 등을 직접 설계

### [작동 방식 및 문제점]
- **전체 파싱**: JSON 내 모든 필드를 객체로 변환하여 CPU 사용량 증가
- **메모리 과부하**: 사용하지 않는 필드(address, email 등)까지 모두 메모리에 로드
- **최적화 부재**: 필터링 조건이 코드 마지막에 있어도 전체 데이터를 먼저 읽는 비효율 발생

---

### 3. DataFrame (Dataset<Row>)
### [특징: 선언형 프로그래밍 및 자동 최적화]
- **구조적 데이터**: 스키마 정보를 기반으로 **Catalyst Optimizer**가 최적의 실행 계획 수립
- **Tungsten 엔진**: 효율적인 바이너리 포맷을 사용하여 메모리 및 CPU 최적화

### [주요 자동 최적화 기법]
- **Column Pruning (컬럼 제거)**: 필요한 필드(`age`, `name`)만 선택적으로 파싱 및 로드
- **Predicate Pushdown (조건절 푸시다운)**: 데이터를 읽는 단계에서 필터링(`age >= 18`)을 수행하여 처리량 감소
- **Execution Plan**: 논리적 계획을 물리적 계획으로 변환 시 비용 기반 최적화 수행

---

### 4. 비교 요약
| 비교 항목 | RDD (Low-level API) | DataFrame (High-level API) |
| :--- | :--- | :--- |
| **최적화 주체** | 엔지니어 (수동) | Spark Catalyst Optimizer (자동) |
| **데이터 구조 인식** | 불가능 (Java/Python 객체) | 가능 (Schema 기반) |
| **성능 일관성** | 개발자의 숙련도에 따라 편차 큼 | 어떤 코드를 작성해도 엔진이 최적화 |
| **가독성** | 로직이 복잡해질수록 낮아짐 | SQL과 유사하여 높음 |
