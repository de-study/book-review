# 스파크 애플리케이션 개념

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
