> 스파크 메모리 최적화 방향을 알아보자  
> 전체 주제 : 설정값과 메모리 관련 처리방법
>1) spark 설정값 변경방법
>2) 메모리 구조
>3) 파티션 - 셔플파티션
>4) 캐싱
>5) 조인전략
>6) UI로 확인


# 01.Spark 설정값 설정

## (1)설정값 변경
> 3가지 설정값 변경방법이 있으며  
> 적용되는 순서가 있다.  
> 나중에 적용된 것으로 실행된다.
> 최종적으로 코드상의 session 설정값이 적용됨 → 세션마다 세밀하게 조정가능

### 설정값 변경방법
1) spark conf파일 내용 수정하기($SPARK_HOME)
	1) $SPARK_HOME/conf/spark-default.conf.template → template 확장자 제거 후 사용
2) spark-submit 명령어 실행시 설정하기(터미널)
3) sparkSession 객체에 설정값 입력하기(etl코드)
1>2>3 순으로 선언/적용된다.
맨마지막 선언된 값으로 최종 실행된다.(sparkSession 설정값 우선) 

- 세션이 실행 중에 변경가능, 변경 불가능한 것이 존재함
```
#`isModifiable()` = "현재 살아있는 세션에서 변경 가능한가?"
spark.conf.isModifiable("spark.sql.shuffle.partitions")
- `True` → 변경 가능
- `False` → 변경 불가 (JVM 시작 시 이미 확정된 값), 세션 죽이고 재시작해야 적용 가능
  
#파티션변경
spark.conf.set("spark.sql.shuffle.partitions", "500")  
```
### 상황별 사용법
1. conf파일 → 팀/클러스터 공통 기본값 설정할 때. 인프라 엔지니어가 한번 세팅해두는 것
2. spark-submit → 잡(Job) 단위로 다른 설정이 필요할 때. 배포 스크립트(Airflow DAG 등)에 박아둠
3. Session 코드 → 쿼리 단위로 동적으로 바꿔야 할 때. 개발/테스트 중에 자주 씀
4. 스파트 UI 대시보드의 'Environment'탭에서 확인 가능

**1. conf파일 - 클러스터 공통 설정**
- DBA나 인프라팀이 `spark.executor.memory=8g` 세팅
- 모든 팀의 모든 잡에 기본 적용
- 개발자는 건드리지 않음

**2. spark-submit - 잡 단위 설정**
- 매일 새벽 2시 Airflow로 돌리는 ETL 잡
- 이 잡만 executor를 20개 써야 함
- `--num-executors 20` 옵션을 Airflow DAG 스크립트에 박아둠

**3. Session 코드 - 쿼리 단위 설정**
- 평소엔 shuffle partition 200개
- 특정 대용량 집계 쿼리 직전에만 500으로 올렸다가
- 쿼리 끝나면 다시 200으로 복구
- 같은 잡 안에서 동적으로 제어

### 노트북 확인 코드
```
#현재 SparkSession에 설정된 모든 configuration key-value 쌍을 반환
spark.conf.getAll()

<result>
[('spark.app.id', 'local-1234567890'), 
('spark.app.name', 'MySparkApp'), 
('spark.master', 'local[*]'), 
('spark.executor.memory', '2g'), 
('spark.driver.memory', '1g'), 
('spark.sql.shuffle.partitions', '200'), 
('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'), ('spark.sql.warehouse.dir', '/user/hive/warehouse'), ('spark.executor.cores', '2'), ('spark.driver.maxResultSize', '1g')]
```





## (2) Executor 동적할당 설정값  
### Spark Executor 동적 자원 할당 개념
> <용어정리>  
> job : action 호출 1번당 1개 생성, `collect()`, `count()`, `write()` 호출할 때마다 새 Job 생성  
> SparkSession = job1, job2, ...  


```
SparkSession
└── Application
    └── Job (action 1번 = Job 1개)
        └── Stage
            └── Task

spark = SparkSession.builder.getOrCreate()   # SparkSession 시작, Executor 할당

df = spark.read.parquet("s3://orders/")      # Job 없음, 그냥 읽기 계획만 수립
df_filtered = df.filter(df.amount > 1000)    # Job 없음, transformation
df_agg = df_filtered.groupBy("user_id")      # Job 없음, transformation

result = df_agg.count()                      # Job 1 실행 (action)
df_agg.write.parquet("s3://output/")         # Job 2 실행 (action)

spark.stop()                                 # SparkSession 종료, 자원 반납
```

#### 뭘 동적으로 할당?  
- 할당 대상: Executor 개수
- 공백 구간에서 Executor를 반납해서 클러스터 자원 낭비를 줄이는 것
- 공백 구간 
	- action과 action 사이 Spark를 사용하지 않는 구간
	- Python 연산, API 호출, time.sleep, 분석가가 결과 보는 시간 등
	- 이 구간에서 Executor는 Driver한테 일 기다리며 idle 상태

#### 작동과정
- 정적할당 일때, 
```
spark = SparkSession.builder.getOrCreate()
# Executor 10개 고정 할당 (세션 끝까지 유지)

df = spark.read.parquet("s3://orders/")
# Transformation, Job 없음, Executor 대기 중

result = df.count()
# Job 1 실행, Executor 10개 사용
# Job 1 완료, Executor 10개 idle 대기

time.sleep(3600)
# 순수 Python, Spark 미사용
# Executor 10개 idle 상태로 자원 점유 중

x = some_python_function()
# 순수 Python, Spark 미사용
# Executor 10개 여전히 idle 상태로 자원 점유 중

api_result = requests.get("https://...")
# 순수 Python, Spark 미사용
# Executor 10개 여전히 idle 상태로 자원 점유 중

df2.write.parquet("s3://output/")
# Job 2 실행, 같은 Executor 10개 사용
# Job 2 완료, Executor 10개 idle 대기

spark.stop()
# SparkSession 종료, Executor 10개 반납
```
- 동적할당 일때, 
```
spark = SparkSession.builder.getOrCreate()
# Executor 2개만 시작 (initialExecutors)

df = spark.read.parquet("s3://orders/")
# Transformation, Job 없음, Executor 2개 대기 중

result = df.count()
# Job 1 시작, Task 큐 증가 감지
# Executor 2개 → 8개로 증가
# Job 1 완료, Executor 8개 idle 상태

time.sleep(3600)
# 순수 Python, Spark 미사용
# idle 60초 초과 → Executor 8개 → 2개로 반납
# 이 1시간 동안 Executor 2개만 점유

x = some_python_function()
# 순수 Python, Spark 미사용
# Executor 2개만 점유 중

api_result = requests.get("https://...")
# 순수 Python, Spark 미사용
# Executor 2개만 점유 중

df2.write.parquet("s3://output/")
# Job 2 시작, Task 큐 증가 감지
# Executor 2개 → 6개로 증가
# Job 2 실행
# Job 2 완료, Executor 6개 idle 상태
# idle 60초 초과 → Executor 6개 → 2개로 반납

spark.stop()
# SparkSession 종료, Executor 2개 반납
```

#### 뭐가 좋지? 
엔지니어 독백:
> "공백 1시간 동안 정적은 Executor 10개 내내 들고 있고, 동적은 2개만 들고 있어. 이 차이가 멀티테넌트 클러스터에서 다른 팀한테 자원을 돌려줄 수 있냐 없냐를 결정하는 거야."  

**동적 할당이 이득인 상황**
- 여러 팀이 클러스터 공유 → 반납된 자원을 다른 팀이 사용 가능
- Job 사이 공백이 긴 경우 → 노트북 분석, 외부 API 호출 등
- Job마다 데이터 크기가 다를 때 → 자동으로 Executor 수 조절

**동적 할당이 불필요한 상황**
- 혼자 쓰는 클러스터
- Job 사이 공백이 거의 없는 배치 파이프라인
- 스트리밍 처리

### 동적할당관련 Spark설정값
> 동적 할당 설정값은 언제 반납하고, 언제 요청하고, 얼마나 요청할지를 제어한다.  

**<기본값>**
```
spark.dynamicAllocation.enabled = false
# 기본 비활성화

spark.dynamicAllocation.minExecutors = 0
# 완전 반납 가능

spark.dynamicAllocation.maxExecutors = infinity
# 무제한 (사실상 클러스터 전체 먹을 수 있음)

spark.dynamicAllocation.initialExecutors = minExecutors와 동일
# 즉 기본 0

spark.dynamicAllocation.executorIdleTimeout = 60s
# 60초 idle이면 반납

spark.dynamicAllocation.schedulerBacklogTimeout = 1s
# 1초 밀리면 Executor 요청

spark.dynamicAllocation.sustainedSchedulerBacklogTimeout = 1s
# 요청 후에도 밀리면 1초 간격으로 추가 요청

spark.shuffle.service.enabled = false
# 기본 비활성화
```

> "기본값 그대로 켜면 두 가지가 위험해. maxExecutors가 infinity라서 클러스터 자원 혼자 다 먹을 수 있고, minExecutors가 0이라서 공백 후 콜드스타트 발생해. 동적 할당 켤 때 반드시 min/max는 명시적으로 설정해야 해."

**<Executor 개수 제어>**
```
spark.dynamicAllocation.enabled = false → true
# 동적 할당 활성화
# 기본 비활성화

spark.dynamicAllocation.minExecutors = 0 → 2
# 공백 구간에서 반납 후 유지할 최소 Executor 수
# 0이면 공백 후 Executor 없는 상태에서 재요청 
# 콜드스타트 발생, Job 시작 느려짐

spark.dynamicAllocation.maxExecutors = infinity → 20
# Task 큐 증가 시 최대로 늘릴 Executor 수
# 무제한이면 클러스터 자원 독점 가능 
# 멀티테넌트 환경에서 다른 팀 잡 굶김

spark.dynamicAllocation.initialExecutors = 2
# SparkSession 시작 시 처음 요청할 Executor 수
```

**<반납 타이밍 제어>**
```
spark.dynamicAllocation.executorIdleTimeout = 60s → 120s
# Executor가 idle 상태로 이 시간 초과하면 반납
# 공백이 짧으면 길게, 공백이 길면 짧게 설정
# 공백이 짧은 파이프라인에서 너무 자주 반납/재요청 발생
# 오버헤드 줄이려면 timeout 늘림
```

**<요청 타이밍 제어>**
```
spark.dynamicAllocation.schedulerBacklogTimeout = 1s → 5s
# Task 큐가 밀리기 시작하고 이 시간 초과하면 Executor 추가 요청
# 순간적인 Task 급증에 과민반응 방지
# 잠깐 밀렸다고 바로 Executor 요청하면 불필요한 자원 낭비

spark.dynamicAllocation.sustainedSchedulerBacklogTimeout = 1s → 5s
<Task 백로그 발생>
→ schedulerBacklogTimeout(1s) 초과 
→ Executor 추가 요청 (1번) 
→ 그래도 백로그 계속 남아있음 
→ sustainedSchedulerBacklogTimeout 간격으로 추가 요청 반복 
→ maxExecutors 도달까지 계속

# 1s면 Executor 요청을 너무 빠르게 반복 
# YARN/K8s에 컨테이너 요청이 폭발적으로 쌓임 
# 클러스터 스케줄러에 부하 줌 
# 5s 정도로 늘리면 요청 간격 여유 생겨서 클러스터 안정적
```

Task 백로그란?:
- Executor에 할당되지 못하고 대기 중인 Task 목록
```
Job 실행
→ Spark가 Job을 Task 여러 개로 쪼갬
→ Task를 Executor에 할당
→ Executor가 부족하면 남은 Task는 큐에서 대기
→ 이 대기 중인 Task들이 백로그
```


**<필수 동반 설정>**
```
spark.shuffle.service.enabled = false → true 
# Executor 반납 시 shuffle 데이터 유실 방지
# 외부 shuffle service가 데이터 대신 보관
# 이거 없으면 Executor 반납할 때 shuffle 데이터 날아감
```

배경:
- Executor는 shuffle 데이터를 자기 로컬 디스크에 들고 있음
- 동적 할당으로 Executor 반납 시 그 Executor의 shuffle 데이터도 같이 사라짐
- 다른 Executor가 그 shuffle 데이터 읽으러 갔다가 실패
- Job 전체 재실행 발생
```
Executor 반납 전
→ External Shuffle Service가 shuffle 데이터 인계받음
→ Executor 반납
→ 다른 Executor가 External Shuffle Service에서 shuffle 데이터 읽음
→ 데이터 유실 없음
```

> "동적 할당 켜고 shuffle service 안 켜면 언젠가 반드시 터져. Executor 반납 타이밍이 shuffle 데이터 읽는 타이밍이랑 겹치면 Job이 죽어버려. 동적 할당이랑 shuffle service는 세트야. 하나만 켜면 의미없어."

```
false → true 
# 동적 할당 사용 시 필수 
# false 상태에서 Executor 반납하면 shuffle 데이터 유실 
# Job 실패 또는 전체 재실행 발생
```


**<엔지니어 독백>**
> "실무에서 minExecutors를 0으로 두면 공백 구간에 Executor가 완전히 없어져. 다음 Job 시작할 때 콜드스타트로 느려지니까 최소 2개는 유지해. maxExecutors는 반드시 명시해. 안 하면 클러스터 자원 혼자 다 먹어버릴 수 있어."



# 02. Executor 메모리
## (1) Executor 메모리 구조 

### Layer1
```
Executor 메모리는 두가지로 나뉜다. 
-spark.executor.memory (예: 8g)
-spark.executor.memoryOverhead (executor 외부 오버헤드)
```
#### [1] spark.executor.memory (JVM 내부):
- JVM 안에서 관리하는 메모리
- JVM이 직접 관리
- Spark 연산 전체 (Execution, Storage, Reserved, User)

#### [2] spark.executor.memoryOverhead (JVM 외부):
- JVM 밖에서 사용하는 메모리
- PySpark Python 프로세스
- JVM 내부 메타데이터, 스택 메모리
- 네이티브 라이브러리
```
spark.executor.memory (예: 8g)
spark.executor.memoryOverhead (executor 외부 오버헤드)

YARN/K8s 컨테이너 (총 9g)
──────────────────────────────────────
│                        │           │
│   JVM 프로세스            │  JVM 외부 │
│   executor.memory (8g) │  Overhead │
│                        │  (1g)     │
│   Spark이 직접 관리       │           │
│                        │           │
──────────────────────────────────────
```

엔지니어 독백:
> "PySpark 쓸 때 Overhead가 중요해. Python 프로세스가 JVM 밖에서 돌거든. executor.memory만 늘리고 Overhead 안 늘리면 Python 쪽에서 OOM 터져. 에러 메시지가 JVM OOM이랑 달라서 처음엔 원인 찾기 헷갈려."


### Layer2 - [1] spark.executor.memory
```
executor.memory 안은 Reserved, Usable Memory 두 영역으로 나뉜다.
-Reserved Memory(300MB)
-Usable Memory(executor.memory - 300MB)
```

**[A]Reserved Memory**
- 300MB 고정
- Spark 내부 시스템 객체 저장용
- SparkEnv, 내부 메타데이터 등 Spark 자체가 동작하는데 필요한 객체
- 개발자가 설정 불가, 변경 불가
- 이 영역이 없으면 Spark 프로세스 자체가 뜨지 못함
> "executor.memory를 아무리 크게 잡아도 300MB는 Spark가 먼저 가져가. 개발자가 쓸 수 있는 영역이 아니야."


**[B]Usable Memory**
- executor.memory에서 Reserved 300MB 뺀 나머지
- 개발자가 실제로 사용할 수 있는 영역
- 여기서 Unified Memory와 User Memory로 다시 나뉨 → Layer3
```
executor.memory = 8192MB
Reserved        =  300MB
Usable          = 7892MB
```
> "Usable이 실제 사용 가능한 전체 풀이야. 이걸 다시 0.6/0.4로 쪼개서 Spark 내부 연산용이랑 사용자 코드용으로 나누는 거야."

#### spark.executor.memory 전체 구조
```
spark.executor.memory (8g)
│
├── Reserved Memory (300MB 고정)
│   └── Spark 시스템 내부 객체
│
└── Usable Memory (executor.memory - 300MB = 7.7g)
    │
    ├── Unified Memory (Usable × 0.6(spark.memory.fraction) = 4.6g)
    │   │
    │   ├── Execution Memory (Unified × 0.5(1 - storageFraction) = 2.3g)
    │   │   ├── shuffle 중간 데이터
    │   │   ├── sort 버퍼
    │   │   └── aggregation 해시맵
    │   │
    │   └── Storage Memory (Unified × 0.5(storageFraction) = 2.3g)
    │       ├── cache(), persist() 데이터
    │       └── broadcast 변수
    │
    └── User Memory (Usable × 0.4 = 3.1g)
        ├── Python UDF
        └── 사용자 데이터구조
```


### Layer3 - [B]Usable Memory
```
Usable Memory는 Unified Memory와 User Memory 두 영역으로 나뉜다.
관련 설정값 : spark.memory.fraction(0.6)
-Unified Memory(Usable × spark.memory.fraction)
-User Memory(Usable × (1-spark.memory.fraction))
```


**[a]Unified Memory (Usable × 0.6)**
- Spark가 직접 관리하는 영역
- 연산과 캐시에 사용
- 내부에서 Execution / Storage로 다시 나뉨 → Layer4
- 설정값: `spark.memory.fraction = 0.6`


**[b]User Memory (Usable × 0.4)**
- Spark가 관리하지 않는 영역
- 개발자 코드가 사용하는 공간
- Python UDF, 사용자 정의 데이터구조, 커스텀 객체
- 설정값 없음, memory.fraction 바꾸면 자동으로 결정됨
- 초과 시 JVM OOM 발생

```
spark.memory.fraction = 0.6

Usable = 7892MB
Unified Memory = 7892MB × 0.6 = 4735MB
User Memory    = 7892MB × 0.4 = 3157MB
```


### Layer4 - [a]Unified Memory
```
Unified Memory는 Execution Memory와 Storage Memory로 나뉜다
둘은 하나의 풀을 공유하는 구조다.
관련 설정값 : spark.memory.storageFraction(0.5)
```

```
spark.executor.memory (8g)
│
├── Reserved Memory (300MB 고정)
│   └── Spark 시스템 내부 객체
│
└── Usable Memory (executor.memory - 300MB = 7.7g)
    │
    ├── Unified Memory (Usable × 0.6(spark.memory.fraction) = 4.6g)
    │   │
    │   ├── Execution Memory (Unified × 0.5(1 - storageFraction) = 2.3g)
    │   │   ├── shuffle 중간 데이터
    │   │   ├── sort 버퍼
    │   │   └── aggregation 해시맵
    │   │
    │   └── Storage Memory (Unified × 0.5(storageFraction) = 2.3g)
    │       ├── cache(), persist() 데이터
    │       └── broadcast 변수
    │
    └── User Memory (Usable × 0.4 = 3.1g)
        ├── Python UDF
        └── 사용자 데이터구조
```

**Execution Memory (Unified × 0.5)**
- Job 실행 중 연산 중간 데이터 저장
- shuffle 중간 버퍼
- sort 정렬 버퍼
- groupBy, join 해시맵
- 부족하면 디스크로 spill
- 설정값: `(1-spark.memory.storageFraction) = 0.5` 

**Storage Memory (Unified × 0.5)**
- 재사용 데이터 보관
- `df.cache()`, `df.persist()` 데이터
- broadcast 변수
- 부족하면 캐시 일부 evict
- 설정값: `spark.memory.storageFraction = 0.5`

```
Unified Memory = 4735MB

Execution Memory = 4735MB × 0.5 = 2367MB
Storage Memory   = 4735MB × 0.5 = 2367MB
```


**핵심: 상호 침범 가능**
```
Execution 부족
→ Storage 영역 침범
→ Storage 캐시 evict(제거)후 공간 확보
→ Execution이 우선순위 높음

Storage 부족
→ Execution 영역 침범 가능
→ 단, Execution이 사용 중이면 대기
→ Execution은 강제 evict 불가
```

- Evict?
	- **어디서 일어나는가**
		- Storage Memory가 꽉 찬 상태에서 새 데이터를 캐시하려 할 때
		- Execution Memory가 Storage 영역을 침범할 때
	- 제거 후 데이터는 어떻게 되는가
		- `MEMORY_ONLY` 로 cache한 경우 → 그냥 삭제, 다음에 다시 계산
		- `MEMORY_AND_DISK` 로 cache한 경우 → 디스크로 내림

- spill이란
	- Execution Memory가 꽉 찼을 때
	- 처리 중인 중간 데이터를 디스크로 내리는 것
	- 메모리 부족해도 Job이 죽지 않고 느려지면서 계속 실행

- Storage 침범이란
	- Execution Memory가 꽉 찼을 때
	- 디스크 쓰기 전에 Storage 영역을 빌려쓰는 것
	- Storage 캐시 evict 후 그 공간을 Execution이 사용
- spill vs Storage 우선순위는?
```
Execution Memory 부족
→ 1순위: Storage 영역 침범 (캐시 evict)
→ 2순위: 그래도 부족하면 디스크로 spill
```
> "Spark가 디스크 쓰기 전에 Storage 캐시 먼저 날려서 공간 확보하려고 해. 디스크 IO가 훨씬 비싸니까. Storage 침범해도 부족하면 그때 디스크로 spill하는 거야."


<엔지니어 독백>
> "Execution이 Storage보다 우선순위가 높아.   
> Execution이 메모리 더 필요하면 Storage 캐시 강제로 날려버릴 수 있어. 
> 반대로 Storage가 Execution 영역 침범하려면   
> Execution이 그 메모리 다 쓰고 반납할 때까지 기다려야 해. 
> cache() 해놨는데 자꾸 느리다면 Execution이 Storage 영역 침범해서 캐시가 evict된 거야."  



## (2)Executor 메모리 설정 

```
세 가지 영역으로 나눠서 진행하자
- 1단계: 메모리 관련 설정값
- 2단계: SparkSession 실행 관련 설정값
- 3단계: I/O 관련 설정값
```

### 1단계 메모리 관련 설정값
> 메모리 관련 설정값은 executor.memory 구조에서 각 영역 크기를 조절하는 것들이다.

**executor.memory 자체**
```
spark.executor.memory = 1g (기본값)
```
- 기본값: 너무 작아서 실무에서 그대로 쓰는 경우 없음
- 변경: 8g ~ 16g
- 이유: 대용량 데이터 처리 시 OOM 방지, shuffle/aggregation 중간 데이터 수용

**Reserved → Usable 비율 조절**
```
spark.memory.fraction = 0.6 (기본값)
```
- Usable 중 Unified Memory 비율
- 변경: 0.7 ~ 0.8
- 이유: shuffle/sort/cache 작업 많을 때 Unified 키우고 User Memory 줄임
- 주의: Python UDF 많이 쓰면 User Memory 줄이면 안 됨

**Execution ↔ Storage 비율 조절**
```
spark.memory.storageFraction = 0.5 (기본값)
```
- Unified 중 Storage Memory 비율
- Execution = Unified × (1 - storageFraction)
- 변경 케이스 1: shuffle/join 많으면 0.3으로 낮춰 Execution 키움
- 변경 케이스 2: cache 많이 쓰면 0.7로 올려 Storage 키움

**Overhead**
```
spark.executor.memoryOverhead = executor.memory × 0.1 (기본값, 최소 384MB)
```
- JVM 외부 메모리
- 변경: 2g ~ 4g
- 이유: PySpark 사용 시 Python 프로세스가 overhead 영역 사용, 기본값으론 OOM 빈번

**Driver 메모리**
```
spark.driver.memory = 1g (기본값)
spark.driver.memoryOverhead = driver.memory × 0.1 (기본값)
```

- Driver도 Executor와 동일한 메모리 구조
- 변경: 4g ~ 8g
- 이유: `collect()`, `toPandas()` 로 데이터 Driver로 가져올 때 OOM 방지


**오프힙 메모리 (선택적)**
```
spark.memory.offHeap.enabled = false (기본값)
spark.memory.offHeap.size = 0 (기본값)
```
- JVM GC 영향 받지 않는 외부 메모리 사용
- 변경: enabled=true, size=2g ~ 4g
- 이유: GC pause 심할 때 Execution/Storage 일부를 offHeap으로 옮겨 GC 부하 감소

엔지니어 독백:
> "실무에서 OOM 터지면 로그 보고 어느 영역인지 먼저 파악해. Python OOM이면 memoryOverhead, shuffle OOM이면 executor.memory 올리거나 storageFraction 낮춰서 Execution 키우고, cache evict 심하면 storageFraction 올려. 무조건 executor.memory만 올리는 건 비효율적이야."


### 2단계 SparkSession 실행 관련 설정값
> SparkSession 실행 관련 설정값은 Job 실행 방식, 병렬성, 셔플을 제어한다.

**병렬성 관련**
```
spark.sql.shuffle.partitions = 200 (기본값)
```
- shuffle 발생 시 생성되는 파티션 수
- 변경: 데이터 크기에 따라 조절
- 소규모 데이터 (수 GB): 50 ~ 100
- 대규모 데이터 (수 TB): 500 ~ 2000
- 이유: 200은 중간값, 데이터 작으면 빈 파티션 많아져 오버헤드, 크면 파티션 너무 커서 spill

```
spark.default.parallelism = (클러스터 코어 수) (기본값)
```
- RDD 연산의 기본 파티션 수
- DataFrame은 sql.shuffle.partitions 따름
- 변경: executor 수 × executor 코어 수 × 2~3
- 이유: 코어당 2~3개 Task 유지해야 자원 낭비 없음


**Task 관련**
```
spark.task.cpus = 1 (기본값)
```
- Task 하나당 할당 코어 수
- 변경: 머신러닝, 딥러닝 Task는 2~4로 올림
- 이유: 멀티스레드 연산 필요한 Task는 코어 더 필요

```
spark.task.maxFailures = 4 (기본값)
```
- Task 실패 허용 횟수
- 변경: 불안정한 클러스터 환경에서 8~10으로 올림
- 이유: 네트워크 불안정, 노드 장애 빈번한 환경에서 Job 전체 실패 방지


**shuffle 관련**
```
spark.shuffle.compress = true (기본값)
```
- shuffle 데이터 압축 여부
- 기본값 유지 권장
- 이유: 네트워크 전송량 감소 효과가 압축 CPU 비용보다 큼

```
spark.shuffle.spill.compress = true (기본값)
```
- shuffle spill 데이터 압축 여부
- 기본값 유지 권장
- 이유: 디스크 IO 감소

```
spark.shuffle.file.buffer = 32k (기본값)
```
- shuffle write 버퍼 크기
- 변경: 64k ~ 128k
- 이유: 버퍼 크게 하면 디스크 IO 횟수 줄어 shuffle write 성능 향상


**Serialization 관련**
```
spark.serializer = org.apache.spark.serializer.JavaSerializer (기본값)
```
- 변경: org.apache.spark.serializer.KryoSerializer
- 이유: Kryo가 Java 직렬화보다 3~10배 빠르고 메모리 효율 높음
- RDD, shuffle 데이터 직렬화에 영향

---

**AQE (Adaptive Query Execution)**
```
spark.sql.adaptive.enabled = true (Spark 3.0부터 기본값)
```
- 실행 중 쿼리 플랜을 동적으로 최적화
- shuffle 파티션 자동 조절, skew join 자동 처리
- 변경: false로 끄는 경우 거의 없음
- 이유: 켜놓으면 sql.shuffle.partitions 자동 최적화해줌

엔지니어 독백:
> "실무에서 제일 자주 건드리는 게 sql.shuffle.partitions야. 200 그대로 쓰다가 shuffle 느리거나 OOM 터지면 이거 먼저 봐. AQE 켜져있으면 어느정도 자동으로 조절해주긴 하는데, 데이터 크기 편차가 크면 수동으로 잡아주는 게 나아."


### 3단계 - I/O 관련 설정값

**파일 읽기 관련**
```
spark.sql.files.maxPartitionBytes = 128MB (기본값)
```
- 파일 읽을 때 파티션 하나의 최대 크기
- 변경: 256MB ~ 512MB
- 이유: 파티션 수 줄여서 Task 오버헤드 감소, 대용량 파일 읽기 성능 향상

```
spark.sql.files.openCostInBytes = 4MB (기본값)
```
- 파일 오픈 비용 추정값
- 작은 파일 여러 개를 하나의 파티션으로 합칠 때 사용
- 변경: 작은 파일 많은 환경에서 8MB ~ 16MB
- 이유: 작은 파일 문제(small file problem) 완화


**압축 관련**
```
spark.io.compression.codec = lz4 (기본값)
```
- 내부 데이터 압축 코덱
- 변경 옵션: lz4, snappy, zstd
- lz4: 기본값, 속도 빠름
- snappy: 압축률/속도 균형
- zstd: 압축률 높음, CPU 비용 있음
- 이유: 네트워크/디스크 IO 많은 환경에서 zstd로 변경해 전송량 감소

```
spark.sql.parquet.compression.codec = snappy (기본값)
```
- Parquet 파일 쓰기 압축 코덱
- 변경: zstd
- 이유: zstd가 snappy보다 압축률 높아 스토리지 비용 절감, S3 저장 비용 감소


**네트워크 관련**
```
spark.network.timeout = 120s (기본값)
```
- 네트워크 통신 타임아웃
- 변경: 300s ~ 600s
- 이유: 대용량 shuffle, 느린 네트워크 환경에서 timeout으로 Job 실패 방지

```
spark.rpc.message.maxSize = 128MB (기본값)
```
- Driver ↔ Executor RPC 메시지 최대 크기
- 변경: 256MB ~ 512MB
- 이유: broadcast 변수 크거나 Task 결과 클 때 메시지 크기 초과로 실패 방지


**S3 관련 (클라우드 환경)**
```
spark.hadoop.fs.s3a.multipart.size = 104857600 (100MB 기본값)
```
- S3 멀티파트 업로드 청크 크기
- 변경: 256MB ~ 512MB
- 이유: 대용량 파일 쓰기 시 청크 크게 해서 요청 횟수 줄임

```
spark.hadoop.fs.s3a.connection.maximum = 100 (기본값)
```
- S3 동시 연결 수
- 변경: 200 ~ 500
- 이유: Executor 많을 때 S3 연결 부족으로 병목 발생 방지


**캐시/persist 관련**
```
spark.storage.level = MEMORY_AND_DISK (기본값)
```
- cache() 호출 시 기본 저장 레벨
- 변경 옵션:
- MEMORY_ONLY: 메모리만, evict 시 재계산
- MEMORY_AND_DISK: 메모리 부족 시 디스크
- MEMORY_ONLY_SER: 직렬화해서 메모리 절약
- 이유: 메모리 충분하면 MEMORY_ONLY, 부족하면 MEMORY_AND_DISK


엔지니어 독백:
> "S3 환경에서 제일 자주 건드리는 게 parquet 압축 코덱이야. snappy에서 zstd로 바꾸면 파일 크기 30~40% 줄어들어서 스토리지 비용이랑 읽기 속도 둘 다 잡을 수 있어. network.timeout은 대용량 shuffle 잡에서 꼭 늘려줘야 해. 기본값 120s로는 shuffle 중간에 timeout 터지는 경우 있어."


### 기타 - 책에언급된 설정값

**shuffle write 버퍼**

```
spark.shuffle.file.buffer = 32k (기본값)
```
- shuffle map Task가 디스크에 쓸 때 사용하는 버퍼
- 변경: 64k ~ 128k
- 이유: 버퍼 크게 하면 디스크 write 횟수 줄어 IO 감소

```
spark.shuffle.unsafe.file.output.buffer = 32k (기본값)
```
- Tungsten shuffle write 시 사용하는 버퍼
- 변경: 64k ~ 128k
- 이유: file.buffer와 같은 목적, Tungsten 엔진 사용 시 이 값이 적용됨


**파일 전송**
```
spark.file.transferTo = true (기본값)
```

- OS 레벨 zero-copy 파일 전송 사용 여부
- NIO transferTo() 사용
- 변경: 일부 NFS/네트워크 파일시스템 환경에서 false
- 이유: 특정 파일시스템에서 transferTo 버그 있어 false로 꺼야 하는 경우 있음


**압축 블록 크기**
```
spark.io.compression.lz4.blockSize = 32k (기본값)
```
- lz4 압축 시 블록 크기
- 변경: 64k ~ 128k
- 이유: 블록 크게 하면 압축률 향상, 메모리 사용량 증가 트레이드오프


**External Shuffle Service 관련**
```
spark.shuffle.service.index.cache.size = 100m (기본값)
```
- External Shuffle Service가 shuffle 인덱스 파일 캐시하는 크기
- 변경: 200m ~ 300m
- 이유: Executor 많고 shuffle 파일 많을 때 인덱스 캐시 부족으로 성능 저하 방지

```
spark.shuffle.registration.timeout = 5000ms (기본값)
```
- Executor가 External Shuffle Service에 등록할 때 타임아웃
- 변경: 10000ms ~ 15000ms
- 이유: 클러스터 부하 높을 때 등록 타임아웃으로 Executor 실패 방지

```
spark.shuffle.registration.maxAttempts = 3 (기본값)
```
- Shuffle Service 등록 실패 시 재시도 횟수
- 변경: 5 ~ 10
- 이유: 네트워크 불안정 환경에서 재시도 횟수 늘려 등록 실패 방지


엔지니어 독백:
> "file.buffer랑 unsafe.file.output.buffer는 세트야. Tungsten 쓰면 unsafe 버퍼가 적용되는데 둘 다 같이 올려줘야 효과 있어. shuffle service 관련 설정은 동적 할당 쓸 때 필수로 챙겨야 해. registration.timeout 기본값 5초는 클러스터 바쁠 때 너무 짧아서 Executor 등록 실패하는 경우 봤어."


## (3)주로 변경하는 설정값 정리 

**AQE가 자동 처리하는 것 (건드릴 필요 없음)**
- `spark.sql.shuffle.partitions` → AQE가 런타임에 자동 조절
- `spark.default.parallelism` → AQE가 커버
- skew join 처리 → 자동 감지


**실무에서 실제로 설정하는 것들**
자원 관련 (필수):
```
spark.executor.memory = 8g~16g
spark.executor.memoryOverhead = 2g~4g   # PySpark 쓰면 필수
spark.driver.memory = 4g~8g
```

동적 할당 (클러스터 공유 환경 필수):
```
spark.dynamicAllocation.enabled = true
spark.dynamicAllocation.minExecutors = 2
spark.dynamicAllocation.maxExecutors = 20
spark.shuffle.service.enabled = true
```

직렬화 (성능 향상):
```
spark.serializer = KryoSerializer   # 기본값이 느림
```

압축 (S3/클라우드 환경):
```
spark.sql.parquet.compression.codec = zstd
```

네트워크 (대용량 잡):
```
spark.network.timeout = 300s   # 기본 120s는 대용량 shuffle에서 자주 터짐
```



**나머지는 언제 건드리나**
- `storageFraction`: cache 많이 쓰거나 shuffle OOM 터질 때
- `memory.fraction`: GC pause 심하거나 User Memory OOM 터질 때
- S3 설정: 대용량 파일 쓰기 병목 생길 때
- shuffle buffer 설정: shuffle 성능 마지막 단계 튜닝할 때

엔지니어 독백:
> "처음부터 다 설정하는 거 아니야. executor.memory, memoryOverhead, 동적할당, serializer 이 정도만 잡고 잡 돌려봐. 문제 생기면 Spark UI 보고 병목 파악한 다음 그 부분만 건드리는 거야. 나머지는 AQE가 알아서 해줘."

<br><br><br><br><br>
---















# Part2

# 01.파티션

## (1)파티션 종류와 개념

### 파티션 종류
1. Input Partition - 파일 읽을 때 결정 (HDFS block, S3 파일 수 기준)
2. Shuffle Partition - `groupBy`, `join` 등 셔플 발생 시 재분배 (기본값 200)
3. Output Partition - 저장 시 파일 수 결정

- 실무에서 문제가 되는 상황
	- Data Skew: 특정 파티션만 데이터가 몰려 그 Task만 느림
	- Shuffle Partition 200 고정: 작은 데이터엔 과도하고, 큰 데이터엔 부족
	- 작은 파일 대량 생성: 파티션이 많으면 S3/HDFS에 작은 파일 수백 개 생성 → "Small File Problem"

### 셔플파티션
#### 개념
- `groupBy`, `join`, `distinct`, `orderBy` 같은 연산은 데이터를 특정 키 기준으로 같은 파티션에 모아야 한다. 이때 네트워크를 통해 데이터를 재분배하는 과정이 셔플이다.
```
셔플 전 (Input Partition)          셔플 후 (Shuffle Partition)
Partition 1: [A, C, B]             Partition 1: [A, A, A] ← A 전부 모임
Partition 2: [A, B, C]    →        Partition 2: [B, B, B] ← B 전부 모임
Partition 3: [C, A, B]             Partition 3: [C, C, C] ← C 전부 모임
```
- 전체 작업흐름 - Shuffle
```
Job
└── Stage 1 (셔플 전)
│    ├── Task 1 → Partition 1 → Core 1 (Executor A)
│    ├── Task 2 → Partition 2 → Core 2 (Executor A)
│    └── Task 3 → Partition 3 → Core 1 (Executor B)
└── Stage 2 (셔플 후)
     ├── Task 4 → Partition 4 → Core 2 (Executor B)
     └── Task 5 → Partition 5 → Core 1 (Executor A)

- Job: Action(count, save 등) 하나가 호출될 때 생성되는 최상위 작업 단위
- Stage: 셔플 발생 지점을 기준으로 Job이 쪼개진 단위. 셔플 없으면 Stage 1개
- Task: Stage 안에서 파티션 1
```
#### 예시
> 셔플은 특정 키가 같은 데이터를 같은 파티션으로 모으는 네트워크 재분배 과정이다.

**실무 예시: 사용자별 구매 합계 (groupBy)**
초기 데이터가 3개 Executor에 분산되어 있다:
```
Executor 1 Partition 1        Executor 2 Partition 2        Executor 3 Partition 3
user_id | amount               user_id | amount               user_id | amount
--------|-------               --------|-------               --------|-------
kim     | 3000                 lee     | 5000                 kim     | 2000
lee     | 1000                 kim     | 4000                 park    | 6000
park    | 2000                 park    | 1000                 lee     | 3000
```

`groupBy("user_id").sum("amount")` 실행하면:
kim의 데이터가 Partition 1, 2, 3에 흩어져 있음 → 한 곳에 모아야 합산 가능

셔플 발생:
```
Executor 1 Partition 1        Executor 2 Partition 2        Executor 3 Partition 3
user_id | amount               user_id | amount               user_id | amount
--------|-------               --------|-------               --------|-------
kim     | 3000    →→→          kim     | 4000    →→→          kim     | 2000
lee     | 1000    →→→          lee     | 5000                 lee     | 3000   →→→
park    | 2000                 park    | 1000    →→→          park    | 6000   →→→
```

셔플 후:
```
Shuffle Partition 1           Shuffle Partition 2           Shuffle Partition 3
user_id | amount               user_id | amount               user_id | amount
--------|-------               --------|-------               --------|-------
kim     | 3000                 lee     | 1000                 park    | 2000
kim     | 4000                 lee     | 5000                 park    | 1000
kim     | 2000                 lee     | 3000                 park    | 6000
         ↓                              ↓                              ↓
        9000                           9000                           9000
```


**셔플이 비싼 이유**
- 네트워크 전송: 다른 Executor로 데이터를 직접 보냄
- 디스크 I/O: 셔플 데이터를 임시 파일로 디스크에 썼다가 읽음
- Stage 경계: 셔플 전 Stage가 100% 완료되어야 다음 Stage 시작 가능

#### 셔플파티션수
**셔플 파티션 수가 중요한 이유**
- 셔플 파티션 수 = 셔플 후 생성되는 파티션 수 = 이후 Task 수
- 이 값이 실제 데이터 크기와 맞지 않으면 두 가지 문제 발생

- 너무 작을 때 (예: 데이터 1TB인데 파티션 10개):
	- 파티션 1개가 너무 커짐 → 메모리 초과 → 디스크 Spill 발생
	- Task 수가 코어 수보다 적으면 코어가 놀게 됨

- 너무 클 때 (예: 데이터 1GB인데 파티션 2000개):
	- 파티션 1개가 몇 KB 수준 → Task 스케줄링 오버헤드가 실제 처리보다 큼
	- 저장 시 작은 파일 수천 개 생성 → Small File Problem


#### AQE와 셔플 파티션수 설정으로부터 해방
**AQE가 이 문제를 해결한 배경**
```
Spark 3.0 이전:
- 셔플 파티션 수를 실행 전에 고정으로 설정해야 했음
- 실제 데이터 분포를 모르는 상태에서 추정값으로 설정 → 맞추기 어려움

Spark 3.0 AQE 도입 후:
- 셔플 완료 후 실제 데이터 크기를 보고 파티션을 동적으로 합침
- 작은 파티션들을 자동으로 coalesce → Small File Problem 완화
- `spark.sql.adaptive.enabled=true` (기본값)

shuffle.partitions=200 설정 
→ 셔플 후 파티션 200개 생성 시도 
→ AQE coalescePartitions=true 이면 
→ 실제 데이터 크기 확인 
→ minPartitionSize 기준으로 작은 파티션 병합 
→ 최종 파티션 수 결정 (200보다 적을 수 있음)
```
- 설정 전략
```
<실무 설정 기준>
파티션 1개당 적정 크기는 128MB~256MB 기준:
- 전체 셔플 데이터 크기 ÷ 128MB = 적정 파티션 수
- 예: 셔플 데이터 20GB → 20000MB ÷ 128MB = 약 150~160개
<AQE에 따른 설정>
- AQE 켜져 있으면 `shuffle.partitions`를 크게 잡고 (500~1000) AQE가 줄이게 둠
- AQE 꺼져 있으면 데이터 크기 ÷ 128MB로 직접 계산해서 설정
```
> AQE 켜져 있으면 `spark.sql.shuffle.partitions`를 넉넉하게 크게 잡고 AQE가 알아서 줄이게 두는 전략도 많이 씀

---

## (2)파티션 관련 설정값
Spark는 Job→Stage→Task로 쪼개고, Task 1개=파티션 1개=코어 1개가 처리한다. 설정값은 이 흐름의 각 지점을 튜닝하는 것이다.

### 전체 흐름
```
Action 호출
└── Job 생성
    └── Stage 분리 (셔플 기준)
        └── Task 생성 (파티션 수만큼)
            └── Executor의 Core가 Task 실행
```


### 1단계: 작업 단위
- Job: `count()`, `save()` 등 Action 1번 호출 = Job 1개
- Stage: 셔플(groupBy, join) 발생 지점에서 잘림. 셔플 없으면 Stage 1개
- Task: 파티션 1개를 처리하는 최소 실행 단위. Stage 안에서 파티션 수만큼 생성


### 2단계: 파티션 종류
- Input Partition: 파일 읽을 때 생성. S3/HDFS 블록 크기 기준으로 결정
- Shuffle Partition: groupBy/join 후 셔플로 재분배될 때 생성
- Output Partition: 저장 시 파일 수 결정

관련 설정값:

| 설정                                  | 기본값   | 권장        | 이유                        |
| ----------------------------------- | ----- | --------- | ------------------------- |
| `spark.sql.files.maxPartitionBytes` | 128MB | 128~256MB | HDFS 블록 크기 기준 I/O 최적      |
| `spark.sql.shuffle.partitions`      | 200   | 데이터 크기 비례 | 기본값 200은 범용값, 실제 데이터와 불일치 |

---

### 3단계: Executor / Core / Thread
- Executor: JVM 프로세스 1개. 워커 노드에 띄워지는 실행 컨테이너
- Core: Executor 안에서 Task를 동시에 실행할 수 있는 슬롯 수
- Thread: Core 1개 = Thread 1개. Spark는 멀티스레드 아님, 코어 수가 곧 동시 실행 수

실제 동작:
```
Executor A (코어 4개)
├── Core 1 → Task 1 처리 중
├── Core 2 → Task 2 처리 중
├── Core 3 → Task 3 처리 중
└── Core 4 → Task 4 처리 중
```

관련 설정값:

| 설정                          | 기본값     | 권장          | 이유                                             |
| --------------------------- | ------- | ----------- | ---------------------------------------------- |
| `spark.executor.cores`      | 1       | 4~5         | 너무 많으면 HDFS I/O 경합, 너무 적으면 Executor 수 증가로 오버헤드 |
| `spark.executor.memory`     | 1g      | 워크로드 기준     | 부족하면 Spill(디스크로 넘침) 발생                         |
| `spark.executor.instances`  | 2       | 클러스터 리소스 기준 | 전체 코어 수 = instances × cores                    |
| `spark.default.parallelism` | 전체 코어 수 | 코어 수 × 2~3  | 코어 1:1이면 느린 Task에서 코어 대기 발생                    |

---

### 4단계: 전체 연결 관점
- 파티션 수가 Task 수를 결정하고
- Task 수가 Executor 코어 수보다 많아야 코어가 놀지 않고
- 파티션 크기가 균등해야 특정 코어만 오래 걸리는 Skew가 안 생김

실무 체크포인트:
- 전체 Task 수 < 전체 코어 수 → 파티션 늘려야 함 (`repartition`)
- 특정 Task만 오래 걸림 → Skew. `repartition` 또는 AQE 활성화
- 저장 파일이 너무 많음 → `coalesce`로 줄이기


### 문제상황별 파티션 수정 
**1. repartition(n)**
- 셔플 발생: O
- 파티션 수: 늘리기/줄이기 모두 가능
- 데이터 분배: 균등 분배
- 사용 시점: 파티션이 skew되어 있을 때, 이후 무거운 연산(join, groupBy) 전에 균등하게 재분배할 때
- 이유: 셔플 비용을 감수하더라도 균등 분배가 전체 처리 속도에 유리할 때 사용

**2. coalesce(n)**
- 셔플 발생: X (기본적으로)
- 파티션 수: 줄이기만 가능
- 데이터 분배: 인접 파티션끼리 합침 → 불균등할 수 있음
- 사용 시점: 최종 저장 직전에 파일 수를 줄일 때
- 이유: 셔플 없이 파티션을 합치므로 비용이 저렴. 단, 늘리는 건 불가능하고 skew 가능성 있음

**3. partitionBy(column)**
- 셔플 발생: O
- 파티션 수: 컬럼 값의 카디널리티에 따라 결정
- 데이터 분배: 특정 컬럼 값 기준으로 분배
- 사용 시점: S3/HDFS 저장 시 디렉토리 구조로 나눌 때 (e.g. `year=2024/month=01/`)
- 이유: 이후 쿼리에서 해당 컬럼으로 필터링 시 해당 디렉토리만 읽음 → Partition Pruning으로 I/O 대폭 감소

**요약**
- 처리 중 skew 해소 → `repartition`
- 저장 직전 파일 수 줄이기 → `coalesce`
- 저장 시 쿼리 최적화 목적 → `partitionBy`


# 02.캐싱과 영속화
> 캐싱과 영속화는 한번 계산한 결과를 메모리/디스크에 저장해서 재계산을 막는 것이다.

## (1)개념 

Spark는 기본적으로 Action이 호출될 때마다 lineage를 처음부터 재실행한다. 같은 DataFrame을 여러 번 쓰면 매번 재계산 → 비용 낭비


**cache() vs persist()**
- `cache()`: 메모리에만 저장. `persist(StorageLevel.MEMORY_ONLY)`와 동일
- `persist()`: 저장 위치와 방식을 직접 지정 가능

주요 StorageLevel:

|레벨|메모리|디스크|설명|
|---|---|---|---|
|MEMORY_ONLY|O|X|기본. 메모리 부족 시 재계산|
|MEMORY_AND_DISK|O|O|메모리 부족 시 디스크로 넘김|
|DISK_ONLY|X|O|메모리 아낄 때|


**실무 사용 시점**
- 같은 DataFrame을 2번 이상 Action에서 사용할 때
- 반복 ML 학습 루프에서 같은 데이터 반복 사용할 때
- 조인 전 전처리된 중간 결과물을 재사용할 때


**주의점**
- 다 쓴 후 `unpersist()` 명시적으로 호출해야 메모리 해제
- 캐싱도 결국 Action 호출 시점에 실제로 저장됨 (lazy)


## (2)사용법  

### cache()
python
```python
df = spark.read.parquet("s3://bucket/orders")
df.cache()

# 첫 Action에서 실제 캐싱 발생
df.count()

# 이후 Action들은 캐시에서 읽음
df.groupBy("user_id").sum("amount").show()
df.filter("status = 'complete'").show()

# 다 쓰면 해제
df.unpersist()
```

```python
# 무거운 join 결과를 여러 번 쓸 때
joined_df = orders.join(users, "user_id").join(products, "product_id")
joined_df.cache()
joined_df.count()  # 캐시 적재

# 이후 3번의 Action이 캐시에서 읽음
region_summary = joined_df.groupBy("region").sum("amount")
user_summary = joined_df.groupBy("user_id").count()
complete_orders = joined_df.filter("status = 'complete'")

region_summary.write.parquet("s3://bucket/region")
user_summary.write.parquet("s3://bucket/user")
complete_orders.write.parquet("s3://bucket/complete")

joined_df.unpersist()
```

###  persist() - StorageLevel 지정
python
```python
from pyspark import StorageLevel

df = spark.read.parquet("s3://bucket/orders")

# 메모리 부족 시 디스크로 넘김
df.persist(StorageLevel.MEMORY_AND_DISK)

df.count()  # 이 시점에 실제 저장

df.unpersist()
```
- persist(MEMORY_AND_DISK) - 적합한 상황
	- 데이터가 커서 메모리만으론 부족할 때:
```python
from pyspark import StorageLevel

# 수백 GB 규모 DataFrame
large_df = spark.read.parquet("s3://bucket/large_events")
large_df.persist(StorageLevel.MEMORY_AND_DISK)
large_df.count()

summary1 = large_df.groupBy("event_type").count()
summary2 = large_df.groupBy("date").sum("value")

large_df.unpersist()
```
- **persist(DISK_ONLY) - 적합한 상황**
	- 메모리를 아껴야 하고 재계산 비용이 매우 클 때:

```python
from pyspark import StorageLevel

# 재계산이 매우 비싼 ML feature 전처리 결과
feature_df = raw_df \
    .join(metadata, "id") \
    .groupBy("user_id") \
    .agg({"amount": "sum", "count": "count"})

# 메모리 아끼면서 디스크에 저장
feature_df.persist(StorageLevel.DISK_ONLY)
feature_df.count()

# ML 학습 반복 루프
for params in param_grid:
    model = train(feature_df, params)
    evaluate(model, feature_df)

feature_df.unpersist()
```


## (3) 주의할점
**사용해야 할 때**
1. 같은 DataFrame으로 여러 Action 실행할 때
    - 집계, 필터, 저장 등 2개 이상 Action에서 동일 DataFrame 사용
2. 반복 연산 루프 안에서 같은 데이터 계속 읽을 때
    - ML 학습 반복, 파라미터 튜닝
3. 무거운 전처리(join, groupBy) 결과를 여러 곳에서 쓸 때
    - 비싼 연산 결과를 한 번만 계산하고 재사용


**사용하면 안 될 때**
1. Action이 1번뿐일 때
    - 캐싱 자체가 메모리 점유 비용인데 재사용이 없으면 낭비
2. 데이터가 너무 클 때
    - 메모리 초과 시 다른 캐시/실행 중인 데이터를 밀어냄 (eviction) → 오히려 성능 저하
3. 데이터가 자주 바뀔 때
    - 스트리밍, 실시간 데이터는 캐시가 stale 데이터를 참조할 수 있음
4. 단순 파이프라인 한 번 실행할 때
    - ETL 파이프라인처럼 읽고 변환하고 저장으로 끝나는 경우
```
이 DataFrame을 Action에서 몇 번 쓰는가?
├── 1번 → 캐싱 X
├── 2번 이상 → 캐싱 O
└── 데이터 크기가 클러스터 메모리의 30% 초과 → MEMORY_AND_DISK 사용
```
