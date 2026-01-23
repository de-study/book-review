
# 01.스파크의 시작
- 구글로부터 시작됨
- RDBMS기반으로 구글 검색시스템을 감당하기 힘듬
- 구글의 GFS(구글파일시스템)을 기반으로 논문을 발표 -> HDFS, 맵리듀스(MR)의 청사진
- GFS 특징 - 장애 내구성이 있는 분산 파일 시스템
- 맵리듀스 특징 - 분산된 데이터에 대한 메타데이터 기반(지역성,랙의 근접성) 고려한 연산 코드 실행
- 새로운 종류의 워크로드를 다루기 위해 각종 에코시스템을 덧붙임: 아파치하이브, 아파치 임팔라, 아파치 지라프, 아파치 드릴, 마하웃.. 등)
<br><br><br> 




# 02.아파치 스파크란
## (1)설계 철학
- 정의 : 대규모 분산 데이터 처리를 위해 설계된 통합형 엔진
- 설계철학
```
- 속도(메모리)  
- 사용편의성(java해방)   
- 모듈성(pyspark라이브러리)  
- 확장성(DF)  
```

<br>
 
  
### 속도
  - 하드웨어의 발전으로 메모리를 사용해서 디스크IO로 인한 성능저하를 피함,
  - DAG기반의 Task로 워커노드로 병렬로 처리,
  - SQL 최적화 코드 생성하는 텅스텐(엔진)

### 사용편리성
- Hadoop MapReduce는 Java로 Mapper/Reducer 클래스를 직접 작성해야 했고, 매번 컴파일/배포/실행이라는 3단계를 거쳐야 했음.
```
"매출 1000 이상만 필터링해서 합계 내라고?
- 필터링 → 변환 → 집계 3단계
1. FilterMapper.java 작성
2. SumReducer.java 작성  
3. Driver.java 작성
4. javac로 컴파일
5. jar로 패키징
6. 클러스터에 업로드
7. hadoop jar 명령어 실행
8. 20분 기다림
```
- Python, Scala, Java, R 선택 가능
```
# Python으로 작성 (컴파일 불필요)
result = (spark.textFile("input.txt")
    .filter(lambda x: int(x.split(",")[2]) >= 1000)  # 필터링
    .map(lambda x: (x.split(",")[0], int(x.split(",")[2])))  # 변환
    .reduceByKey(lambda a, b: a + b))  # 집계

result.saveAsTextFile("output")
```
- 이걸 가능하게 만든 것 - RDD/트랜스포메이션/액션
  - RDD : 데이터를 어떻게 만들지에 대한 레시피(Lineage), "변환 히스토리를 기록하는 메타데이터 객체
  - 트랜스포메이션 : 연산 종류
  - 액션 : 연산 실행을 위해 행하는 것, 데이터 , 데이터 메모리에 올리기
```
rdd = sc.textFile("data.txt")  
# rdd 객체 안에는:
# - "data.txt를 읽어라"라는 지시사항만 저장됨
# - 실제 파일 내용은 안 읽음
# - 각 파티션 위치 정보만 기록
```
```
# RDD의 의미
rdd = sc.textFile("data.txt")  
-- rdd 객체 안에는:
- "data.txt를 읽어라"라는 지시사항만 저장됨
- 실제 파일 내용은 안 읽음
- 각 파티션 위치 정보만 기록

# 트랜스포메이션 종류   
- map: 각 요소를 1:1 변환
- filter: 조건 맞는 요소만 남김
- flatMap: 각 요소를 0개 이상으로 변환
- reduceByKey: 같은 키끼리 집계
- join: 두 RDD를 키로 결합

# 액션 종류  
- collect(): 전체 데이터를 드라이버로 가져옴
- count(): 개수만 반환
- first(): 첫 요소 하나만
- take(n): n개만 가져옴
- saveAsTextFile(): HDFS/S3에 저장
- foreach(): 각 요소마다 함수 실행
```

### 모듈성
- (기존) Hadoop생태계는 작업 타입마다 별도 시스템을 설치/운영해야 했음.
- (변경) Spark는 하나의 엔진으로 배치/스트리밍/ML/그래프/SQL을 모두 처리하도록 설계.
  - 라이브러리 형태로 제공
```
# 기존방식(하둡) 
PM: '유저 로그 배치 처리하고, 실시간 추천도 하고, 
     머신러닝으로 이탈 예측도 해야 해요.'

나: '...그러면 시스템이 3개 필요한데요?'

1. 배치 처리 → Hadoop MapReduce
   - HDFS에 데이터 저장
   - Java로 MapReduce Job 작성
   - Cron으로 매일 실행

2. 실시간 처리 → Apache Storm
   - 별도 클러스터 구축
   - Topology 작성 (Spout/Bolt)
   - Kafka 연동

3. 머신러닝 → Apache Mahout
   - MapReduce 기반이라 느림
   - 반복 학습마다 디스크 I/O

인프라팀: '서버 30대 더 필요합니다'
나: '...예산이...'

그리고 이 시스템들 데이터 주고받으려면:
- MapReduce 결과 → HDFS 저장
- Storm이 HDFS 읽기 (또는 Kafka 경유)
- Mahout이 HDFS에서 학습 데이터 로드
```
> 중요한건, 여러 서버가 필요할뻔했던걸 스파크 하나의 클러스터에서 처리 가능해졌다는 것!

```
# 같은 SparkContext로 모든 워크로드 처리

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.ml import Pipeline

# 1. 배치 처리 (RDD)
sc = SparkContext()
rdd = sc.textFile("hdfs://logs/")
result = rdd.filter(...).map(...).reduceByKey(...)

# 2. SQL 분석 (DataFrame)
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("hdfs://data/")
df.createOrReplaceTempView("users")
spark.sql("SELECT * FROM users WHERE age > 30")

# 3. 스트리밍 (DStream)
ssc = StreamingContext(sc, 1)
stream = ssc.socketTextStream("localhost", 9999)
stream.map(...).reduceByKey(...)

# 4. 머신러닝 (MLlib)
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression()
model = lr.fit(df)
```

<br>

### 확장성
- 다양한 데이터소스를 DF기반으로 동일한 양식으로 만들어서 처리가능하도록 만듬.  
- 아파치 하둡, 카산드라, Hbase, 몽고DB, RDBMS 등의 여러 데이터 소스를 통합 연산 처리 가능하게 만듬.

<br><br><br> 


## 03.스파크의 분산 실행 
- 분산 실행 방식
  - [클러스터 매니저] - [executor1], [executor2]
> 여기서 중요한 개념이 스파크 session이다
> session은 스파크 job과 그것을 실행할 클러스터의 연결지점이자 실행시 필요한 메타데이터를 지정해주는 역할을 함.  
```
SparkSession 없으면:
- 어느 서버에서 실행할지 모름
- 메모리 얼마 쓸지 모름
- Executor 몇 개 띄울지 모름
- 코드를 누가 받아서 실행하나?

SparkSession이 이 모든 걸 세팅해주는 거였어."
```
### 기존방식
- **Hadoop MapReduce:**
```
java// 매번 Job 객체 새로 생성
Job job1 = new Job(conf);
job1.submit();
// job1 종료, 리소스 해제

Job job2 = new Job(conf);  
job2.submit();
// 클러스터 연결 다시 맺음
문제점:

작업마다 클러스터 연결 새로 수립
설정 매번 전달
메타데이터 공유 불가
대화형 분석 불가능
```
- 문제점:
  - 작업마다 클러스터 연결 새로 수립
  - 설정 매번 전달
  - 메타데이터 공유 불가
  - 대화형 분석 불가능
### 개선된 스파크방식
```
spark = SparkSession.builder \
    .master("spark://cluster:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "10") \
    .getOrCreate()

[드라이버 프로그램 - 당신 로컬/노트북]
    ↓
SparkSession 생성
    ↓
클러스터 마스터에 요청:
"Executor 10개 띄워줘
 각각 메모리 4GB, 코어 2개로"
    ↓
[마스터 노드]
워커 노드들에게 지시
    ↓
[워커 노드 1]     [워커 노드 2]     [워커 노드 3]
Executor 시작     Executor 시작     Executor 시작
메모리 4GB 할당   메모리 4GB 할당   메모리 4GB 할당
    ↓                 ↓                 ↓
드라이버와 통신 채널 확립
```

> 궁금한점
> 근데 그래서 spark는 java로 실행되는거 아니야?
> 근데 어떻게 저렇게 python으로 실행하고 
> 대화형으로 가능하게 만든거지?
> 자동으로 빌드,배포되도록 만들었나?

>결론: Spark 엔진은 Java/Scala(JVM)로 실행되고, Python은 PySpark를 통해 JVM에 명령만 전달. Python 코드 자체는 실행 안 되고, Python이 "JVM 리모컨" 역할만 함.
```
Hadoop MapReduce:
- Java로 컴파일 필수
- Python Streaming API 있었지만 느리고 제한적
- 대화형 실행 불가능

Spark의 목표:
- JVM 성능 유지
- Python 편의성 제공
- 실시간 대화형 지원
```

### context, session
- SparkContext: Spark 1.x 시절의 클러스터 연결 진입점 (RDD 전용)
- SparkSession: Spark 2.0+의 통합 진입점 (RDD, DataFrame, SQL 모두 지원)

- context가 나온이유
 - Spark가 발전하면서:
  - DataFrame API 추가 → SQLContext 필요
  - Streaming 추가 → StreamingContext 필요
  - Hive 연동 → HiveContext 필요

- SparkSession (현세대)
 - 발생 배경
 - Spark 2.0에서 여러 Context를 하나로 통합:
  - 이전 기술의 한계: Context가 너무 많아 복잡함
  - 해결 방법: 단일 진입점으로 통합
