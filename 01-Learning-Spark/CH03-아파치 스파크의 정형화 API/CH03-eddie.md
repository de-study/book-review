# INTRODUCTION
- 데이터 프레임이 나오게된 배경  
- 스파크 SQL엔진
<br><br>

# 01.RDD 
## (1)발생배경

### Hadoop-MapReduce의 한계 :
- Hadoop(하둡) : 분산 시스템 프레임워크, 2006년 Yahoo가 Google 논문 보고 만든 오픈소스
  - 구성요소
    - HDFS (Hadoop Distributed File System)
    - YARN (Yet Another Resource Negotiator) : 클러스터 리소스 관리
    - MapReduce 엔진
- MapReduce(맵리듀스): 데이터를 분산 처리하는 방식, 하둡에서 데이터 처리 엔진, 2004년 Google 논문에서 발표한 분산 처리 패러다임
  - Map 함수: 데이터를 key-value 쌍으로 변환
  - Reduce 함수: 같은 key의 value들을 집계
  - Spark에서도 map(), reduce() 쓰면 MapReduce 패러다임 사용함  
- MapReduce 예시
```
입력: 문서들

Map: (문서) → (단어, 1) 쌍들 생성
Shuffle: 같은 단어끼리 모음
Reduce: (단어, [1,1,1...]) → (단어, 합계)

출력: 단어별 빈도수
```
- 한계점(성능)
  - 매 작업마다 디스크 I/O 발생 (Map 결과를 HDFS에 쓰고, Reduce가 읽음)
  - 반복 작업(머신러닝, 그래프 처리)에서 엄청난 성능 저하
  - 메모리 사용안함 -> 중간 결과를 메모리에 캐싱할 방법이 없음
  - 중간에 뻑나면 끝. 다시 돌려야함  
<br><br>

### Spark-RDD-DataFrame까지 :   
> 하둡의 처리방식 엔진인 맵리듀스의 한계를 극복하기위해 spark가 나옴.  
> 하둡의 버전업이라고하기엔 처리 엔진이 완전히 달라졌으므로 새로운 프레임워크 명칭을 달고 나온듯   
> 하지만 하둡과의 호환성을 위해 기존 HDFS,Yarn을 그대로 사용가능하게 만듬

<details>
  <summary>Dataframe의 배경</summary>

  - Dataframe의 시작
  ```   
  R의 data.frame (1990년대) → Pandas DataFrame (2008) → Spark DataFrame (2014)
  ```
  - Dataframe을 사용하는 이유
  ```
  - 행렬(matrix)과 달리 컬럼마다 다른 타입 가능
  - 통계 분석에 최적화된 구조
  - SQL처럼 subset, merge, aggregate 가능
  ```
  끝~
  ---
  <br><br> 
  
</details>




- Spark의 해결책
  - HDFS 그대로 사용 (검증된 분산 파일시스템)
  - YARN으로 리소스 공유 (MapReduce와 Spark 동시 실행 가능)
  - MapReduce 엔진만 교체 (Spark 엔진으로) 
<br> 

**정리하자면,**
- Spark가 Hadoop에서 가져온 것
  - HDFS (분산 파일 저장)
  - YARN (리소스 관리, 선택사항)

- Spark가 버린 것
  - MapReduce 실행 엔진
  - 디스크 기반 shuffle
  - Job 단위 실행 모델

- Spark가 새로 만든 것
  - RDD 추상화 -> Dataframe으로 상위 API로 감싸서 업그레이드 됨   
  - DAG 기반 실행 엔진
  - 메모리 기반 처리
  - 통합 애플리케이션 모델
```
┌─────────────────┬──────────────────┐
│   Hadoop        │     Spark        │
├─────────────────┼──────────────────┤
│ HDFS            │ (HDFS 그대로)      │
│ YARN            │ (YARN 선택사용)    │
│ MapReduce 엔진   │ Spark Core 엔진   │
└─────────────────┴──────────────────┘
```
## (2) RDD의미
```
RDD
- Resilient (회복력) : 파티션이 손실되면 lineage(계보) 정보로 재계산
- Distributed (분산) : 데이터가 여러 파티션으로 나뉘어 클러스터 노드들에 분산
- Dataset (데이터셋) : 읽기 전용(immutable) 컬렉션
```
- Spark의 가장 기본적인 데이터 추상화 단위.
- 불변(immutable)한 분산 객체 컬렉션 
- 각 RDD는 여러 개의 파티션으로 나뉘며, 이 파티션들은 클러스터의 서로 다른 노드에서 계산될 수 있다.
- RDD는 Python, Java, Scala의 어떤 타입의 객체라도 담을 수 있다.

### SparkContext vs SparkSession
**2010년 초기 Spark**
- SparkContext만 존재
- RDD만 사용
- DataFrame 없음

**2014년 이후**
- SparkSession 추가 (DataFrame용)
- SparkContext는 SparkSession 내부에 포함
- 하위 호환성 유지
**RDD 사용 방법1:**
```
from pyspark import SparkContext, SparkConf

# SparkContext 직접 생성
conf = SparkConf().setAppName("RDD Only").setMaster("local[*]")
sc = SparkContext(conf=conf)

# RDD 작업
rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.map(lambda x: x * 2).collect()
print(result)

sc.stop()
```
주의
- SparkContext는 프로세스당 1개만 가능  
- 중복 생성하면 에러
<br>

**RDD 사용 방법2:**
```
from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder \
    .master("local[*]") \
    .getOrCreate()

# SparkContext 추출
sc = spark.sparkContext

# RDD만 사용
rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.map(lambda x: x * 2).collect()
print(result)

spark.stop()
```
## (3) RDD한계
**RDD Lazy Evaluation**
```
- 최적화 아님
- 실행 지연으로 불필요한 계산 회피
- 조기 종료로 데이터 덜 읽기
- Lineage로 fault tolerance
```
**한계**
```
- 블랙박스 함수라 내부 모름
- 연산 순서 그대로 실행
- 사용자가 직접 최적화 필요
```
<br>

**DataFrame의 차이**
```
Lazy + Catalyst 최적화
쿼리 분석해서 자동 재배치
10~100배 빠름
```
## (4) RDD특징
- RDD : Spark 1.0부터 스파크에 도입된 가장 기초적인 데이터구조
- RDD의 세 가지 핵심 특징
    - 의존성(dependency) : 어떤 입력이 필요하고 생성되는 RDD가 어떻게 만들어지는지에 대한 정보
    - 파티션(partition)(지역성 정보 포함) : 작업을 나누어 이그제큐더들에 분산해 파티션별로 병렬 연산할 수 있는 능력 부여. 만약 파일을 읽는 경우 각 이그제큐터가 가까이 있는 데이터를 처리할 수 있는 이그제큐터에게 우선적으로 작업을 보냄.
    - 연산 함수(compute function) : 저장된 데이터를 Iterator[T] 형태로 만들어 줌.

## (5) RDD와 데이터프레임 실행
- RDD 파이썬예제(저수준)
```
sc = spark.sparkContext
# (name, age) 형태의 튜플로 된 RDD 생성
dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])
# 집계와 평균을 위한 람다 표현식, map, reduceByKey transformation
ageRDD = (dataRDD
          .map(lambda x: (x[0], (x[1], 1)))
          .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
          .map(lambda x: (x[0], x[1][0] / x[1][1]))
          )
print(ageRDD.collect())
```
<br>

- DataFrame 예제(고수준)
```
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)], ['name', 'age'])
avg_df = data_df.groupBy('name').agg(avg('age'))
avg_df.show()
```
