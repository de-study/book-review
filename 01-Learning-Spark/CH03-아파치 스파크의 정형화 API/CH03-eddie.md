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
## (2) 의미
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
