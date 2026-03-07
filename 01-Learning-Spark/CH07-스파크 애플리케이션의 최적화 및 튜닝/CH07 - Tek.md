# CH.07 스파크 애플리케이션의 최적화 및 튜닝
**CH 07 내용**
- 최적화와 관련된 설정 내용들에 대해 논의
- 스파크의 `JOIN` 전략
- 스파크 UI 내용 분석 및 나쁜 상태에 대한 단서 찾는 방법
## 효율적으로 스파크를 최적화 및 튜닝하기
### 아파치 스파크 설정 확인 및 세팅
1. 설정 파일들을 통한 방법
	- Spark properties: `conf/spark-defaults.conf`을 수정해서 기본값 제공
	- Environment Variables: `conf/spark-env.sh` 환경 변수 통해서 시스템별 설정
	- Logging: `conf/log4j.properties`
2. 스파크 애플리케이션 안에서 혹은 명령 행에서 애플리케이션을 `spark-submit`으로 제출할 때 `--conf` 옵션 사용해서 직접 설정 
	```
	./bin/spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=false
	  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" myApp.jar
	```
3. 스파크 셸에서 프로그래밍 인터페이스를 통한 방법
	- API는 스파크와 상호 작용 위한 가장 근본적인 방법
	- `SparkSession` 객체를 사용하면 대부분의 스파크 설정에 접근 가능
4. 스파크 UI의 Environmetn 탭 통해 현재 설정 확인 가능 (변경은 불가능

**스파크 설정값 결정하는 방법 들 중, 어떤 값이 우선하는지 결정하는 우선순위 존재**
> 순서에 따라 중복된 설정은 초기화된다
1. `spark-default.conf`에 정의된 값이나 플래그
2. `spark-submit`의 명령 행 설정
3. 스파크 애플리케이션에서 `SparkSession` 통해 설정된 값

### 대규모 워크로드를 위한 스파크 규모 확장
- 자원 부족이나 점진적 성능 저하에 의한 작업 실패를 피하기 위해 사용해볼 수 있는 스파크 설정<br> → 이 설정들은 Spark Driver, Executor, Executor에서 실행되는 Shuffle 서비스 등 세 가지 스파크 컴포넌트에 영향을 미친다
	- Spark Driver: 클러스터 매니저와 함께 클러스터에 Executor들을 의우고 그 위에서 돌아갈 Spark Task들을 스케줄링하는 역할
#### 정적/동적 자원 할당
**`spark-submit`에 명령 행 인자로 자원량 지정 → 제한을 두는 것**

**동적 자원 할당 설정**: 
>Spark Driver는 거대한 워크로드가 밀려오거나 빠져나갈 때마다 요청에 맞춰 
>컴퓨팅 자원 더 할당하거나 줄이도록 요청 가능
- e.g. 스트리밍: 보통 데이터양의 흐름이 불규칙
- e.g. 온디맨드 디에터 분석: 피크 타임에는 SQL 쿼리가 대용량 처리하는 경우가 많다
- 동적 자원 할당을 사용하면 스파크가 자원 사용을 더 효율적으로 하게 한다
	- 사용하지 않는 executor 해제하고 필요할 때 새롭게 띄우도록 한다
- 스파크의 리소스 요구량 변화가 동시에 자원을 필요로 하는 다른 애플리케이션에도 영향 미칠 수 있다
#### 스파크 이그제큐터의 메모리와 셔플 서비스 설정
**Executor들이 메모리 부족에 시달리거나 JVM Garbage Collection으로 문제 겪지 않게 하려면, Executor 메모리가 어떻게 구성되고 스파크가 어떻게 사용되는지 알아야 한다**

![스파크 메모리](https://1ambda.blog/wp-content/uploads/2021/12/image-132.png)

![스파크 이그제큐터](https://miro.medium.com/v2/resize:fit:720/format:webp/1*sM-zKpc2aUGyo4O5lyRfLg.png)
- Executor에서 사용 가능한 메모리의 양: `spark.executor.memory`에 의해 제어

**예비 메모리 (Reserved Memory)**
- 스파크 내부 시스템 동작을 위해 미리 확보해둔 공간 (300MB 고정값)
- OOM 에러 예방 목적

**유저 메모리 (User Memory)**
- `(1 - spark.memory.fraction)` 비율만큼 할당. (이미지에서는 전체의 25%)
	- `spark.memory.fraction` 기본 값은 보통 **0.6**
-  스파크의 관리 영역(Unified Memory) 밖에서 사용되는 공간. 
	- 사용자가 코드에서 직접 선언한 변수, Map, 리스트 같은 **객체 데이터**나 **RDD 메타데이터**, **UDF(사용자 정의 함수)** 실행 시 필요한 메모리가 여기서 소비

**스파크 메모리 (Spark Memory)**
- 실질적인 데이터 처리가 일어나는 핵심 영역
- 저장 메모리 (Storage Memory)
	- `.cache()` 또는 `.persist()`를 통해 데이터를 메모리에 올릴 때 사용
- 실행 메모리 (Execution Memory)
	- 조인(Join), 정렬(Sort), 집계(Aggregation) 등 실제 연산을 수행하는 동안 임시로 데이터를 담아두는 공간

맵이나 셔플 작업이 이루어지는 동안, 스파크는 로컬 디스크의 셔플 파일에 데이터 쓰고 읽는다 <br> **→ 큰 I/O 작업 발생**
- 기본 설정 값은 대용량 스파크 작업에 최적화되어 있지 않기 떄문에 병목 현상 발생 가능

#### 스파크 병렬성 최대화
스파크 유용성의 많은 부분들은 여러 태스크 동시에 대규모로 실행시킬 수 있는 능력에서 기인

**파티션**
- 데이터를 관리 가능하고 쉽게 읽어 들일 수 있도록 디스크에 연속된 위치에 조각이나 블록들의 모음으로 나눠 저장하는 방법
	- 이 데이터 모음들은 병렬적 + 독립적으로 읽어서 처리 가능
	- 필요하다면 하나의 프로세스 안에서 멀티 스레딩으로 처리 가능
- **독립적으로 처리 가능한 점이 대규모 병렬 처리 데이터 가능하게 하는 중요한 점**

> 대용량 워크로드 위한 Spark Job은 여러 Stage 거치고, 각 Stage에서 많은 태스크 처리
→ 스파크는 최대한 각 코어에 Task 할당 + 각 Task에 또 스레드 스케줄링 + 각 Task는 개별 파티션 처리
→ 자원 사용 최적화하고 병렬성 최대로 끌어올리려면 가장 이상적인 것은 Executor에 할당된 코어 개수 만큼 파티션들이 최소한으로 할당되는 것
→ 파티션이 가장 기본적인 병렬성의 한 단위, 하나의 코어에서 돌아가는 하나의 쓰레드는 하나의 파티션을 처리할 수 있다
→ 즉, **1 Core = 1 Task = 1 Partition**

- Executor의 Core 당 Task 1개 , 1개의 Task에서 1개의 Partition을 처리하므로 Core 수 = Task 수 = Partition 수를 처리한다. 
	- 이때 Partition 수에 따라 각 Partition의 크기가 결정되는데, Partition의 수 = Core 수 라면, Partition의 크기 = Memory 크기
	- 즉, Partition의 크기가 Core당 필요한 메모리의 크기를 나타낸다
![스파크 태스크, 코어, 파티션, 병렬성 간의 관계](https://velog.velcdn.com/images/gorany/post/26f74a2e-1a50-4d57-84ae-c02212327eee/image.png)

#### 파티션은 어떻게 만들어지는가

**스파크의 Task들은 데이터를 디스크에서 읽어 메모리로 올리면서 Partition 단위로 처리**
- 디스크의 데이터는 저장 장치에 따라 조각이나 연속된 파일 블록으로 존재
- 기본적으로 데이터 저장소의 파일 블록은 64~128MB

**스파크에서 한 Partition의 크기는 `spark.sql.files.maxPartitionBytes`에 따라 결정**
- 기본값 128MB, 이 크기를 줄이게 되면 **작은 파일 문제** 발생 가능
- 작은 Partition 파일 많아진다 → 디스크 I/O 급증 + 디렉토리 여닫기, 목록 조회 등 파일 시스템 작업으로 인해 성능 저하 → **분산 파일 시스템 느려질 수 있다**

**셔플 단계에서 만들어지는 셔플 파티션 (Shuffle Partition)**
- `spark.sql.shuffle.partitions` 기본값 200
	- `spark.sql.shuffle.partitions` 기본값 200은 작은 규모나 스트리밍 워크로드에는 너무 높을 수 있다
- 데이터 사이즈 크기에 따라 이 숫자를 조정해서 너무 작은 Partition들이 Executor에 할당되지 않게 할 수 있다
- `groupBy()`, `join()` 처럼 넓은 트랜스포메이션으로 알려진 작업 중 생성되는 셔플 파티션은 네트워크, 디스크 I/O 모두 사용 <br> → 셔플 중 `spark.local.directory`에 지정된 executor의 로컬 디렉토리에 중간 결과 쓰게 된다

## 데이터 캐싱과 영속화
**스파크에서는 캐싱과 영속화 (persistence) 동일**
- `cache()`, `persist()` 사용할 때 데이터프레임은 모든 레코드를 접근하는 액션 (e.g. `count()`) 호출하기 전에는 완전히 캐시되지 안흔ㄴ다
- 차이점
	- 영속화는 데이터가 저장되는 위치와 방식에 대해 좀 더 세밀한 설정 가능 (메모리/디스크 여부, 직렬화 여부)
### `DataFrame.cache()`
- 데이터 프레임은 그 중 일부만 캐시될 수 있지만 그 파티션들은 개별 파티션의 일부만 저장될 수 없다. (e.g, 4.5파티션 정도만 들어갈 메모리만 있다면 4개의 파티션이 캐시)
- 하지만 모든 파티션이 캐시된 것이 아니라면 데이터에 접근을 다시 시도할 때 캐시되지 않은 파티션은 재계산되어야 하고 이는 스파크 job을 느리게 만들 것
### `DataFrame.persist()`
- 디스크의 데이터는 자바든 크리오든 직렬화 항상 사용
- `persist(StorageLevel.LEVEL)`의 함수 호출 방식은 `StorageLevel`을 통해 데이터가 어떤 방식으로 캐시될 것인지 제어
	- 각 `StorageLevel`은 동일한 기능 하는 `레벨_이름_2` 형태의 옵션 존재
		- 이는 서로 다른 스파크 executor에 복제해서 두 벌씩 저장되는 것을 의미
		- 캐싱에 자원을 더 소모하지만 데이터를 두 군데 저장 → 장애 발생 시 다른 카피본에 대해 태스크 스케줄링 가능
```java
import org.apache.spark.storage.StorageLevel

// 천만 개의 레코드를 가지는 데이터 프레임 생성
val df = spark.range(1*10000000).toDF(“id”).withColumn(“square”, $”id” * $”id”)
df.persist(StorageLevel.DISK_ONLY) // 데이터를 직렬화해서 디스크에 저장
df.count() // 캐시 수행

res2: Long = 10000000
Command took 2.08 seconds

df.count() // 이제 캐시를 사용

res3: Long = 10000000
Command took 0.38 seconds

// 데이터는 메모리가 아닌 디스크에 저장
// 캐시를 비우고 싶다면, DataFrame.unpersist()를 호출
```
### 캐시나 영속화 사용 / 사용하면 안 되는 경우
**(사용) 큰 데이터세트에 쿼리나 트랜스포메이션으로 반복적으로 접근해야 하는 시나리오**
- 반복적인 머신러닝 학습을 위해 계속 접근해야 하는 데이터프레임들
- ETL(Extract, Transform, Load)이나 데이터 파이프라인 구축 시 빈도 높은 트랜스포메이션 연산으로 자주 접근해야 하는 데이터 프레임들

**모든 상황에서 캐시가 효과를 발휘하지는 않는다**
- 데이터 프레임이 메모리에 들어가기엔 **너무 큰 경우**
- **자주 쓰지 않는** 데이터프레임에 대해 비용이 크지 않은 트랜스포메이션을 수행할 때
- 메모리 캐시는 사용하는 `StorageLevel`에 따라 직렬화/역직렬화에서 비용을 발생시킬 수 있기 때문에 주의 깊게 사용해야 함

## 스파크 조인의 종류
**조인 연산들은 스파크 Executor들 사이에 방대한 데이터 이동 발생**

**셔플 (Shuffle)**
- 서로 다른 노드에 흩어져 있는 데이터를 조인 키(Join Key)를 기준으로 **같은 노드로 모으는 재배치 과정**을 의미
- 조인 키를 기준으로 데이터를 네트워크를 통해 이동시켜 다시 그룹화하는 과정

**셔플이 발생하는 이유**
- 조인하려는 두 테이블의 데이터가 조인 키에 따라 정렬되어 있거나 동일하게 파티셔닝되어 있지 않기 때문

**셔플은 리소스가 많이 드는 작업이다**
1. **네트워크 I/O:** 대량의 데이터가 노드 간에 이동하므로 대역폭을 많이 소모
2. **디스크 I/O:** 셔플 중간 데이터를 로컬 디스크에 쓰고 읽는 과정에서 지연 발생
3. **직렬화/역직렬화:** 데이터를 네트워크로 보내기 위해 객체를 바이트 형태로 바꾸고(Serialization) 다시 복원하는 과정에서 CPU를 많이 사용
4. **데이터 스큐(Data Skew):** 특정 키에 데이터가 몰려 있으면 특정 익제큐터만 과부하가 걸려 전체 Job 느려진다

### 셔플을 피하는 최적화 전략
#### 브로드캐스트 조인 (Broadcast Join)
한쪽 테이블이 메모리에 올릴 수 있을 만큼 충분히 작다면, 셔플을 하지 않고 작은 테이블을 모든 익제큐터에 복제해서 보낸다
- **장점:** 대규모 데이터 이동(Shuffle)이 없어 성능이 매우 빠르다
- **조건:** `spark.sql.autoBroadcastJoinThreshold` 설정값 이하의 크기여야 한다
#### 버켓팅 (Bucketing)
데이터 저장할 때 미리 Join 키를 기준으로 파티셔닝하여 저장한다
- **장점:** 이미 키별로 나누어져 저장되어 있기 때문에 조인 시점에서 셔플 건너뛸 수 있다

**스파크는 이그제큐터 간 데이터를 교환, 이동, 정렬, 그룹화, 병합하는 다섯 종류의 조인 전략 가진다**
- 브로드캐스트 해시 조인 (교재)
- 셔플 해시 조인
- 셔플 소트 머지 조인 (교재)
- 브로드캐스트 네스티드 루프 조인
- 셔플 복제 네스티드 루프 조인 (=카테시안 프로덕트 조인)

### 브로드캐스트 해시 조인 (=맵사이드 조인)
**이상적으로는 데이터 이동이 거의 필요 없도록 한 쪽은 작고 다른 쪽은 큰 두 종류의 데이터 사용하여 특정 조건이나 칼럼 기준으로 조인한다**
- 스파크 브로드캐스트 변수 사용해서 더 작은 쪽의 데이터가 드라이버에 의해 모든 스파크 executor로 뿌려진다<br>→ 이어서 각 executor에 나뉘어 있는 큰 데이터와 조인이 된다
- 이 전략은 큰 데이터 교환이 일어나지 않게 한다
- 스파크는 기본적으로 작은 쪽 데이터가 10MB 이하일 때 브로드캐스트 조인 사용 (`spark.sql.autoBroadcastJoinThreshold`에 설정 지정)
![브로드캐스트 조인](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2Fdh9RCK%2Fbtss9ASFMqI%2FAAAAAAAAAAAAAAAAAAAAALp1w1jo80vGjMaF_ZVMszDWKKK1PSQRkBPDTA_gVKt8%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3DpbZCI6EZNEPKJIbEq7d9QP0Znrw%253D)
- 일반적인 사용 예제: 두 데이터 프레임에 공통적인 키 존재, 한 쪽이 갖고 있는 정보가 적은데 양쪽의 뷰 병합하는 경우
	- e.g. 전 세계 축구 선수 정보 있는 큰 데이터 세트인 `playersDF` & 축구 팀 정보가 있는 작은 데이터 세트인 `clubsDF` 를 공통 키에 대해 조인하는 경우

**BHJ은 어떤 셔플도 일어나지 않는다 → 스파크가 제공하는 가장 쉽고 빠른 조인 형태**

#### 어떤 경우에 BHJ 사용하는가
- 양쪽 데이터세트의 각 키가 스파크에서 동일한 파티션 안에 해시될때
- 한 데이터가 다른 쪽보다 많이 작은 경우 (그리고 10MB 기본 설정 또는 충분한 메모리 있을 경우 그 안에 들어갈 만한 데이터 사이즈일 때)
- 정렬되지 않은 키들 기준으로 두 데이터 결합함녀서 동등 조인 (equi-join) 수행할 때
- 더 작은 쪽의 데이터가 모든 스파크 Executor에 브로드캐스트될 때 발생하는 과도한 네트워크 대역폭이나 OOM 오류에 대해 걱정할 필요 없는 경우

### 셔플 소트 머지 조인
**정렬 가능하고 겹치지 않으면서 공통 파티션에 저장 가능한 공통 키를 기반으로, 큰 두 종류의 데이터세트 합칠 수 있는 효과적인 방법**
- 해시 가능한 공통 키를 가지면서, 공통 파티션에 존재하는 두 가지의 데이터세트 사용
- 스파크의 관점에서, 이는 **각 데이터세트의 동일 키 가진 데이터세트의 모든 레코드가 동일 이그제큐터의 동일 파티션에 존재한다는 것을 의미**

#### 1. "동일 키 = 동일 executor의 동일 파티션"의 의미
스파크가 두 개의 커다란 데이터세트(A, B)를 조인할 때, 가장 먼저 해결해야 할 숙제는 **"서로 짝이 맞는 레코드들을 어떻게 한자리에 모을 것인가?"**
##### 물리적 배치의 원리 (Shuffle)
1. **해시 함수 사용:** 
	- 스파크는 조인 키(Join Key)에 해시 함수 적용. 
	- Hash(Key)(mod Partition Count) 연산을 통해 해당 레코드가 이동할 목적지 파티션 번호를 결정
2. **데이터 이동:** 
	- 이 계산 결과에 따라 데이터세트 A의 'ID: 10'인 데이터와 데이터세트 B의 'ID: 10'인 데이터는 네트워크를 타고 **동일한 번호의 파티션**으로 모이게 된다
3. **Executor 상주:** 
	- 특정 번호의 파티션은 특정 **이그제큐터(Executor)** 내의 스레드에 할당
	- 결과적으로 **"동일 키를 가진 모든 레코드는 물리적으로 같은 컴퓨터(Executor)의 같은 메모리 공간(파티션)에 존재하게 된다"**는 뜻
    
> **이렇게 하는 이유?** 
> 데이터가 서로 다른 컴퓨터에 있으면 네트워크를 계속 왔다 갔다 하며 비교해야 하므로 연산이 불가능에 가깝다. 
> 조인을 시작하기 전에 **"비교 대상을 한 바구니에 담는 과정"**

#### 2. "정렬 가능하고 겹치지 않는 공통 키"의 의미
**데이터가 한 파티션에 모였다고 해서 끝이 아니고, 이제 효율적으로 짝을 찾아야 한다**
##### 소트(Sort) 단계
- 모인 데이터들을 조인 키 기준으로 **오름차순 정렬**
- **정렬 가능(Sortable)**해야 하는 이유: 
	- 정렬이 되어 있어야만 두 데이터를 위에서 아래로 한 번만 훑으면서(Linear Scan) 짝을 찾을 수 있기 때문  
##### 머지(Merge) 단계: "겹치지 않음"의 활용
- 두 데이터세트가 정렬되어 있다면, 한쪽을 읽다가 반대쪽보다 큰 값이 나오면 더 이상 이전 데이터는 볼 필요가 없다
- **비중복성(Non-overlapping)의 이점:** 
	- 특정 범위의 키를 처리하고 나면, 그 데이터는 다시 돌아볼 필요 없이 버릴 수 있다. 
	- 이를 통해 시간 복잡도를 혁신적으로 줄인다 ($O(n+m)$)
	
| **단계**         | **데이터 상태 및 위치**       | **데이터 엔지니어의 해석**                   |
| -------------- | --------------------- | ---------------------------------- |
| **1. Shuffle** | 키 기반 해싱 후 네트워크 이동     | **"동일 키를 동일 파티션으로 집결시킨다"**         |
| **2. Sort**    | 파티션 내에서 키 순서대로 정렬     | **"비교 효율을 위해 데이터를 줄 세운다"**         |
| **3. Merge**   | 두 리스트를 위에서 아래로 읽으며 매칭 | **"한 번의 스캔($O(n+m)$)으로 조인을 완료한다"** |

#### 셔플 소트 머지 조인 최적화
**버케팅(Bucketing)을 사용하지 않는 일반적인 상황에서 셔플 소트 머지 조인(Sort-Merge Join)은 반드시 Exchange(Shuffle)와 Sort 작업을 머지 이전에 수행**
- Exchange(Shuffle)은 비싼 작업
	- Executor들 간에 네트워크 상으로 파티션들이 셔플되어야 한다
##### 버케팅
- Exchange와 Sort 작업을 미리 해두는 기술
- **버케팅의 핵심은 "나중에 할 고생(Shuffle)을 미리 매 맞듯 저장 시점에 끝내버리는 것"**
	- 버케팅을 하면 데이터를 읽어오는 단계에서 이미 특정 키들이 특정 파일(Bucket)에 정렬되어 모여 있다. 
	- 따라서 드라이버는 "이미 데이터가 준비되었네?"라고 판단하고 **Exchange와 Sort 노드를 실행 계획에서 삭제**한다
###### 버케팅 장점
- **셔플 제거 (Shuffle Elimination):**
    - 데이터가 이미 조인 키를 기준으로 물리적인 파일(Bucket)에 나뉘어 저장
    - 따라서 조인 시점에 네트워크를 통해 데이터를 주고받는 **Exchange(Shuffle)** 과정을 완전히 생략할 수 있어 네트워크 I/O 비용이 0에 수렴
- **정렬 비용 절감 (Sort Avoidance):**
    - 버케팅 시 `sortBy` 옵션을 함께 사용하면 데이터가 각 버켓 내에서 이미 정렬된 상태로 저장
    - 셔플 소트 머지 조인에서 가장 시간이 많이 걸리는 **Sort** 단계를 건너뛰고 즉시 **Merge**로 진입
- **쿼리 성능의 예측 가능성 향상:**
    - 셔플은 네트워크 상태나 클러스터 부하에 따라 성능 널뛰기가 심하다. 
    - 버케팅된 데이터는 물리적 위치가 고정되어 있어 쿼리 실행 시간이 매우 일정하게 유지.
- **데이터 스큐(Data Skew) 완화:**
    - 저장 시점에 해시 함수를 통해 데이터를 균등하게 배분하므로, 특정 파티션에 데이터가 몰리는 현상을 사전에 방지하거나 관리 가능
###### 버케팅 단점
- **쓰기 작업의 오버헤드 (Write-side Penalty):**
    - 데이터를 저장할 때마다 셔플과 정렬을 수행해야 하므로, 단순 저장보다 **쓰기 시간이 훨씬 오래 걸린다.**
    - 데이터가 빈번하게 업데이트되거나 매번 새롭게 생성되는 환경에서는 배보다 배꼽이 더 클 수 있다.
- **스몰 파일 문제 (Small File Problem):**
    - `버켓 수 * 파티션 수`만큼의 파일 생성. 
    - 버켓 수를 너무 크게 잡으면 개별 파일 크기가 너무 작아져서, 읽기 성능이 오히려 떨어지고 HDFS/S3의 메타데이터 부하가 커진다
- **엄격한 조건 (Rigidity):**
    - **조인 키 일치:** 버케팅된 키와 실제 조인에 사용하는 키가 정확히 일치해야만 최적화가 작동
    - **버켓 개수 일치:** 
	    - 조인하려는 두 테이블의 **버켓 개수**가 같거나 배수 관계여야 셔플 없이 조인이 가능. 
	    - 하나라도 다르면 스파크는 다시 전체 셔플을 수행
- **메타데이터 관리 필요:**
    - 버케팅 정보는 하이브 메타스토어(Hive Metastore) 등에 저장되어야 스파크가 인지 가능
    - 단순한 파일 경로로만 읽으면 버케팅 효과를 볼 수 없다\

```python
from pyspark.sql.functions import *

# parquet 포맷으로 버케팅 bucketBy()해서 스파크 관리 테이블로 저장
usersDF.orderBy(asc("uid")) \
        .write.format("parquet") \
        .bucketBy(8, "uid") \ 
        .mode("overwrite") \
        .saveAsTable("UsersTbl")
ordersDF.orderBy(asc("users_id")) \
        .write.format("parquet") \
        .bucketBy(8, "users_id") \
        .mode("overwrite") \
        .saveAsTable("OrdersTbl")

# 테이블 캐싱
spark.sql("CACHE TABLE UsersTbl")
spark.sql("CACHE TABLE OrdersTbl")

# 다시 읽는다
usersBucketDF = spark.table("UsersTbl")
ordersBucketDF = spark.table("OrdersTbl")

# 조인하고 결과 보여준다
joinUsersOrdersBucketDF = ordersBucketDF .join(usersBucketDF, ordersBucketDF.users_id == usersBucketDF.uid)

joinUsersOrdersBucketDF.show()
```

![버케팅](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FbOG4F1%2FbtssTrKgKJq%2FAAAAAAAAAAAAAAAAAAAAAEmgilUFiJ9j_wOA4uTxCkd7K7Azis_aO1FytvjpbZr6%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3DrKFJKvl8u4koJJWGJkWr3HDujKk%253D)
- 공통의 정렬된 키(컬럼)에 따라 파티셔닝 된 bucket을 사용하여 조인했기 때문에 조인 결과도 정렬되어 있다. 
	- 그러므로 조인 과정에서 정렬할 필요가 없다. 
- Exchange가 수행되지 않고 바로 `WholeStageCodegen`(Spark SQL의 물리적 쿼리 최적화 단계)로 넘어가는 것을 확인할 수 있다.
#### 셔플 소트 머지 조인 사용해야 하는 경우
- 두개의 큰 데이터세트의 각 key가 정렬 및 해싱되어 스파크 내 동일한 partition에 있을 때
- 동일한 정렬된 키로 두 데이터세트를 결합하는 동일 조건 조인을 수행하는 경우
- 네트워트 간에 규모가 큰 셔플을 일으키는 exchange 연산과 sort 연산을 안하고 싶을 때

## 스파크 UI 들여다보기
>스파크 UI는 메모리 사용량, 잡, 스테이지, 태스크 등에 대한 자세한 정보 뿐만 아니라 이벤트 타임라인, 로그, 스파크 애플리케이션 안에서 무슨 일이 벌어지는지 알 수 있는 다양한 통계 정보를 스파크 Driver와 각 개별 Executor 레벨에서 제공한다

**`spark-submit`은 스파크 UI 띄우게 되며 로컬 호스트 (로컬 모드)나 스파크 드라이버 (그 외 모드)를 통해 기본 포트 4040으로 연결 가능**

### 스파크 UI 탭 둘러보기
#### Jobs와 Stage 탭
- 개별 태스크의 완료 상태, I/O 관련 수치, 메모리 소비량, 실행 시간 등을 살펴볼 수 있다
##### Jobs 탭
- Jobs 탭은 Executor들이 어떤 시점에 클러스터에 추가되거나 삭제되었는지 보여준다
- 표 형태로 완료된 Job 목록도 보여준다
- **이벤트 타임라인** 에서는 시간 순으로 Executors와 Jobs의 상태를 그래픽으로 보여준다. - 
	- (2)의 상단에는 한 개의 상자가 하나의 Executor, 하단에서는 하나의 상자가 하나의 Job에 해당된다. 
![스파크 잡스 탭](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FbL97Rt%2Fbtq8Ch7XhbC%2FAAAAAAAAAAAAAAAAAAAAAAFa0EUjR4WgMUWBSOEZlkpE3YCnKaQytJfdWywVeWDQ%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3DrTfRa3Y3d2zXYh%252F77EGLZK%252BFAcc%253D)
##### Stage 탭
- 애플리케이션의 모든 잡의 모든 스테이지의 현재 상태에 대한 요약 제공
- 추가적인 메트릭 말고도 각 태스크의 평균 수행 시간, Garbage Collection 걸린 시간, 셔플 읽기로 읽어 들인 바이트 크기나 레코드 개수 등도 볼 수 있다
![스테이지 탭 1](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2Fb0PoBf%2Fbtq8H4GKNnn%2FAAAAAAAAAAAAAAAAAAAAAK05tY1SCHHSbZcdAa4bn9dtPqEJNCJjbYSg1Y_FDouY%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3DvslrzwqRqEC7sCUW9jibPEXu8%252BM%253D)
![스테이지 탭 2](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FyTUqj%2Fbtq8HUEnniv%2FAAAAAAAAAAAAAAAAAAAAAAdgh4CnPHvT_5I4m0c8rk-VJ9uJmFVr8iKclYfECLjq%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3DmWRT%252FZrJYuOJRb5y3N8Tu7Gj2tA%253D)
- 최소 단위인 Task들의 실행 이벤트 타임라인과 다양한 개별 또는 집계 메트릭을 확인 가능
	- Job, Stage 레벨에서 특정 현상에 대한 전체적인 흐름을 조망했다면, 아래와 같은 개별 Task를 탐색하면서는 일부 이상치 또는 현상에 대해 더욱 세밀한 분석을 진행 가능
#### Executors 탭
- 애플리케이션에서 생성된 Executor들에 대한 정보 제공
	- shuffle의 IO와 함께 Executor 메모리가 주요 요소로 많이 다뤄지기에, 그러한 상황에서도 위와 같은 수치가 어떠한 부분을 나타내어 주는지 알고 있는 것은 매우 중요
![이그제큐터 탭](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FkOGgW%2Fbtq8G5sUYMy%2FAAAAAAAAAAAAAAAAAAAAAFwXvsb0EbDltotl9rlmkoxu4YbV1fpW8DD48IoEHSOi%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3DJOml1KhMK8HI3v5ppV9vA5Alszo%253D)
#### Storage 탭
- 애플리케이션에 캐시된 데이터 프레임이나 테이블의 정보 제공
	- Storage 탭에서 `cache()`나 `persist()`의 결과로 애플리케이션에 캐시된 데이터프레임이나 테이블의 정보 제공
- 메모리 사용량의 세부 사항을 보여준다
![storage 탭](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FbD73V0%2Fbtss8kbChT9%2FAAAAAAAAAAAAAAAAAAAAAGCHpJobkknUAX5ObQSGEMWOJW5KHGfy28s8Oq7MMG3p%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3Du64tNjyebEdq6u0ouRBqoln7PH4%253D)

#### SQL 탭
- 스파크 애플리케이션의 일부로 실행된 스파크 SQL 쿼리의 효과 확인
- 쿼리가 언제 어떤 Job에 의해 실행 되었고 얼마나 걸렸는지 알 수 있다
![sql 탭](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2Frk8A1%2Fbtss8jcJ0OV%2FAAAAAAAAAAAAAAAAAAAAAETe0qL_GHQjp3i6RJz_lIHPN06l8NWkbxGsvoQLJ_4a%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3Dd655hnzb24r01i93lQjRukAQU%252Bk%253D)
- Description 클릭하면 SQL 쿼리에 대한 상세 통계정보 보여주는 스파크UI ; 모든 물리적 작업들과 상세 실행 계획을 보여준다
![sql description](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FQcAaR%2FbtssVUSxtBx%2FAAAAAAAAAAAAAAAAAAAAAFk1gBQYm66Fqfc9ZHCmHDu7VvuD4cl4GEoAfmU1fzaX%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3DPJxrytHlgq2DgLjNQ7LQVf1j0gs%253D)
#### Environment 탭
- 스파크가 돌아가고 있는 환경에 대해 알려준다(트러블 슈팅에 유용한 많은 단서 제공)
	- e.g. 환경 변수, jar 파일, 스파크 특성, 시스템 특성, 런타임 환경(JVM/Java Version)
![environment](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2Fr9pm2%2Fbtss3HLLdYk%2FAAAAAAAAAAAAAAAAAAAAANDs-Tgp5KLb4x10X1LYIFWK7ITlfDEv-R9o1YwM6rTg%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3DXLtgTvqnPrF470BSLwY4vbwrZtI%253D)

## References 

```cardlink
url: https://nowolver.tistory.com/135
title: "[PySPARK] 스파크 애플리케이션의 최적화 및 튜닝"
description: "효율적으로 스파크를 최적화 및 튜닝하기 스파크는 튜닝을 위한 많은 설정이 있지만 중요하고 자주 사용되는 것만 다뤘다. 아파치 스파크 설정 확인 및 세팅 스파크 설정을 확인하고 설정하는 방법은 세가지가 있다. 설정 파일을 통한 방법 배포한 $SPARK_HOME 디렉터리안에 conf/spark-defaults.conf.template, conf/log4j.properties.template, conf/spark-env.sh.template 이 파일들 에 있는 기본값을 변경하고 .template 부분을 지우고 저장한다. (conf/spark-defaults.conf 설정을 바꾸면 클러스터와 모든 애플리케이션에 적용 스파크 애플리케이션 안에서 혹은 명령 행에서 —conf 옵션을 사용 2-1) 명령 행에서 —co.."
host: nowolver.tistory.com
favicon: https://t1.daumcdn.net/tistory_admin/favicon/tistory_favicon_32x32.ico
image: https://img1.daumcdn.net/thumb/R800x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FsScHl%2FbtssT9Jkl9Z%2FAAAAAAAAAAAAAAAAAAAAAEHJIxmvrY1oqCqYYEydyKObE7KKleK17QFGpU-1qLx0%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3DCMlau8udpBiC90sfYhFEMTjAKg8%253D
```

```cardlink
url: https://kadensungbincho.tistory.com/93
title: "Apache Spark(아파치 스파크) Web UI 관찰하기"
description: "스파크 애플리케이션의 상태를 파악하는데에 있어서 Web UI는 매우 중요한 역할을 합니다. 이번 글에서는 Spark Web UI를 살펴보며, 어떤 부분들을 고려할 수 있는지 알아보도록 하겠습니다. 살펴볼 세부적인 사항들은 아래와 같습니다: Jobs: 스파크 애플리케이션의 모든 job에 대한 요약 정보 제공 Stages: 모든 jobs의 모든 stages의 현재 상태 요약 정보 제공 Storage: persisted RDD와 DataFrames 정보 제공 Environment: 다양한 환경 변수값 Executors: 애플리케이션을 위해 생성된 엑서큐터 정보 제공. 메모리와 디스크 사용량과 task, shuffle 정보 등 SQL: 애플리케이션이 Spark SQL 쿼리 실행 시 정보 제공 Streaming:.."
host: kadensungbincho.tistory.com
favicon: https://t1.daumcdn.net/tistory_admin/favicon/tistory_favicon_32x32.ico
image: https://img1.daumcdn.net/thumb/R800x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FbL97Rt%2Fbtq8Ch7XhbC%2FAAAAAAAAAAAAAAAAAAAAAAFa0EUjR4WgMUWBSOEZlkpE3YCnKaQytJfdWywVeWDQ%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1774969199%26allow_ip%3D%26allow_referer%3D%26signature%3DrTfRa3Y3d2zXYh%252F77EGLZK%252BFAcc%253D
```

```cardlink
url: https://tech.kakao.com/posts/461
title: "Spark Shuffle Partition과 최적화 - tech.kakao.com"
description: "안녕하세요. 카카오 데이터PE셀(응용분석팀)의 Logan입니다.응용분석팀에서 식..."
host: tech.kakao.com
favicon: https://www.kakaocorp.com/page/favicon.ico
image: https://img1.kakaocdn.net/thumb/U896x0/?fname=https%3A%2F%2Ft1.kakaocdn.net%2Fkakao_tech%2Fimage%2F2021%2F10%2Fimages%2Fmain-12.png
```
