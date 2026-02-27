
# 01.데이터 세트 개념


## (1) 배경 및 등장 이유

**데이터 세트 - Spark의 타입 안전(type-safe) 분산 데이터 컬렉션 객체이자 API**
```
- RDD: 타입 안전하지만 Catalyst 옵티마이저 혜택 없음, 느림
- DataFrame: Catalyst 옵티마이저로 빠르지만 컴파일 타임 타입 체크 없음 (런타임 에러)
```

**핵심 특징**
- 컴파일 타임에 타입 에러 잡을 수 있음
- Catalyst 옵티마이저 + Tungsten 엔진 혜택 받음
- JVM 언어(Scala, Java)에서만 완전히 지원됨

Python과 R은 동적 타입 언어라 Dataset API를 지원하지 않는다. 
Pyspark에는 데이터세트 없음
### 내부적 설계
Spark 내부적으로는 `DataFrame = Dataset[Row]` 로 정의되어 있다. 
DataFrame은 Dataset의 특수한 형태다.

- Dataset이 상위 개념, DataFrame은 그 특수 케이스
- DataFrame은 `Dataset[Row]`의 **타입 별칭(type alias)**
- Row는 "타입 정보 없이 컬럼명으로 접근하는 제네릭 타입"

**DataFrame은 타입 체크 안하는 데이터세트의 한 종류**
- `Dataset[Person]` → 컴파일 타임에 타입 체크 (Scala/Java에서 주로 사용)
- `Dataset[Row]` = `DataFrame` → 런타임에 스키마로 타입 처리 (Python은 여기만 해당)
	-  `Row` :Spark가 제공하는 내장 클래스

### 필요성
- DataFrame과의 차이
	- 데이터세트 - 컴파일시 에러 발생
	- 데이터프레임 - 런타임에 에러 발생
- 컬럼명 잘못기입, 없는 컬럼 코드에 들어가 있을 때

**전체 흐름에서 위치**
```
1. 코드 작성
2. 컴파일 → 여기서 타입 에러 잡힘 (Dataset)
3. JAR 패키징
4. Spark 클러스터 제출 (spark-submit)
5. 런타임 → 여기서 에러 잡힘 (DataFrame)
6. 실제 데이터 처리

```


#### 실무에서 왜 중요하냐

Dataset (컴파일 에러):
- 코드 짜는 순간 IDE가 빨간줄 그어줌
- 배포 전에 발견

DataFrame (런타임 에러):
- 클러스터에 올려서 수억건 데이터 처리하다가 에러
- 이미 리소스 다 썼고, 시간도 날아감


## (2) 단일화된 API를 제공의 의미

### RDD는 언어별로 따로 구현하는 구조
### Dataframe은 언어별 구현 차이 없음 → 단일화된 API 


**실제 사례**
- RDD 시절 → Scala 먼저, Python 나중에 구현
	- `MLlib` 초기 버전 → Scala/Java만 지원, Python은 한참 뒤에 추가
	- `GraphX` → 아직도 Python 미지원 (Scala/Java 전용)
	- `Streaming` 초기 DStream API → Scala 먼저, Python 구현 나중에
	- 일부 RDD 고급 메서드들 → Python에서 누락되거나 동작이 달랐음


RDD는 언어별로 따로 구현하는 구조.
DataFrame은 JVM 위에서 실행되는 공통 실행 계획(Catalyst)으로 돌아감. → 단일화된 API

DataFrame 수준에서의 `groupBy()`, `filter()`, `select()` 같은 메서드들은 Scala에 추가되면 Python/Java에도 동일하게 추가된다는 말이다.


## (3) 사용법
> 스칼라, 자바 언어별 사용법이 존재

### 타입 선언
- 스칼라 : case class
- 자바 :  bean객체

#### Scala
```
// 타입 정의
case class User(name: String, age: Int)

// Dataset 생성
val ds = spark.read.json("users.json").as[User]

// 타입 안전 연산
ds.filter(u => u.age > 30)  // u가 User 타입으로 확정됨
```

#### Java
```
// 타입 정의
public class User implements Serializable {
    public String name;
    public Integer age;
}

// Dataset 생성
Dataset<User> ds = spark.read().json("users.json").as(Encoders.bean(User.class));

// 타입 안전 연산 - user.address 존재안하면 컴파일 에러
ds.filter(user -> user.age > 30);
```

### Java vs Scala 차이
- 객체 생성시, 인코터 생성 필요함
	- 인코더는 뒤에 설명함
	- Java : Dataset을 쓸 때 Spark가 Java 객체를 직렬화/역직렬화하는 방법을 명시적으로 알려줘야 함
	- Scala : case class가 있어서 Spark가 자동으로 인코더를 만들어줌

**인코더가 하는 일**
Java 객체 ↔ Spark 내부 바이너리 포맷 변환을 담당한다. Spark는 데이터를 JVM 객체가 아닌 자체 바이너리 포맷으로 처리하기 때문에 이 변환이 필요하다.



## (4) 람다 vs DSL 표현

### 개념

**핵심 차이: Catalyst 옵티마이저가 개입할 수 있냐 없냐야.**
- DSL: Spark가 만든 메서드 → Catalyst가 뭔지 안다 → 바이너리 직접 처리
- 람다/UDF: 내가 만든 코드 → Catalyst가 뭔지 모른다 → JVM 객체로 변환 필요

**DSL 방식**
- DSL = Spark가 제공하는 내장 메서드로 작성한 코드
	- Catalyst가 직접 해석할 수 있는 Spark 내장 연산이라는 뜻
- `$"age" > 30` → Spark가 이해하는 **Expression 트리**로 변환
- Catalyst 옵티마이저가 분석/최적화 가능(텅스텐,인코더 사용)→ java 직렬화/역직렬화 사용 x
- 실행 계획에서 predicate pushdown, 컬럼 pruning 등 적용

**람다 방식**
- `person => person.age > 30` → JVM 바이트코드인 **불투명한 블랙박스**
- Catalyst가 내부를 들여다볼 수 없음
- 최적화 불가, 그대로 실행
- 자바 직렬화/역직렬화 사용

**실무 영향**
- 람다: Catalyst 최적화 없음 → 대용량에서 성능 차이 발생
- DSL: Catalyst가 최적화 → Parquet 읽을 때 필요한 컬럼/파티션만 읽음
### 스칼라 예시
**람다 방식**
```scala
dataset.filter(person => person.age > 30)
```

**DSL 방식**
```scala
dataset.filter($"age" > 30)
```


### 자바 예시
**람다 방식**
```java
dataset.filter((FilterFunction<Person>) person -> person.age > 30);
```

**DSL 방식**
```java
dataset.filter(col("age").gt(30));
```

- Scala랑 구조는 같고 Java 문법 특성상 람다에 타입 명시(`FilterFunction<Person>`) 필요





# 02.스파크 메모리 관리 방법

## (1) 배경지식
### Java의 직렬화/역직렬화
**직렬화는 객체를 바이트스트림으로 변환하는 것, 역직렬화는 그 반대야.**

#### 왜 필요한가
- JVM 객체는 메모리(힙) 위에만 존재
- 네트워크 전송이나 디스크 저장 시, 바이트 형태로 변환해서 전송 필요
```
[JVM 힙 메모리]          [네트워크/디스크]
Person 객체(참조값)  →  01001101 10110010 ...
age=30, name="Eddie"     (바이트스트림)
```

#### 종류
Java 기본 직렬화:
- JVM 기본 제공
- 느리고 용량 큼
- 클래스 메타데이터 전부 포함

Kryo 직렬화:
- 서드파티 라이브러리
- Java 직렬화보다 10배 빠름
- 용량도 작음
- 단 클래스를 미리 등록해야 함
- RDD에서 주로 사용

#### Spark에서 언제 발생하냐
```
Worker Node 1                                  Worker Node 2
[Person 객체]  →  직렬화  →  네트워크  →  역직렬화  →  [Person 객체]
```

- 셔플(shuffle) 발생 시 노드 간 데이터 이동할 때
- 드라이버 ↔ 익스큐터 간 데이터 주고받을 때

### Java의 문제(고질병) - GC
**Java Heap의 문제**
> JVM은 개발자가 메모리 해제를 안 해도 되도록 GC가 자동으로 정리한다.   
> 근데 GC가 동작할 때 JVM 전체를 잠깐 멈춘다. 이게 Stop-The-World다.   

**Spark에서 왜 문제냐**
> Executor가 수억건 데이터를 처리하면 객체가 엄청나게 생성/소멸된다. GC가 자주 발동되고, 발동될 때마다 Executor 전체가 멈춘다. 대용량 처리일수록 GC 부담이 커져서 성능이 급격히 떨어진다.
#### GC란
- GC(Garbage Collector)는 JVM이 사용하지 않는 Java 객체를 자동으로 메모리에서 제거하는 기능
```
[Heap 메모리]
┌─────────────────────────────────┐
│  User 객체 (사용중)               │
│  Order 객체 (사용중)              │
│  String 객체 (더이상 참조없음)      │  ← GC가 제거 대상으로 표시
│  List 객체 (더이상 참조없음)        │  ← GC가 제거 대상으로 표시
└─────────────────────────────────┘
         │
         │ GC 발동
         ▼
┌─────────────────────────────────┐
│  User 객체 (사용중)              │
│  Order 객체 (사용중)             │
│  (빈 공간)                      │  ← 제거됨
│  (빈 공간)                      │  ← 제거됨
└─────────────────────────────────┘
```

#### Java의 노력 -오프힙도입
**Heap vs Off-Heap**
Heap:
- JVM이 자동 관리
- GC 대상 → Stop-The-World 발생
- 크기 제한 있음 (`-Xmx` 옵션으로 설정)
- 개발자가 직접 해제 불필요

Off-Heap:
- Off-Heap은 JVM Heap 외부의 OS가 직접 관리하는 메모리 공간
- OS가 직접 관리
- GC 대상 아님 → Stop-The-World 없음
- 물리 메모리가 허용하는 만큼 사용 가능
- 개발자가 직접 할당/해제 책임
> OS는 원래 메모리를 프로세스 단위로 직접 할당/해제한다. GC 같은 자동 관리 개념이 없다. 할당하면 그냥 있고, 해제하면 그냥 없어진다. 중간에 멈추는 일이 없다.

```
[물리 메모리 (RAM)]
┌─────────────────────────────────┐
│  JVM 프로세스                   │
│  ┌───────────────────────────┐  │
│  │  Heap                     │  │  ← JVM이 관리, GC 대상
│  │  (Java 객체 저장)          │  │
│  ├───────────────────────────┤  │
│  │  Stack                    │  │  ← 메서드 호출 스택
│  ├───────────────────────────┤  │
│  │  Metaspace                │  │  ← 클래스 메타데이터
│  └───────────────────────────┘  │
│                                 │
│  Off-Heap                       │  ← JVM 밖, OS 직접 관리
│  (Native Memory)                │
└─────────────────────────────────┘
```


## (2) 스파크의 메모리 효율적 사용을 위한 노력

### 배경

RDD는 순수 JVM 객체로만 처리
- Java 객체 오버헤드로 메모리 낭비 (실제 데이터보다 메타데이터가 더 큼)
- GC가 수억건 객체를 관리하다 Stop-The-World 발생
- Executor간 데이터 전송 시 느린 Java 직렬화 사용
```
[Executor 메모리 - Java 객체 방식]
┌─────────────────────────────────┐
│  User 객체                      │
│  ├─ Mark Word (8바이트)          │  ← 이 객체의 GC상태, 락 정보 등 JVM 관리용
│  ├─ Class Pointer (8바이트)      │  ← 이 객체가 User클래스임을 표시
│  ├─ age: 30 (4바이트)           │  ← 실제 데이터
│  ├─ name 포인터 (8바이트)        │  ← String이 다른 메모리에 있음
│  └─ String 객체                 │
│      ├─ 객체 헤더 (16바이트)     │  ← 또 메타데이터
│      └─ "Eddie" (5바이트)       │  ← 실제 데이터
└─────────────────────────────────┘
실제 데이터 9바이트, 총 49바이트 사용
메타데이터가 데이터 옆에 분산 저장

```
이를 해결하기 위해 Spark 1.4에서 Tungsten, Encoder가 등장했다.

### 인코더-텅스텐 도입
> java 객체의 직렬화/역직렬화 방식이 아닌 스파크만의 직렬화/역직렬화 방식 만듬 
> Spark1.X 버전부터 도입됨  

**Encoder 역할**
- Driver에서 스키마 규칙 생성 ("0~3바이트가 age, 4~8바이트가 name")
- Executor에 배포
- 이 규칙 덕분에 데이터 옆에 메타데이터를 붙일 필요가 없어짐

**Tungsten 역할**
- Encoder 규칙을 실제로 실행하는 엔진
- Java 객체 → Tungsten 바이너리 변환
- 데이터를 Off-Heap에 저장 (GC 대상 아님)
- Executor간 전송 시 압축된 바이너리로 전송

**Encoder/Tungsten이 해결한 것**
1. 메모리 사용량 감소
    - Java 객체 오버헤드(헤더, 포인터) 제거
    - 같은 데이터를 더 적은 메모리에 저장
2. 네트워크 전송량 감소
    - Executor끼리 주고받을 때 압축된 바이너리로 전송
    - Java 객체보다 훨씬 작은 용량
3. 직렬화/역직렬화 속도 향상
    - Java 객체 변환 자체가 빨라짐
```

[Executor 메모리 - Tungsten 방식]

Encoder (별도 저장):
┌─────────────────────────────────┐
│  User 스키마                    │
│  ├─ 0~3바이트: age (Int)        │
│  └─ 4~8바이트: name (String)    │
└─────────────────────────────────┘

실제 데이터 (연속 메모리):
┌─────────────────────────────────┐
│  [30][Eddie]                    │  ← 9바이트만 사용
└─────────────────────────────────┘

```
- 텅스텐 → 오프힙사용 
```
[Executor JVM 메모리]
┌─────────────────────────────────┐
│  Heap 영역                      │
│  ┌───────────────────────────┐  │
│  │  Encoder 객체             │  │  ← 코드/규칙 저장 영역
│  │  (스키마 규칙 보유)        │  │
│  └───────────────────────────┘  │
│  ┌───────────────────────────┐  │
│  │  Tungsten Off-Heap 영역   │  │  ← 실제 데이터 저장 영역
│  │  [30][Eddie][25][John]... │  │
│  └───────────────────────────┘  │
└─────────────────────────────────┘
```

**작동 과정**
```
[Driver]
Encoder 생성 (스키마 규칙)
         │
         │ Executor에 배포
         ▼
[Executor]
Tungsten이 Encoder 규칙으로
Java 객체 → 바이너리 변환
         │
         │ Off-Heap에 저장 (GC 없음)
         │
         │ 셔플 시 바이너리 그대로 전송
         ▼
[Executor 2]
Encoder 규칙으로 바이너리 → Java 객체 복원
(람다/UDF 실행 시에만)
```
- 실행 플로우
```
[Driver]
┌─────────────────────────────────────┐
│  1. Catalyst가 실행계획 + 스키마 확정 │
│  2. Encoder 생성 (변환 규칙)         │
│  3. 태스크 + 스키마 + Encoder 패키징 │
└─────────────────────────────────────┘
         │
         │ 태스크 + Encoder 전송
         ▼
┌──────────────┐  셔플시 바이트 전송  ┌──────────────┐
│  Executor 1  │ ──────────────────► │  Executor 2  │
│ ┌──────────┐ │                     │ ┌──────────┐ │
│ │Tungsten  │ │                     │ │Tungsten  │ │
│ │Java객체  │ │                     │ │바이트→   │ │
│ │→바이트   │ │                     │ │Java객체  │ │
│ └──────────┘ │                     │ └──────────┘ │
└──────────────┘                     └──────────────┘
```


**핵심 정리**
- Encoder: 메타데이터를 데이터 옆이 아닌 한곳에 분리 보관
- Tungsten: 그 덕분에 데이터만 Off-Heap 바이너리로 저장/전송
- 결과: 메모리 절약 + GC 부담 감소 + 네트워크 전송량 감소

### 스파크에서 Java 직렬화처리하는 경우 
**컴포넌트별 직렬화 발생 시점**
- Driver → Executor: 태스크 코드 전송 시
- Executor → Executor: 셔플(groupBy, join) 시 파티션 데이터 교환 시
- Executor → 디스크: 메모리 부족으로 스필(spill) 발생 시
- Executor → Driver: 결과 collect() 시

**역직렬화 발생 여부 정리**

|방식|역직렬화|
|---|---|
|DataFrame DSL|없음|
|DataFrame 람다|불가 (람다 못씀)|
|Dataset DSL|없음|
|Dataset 람다|발생|
|SQL|없음|
#### 예시 
**DataFrame**
```scala
// DSL - 역직렬화 없음 (Catalyst가 바이너리 직접 처리)
df.filter(df("age") > 30)

// DataFrame은 타입이 없어서 람다 사용 불가
// df.filter(user => user.age > 30)  → 컴파일 에러
```


**Dataset**
```scala
// DSL - 역직렬화 없음
ds.filter(ds("age") > 30)

// 람다 - 역직렬화 발생 (JVM 객체 필요)
ds.filter(user => user.age > 30)
```


**SQL**
```scala
// 역직렬화 없음 (Catalyst가 직접 처리)
spark.sql("SELECT * FROM users WHERE age > 30")
```

---

**결론**
> 역직렬화가 발생하는 경우는 Dataset 람다 하나뿐이다. DataFrame DSL, Dataset DSL, SQL 전부 Catalyst가 Tungsten 바이너리를 직접 처리한다. Dataset 람다만 JVM 코드라 어쩔 수 없이 역직렬화가 발생한다.

