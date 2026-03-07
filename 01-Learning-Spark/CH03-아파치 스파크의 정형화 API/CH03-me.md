# 1. 스파크: RDD의 아래에는 무엇이 있는가

## 1.1 스파크의 핵심 특성

- 의존성
    - 어떤 입력을 필요로 하고 현재의 RDD가 어떻게 만들어지는지 스파크에게 가르쳐 주는 의존성
    - 결과를 새로 만들어야 하는 경우 스파크는 이 의존성 정보를 참고하여 재연산 후 RDD 생성
    - 의존성은 결국 RDD에  **유연성**을 부여
- 파티션(지역성 정보 포함)
    - 이그제큐터들이 파티션별로 병렬 연산 가능하도록 함
    - 각 이그제큐터가 가까이 있는 데이터를 처리할 수 있도록 지역성 정보를 사용
- 연산 함수: Partition → Iterator[T]
    - RDD에 저장되는 데이터를 Iterator[T] 형태로 만들어주는 연산 함수

## 1.2 문제점

- 연산 함수나 연산식 자체가 스파크에 투명하지 않음 (스파크는 어떤 작업을 하는지 모름) → 최적화 못함
- Iterator[T] 데이터 타입이 파이썬 RDD에서 불투명함 → 스파크에서는 단지 파이썬 기본 객체로만 인식이 가능

→ 결과적으로 스파크가 연산 순서를 재정렬해 효과적인 질의 계획을 바꾸는 능력을 방해

<br>

# 2. 스파크의 구조 확립

## 2.1 스파크 DSL 연산자

- `DSL(Domain Sepecific Language: 도메인 특화 언어)` : 스파크에서 SQL 문장을 문자열로 작성하는 대신, 프로그래밍 언어(Scala, Java, Python 등)의 메서드 호출 형태로 데이터 조작을 수행하는 방식
- 스파크에서 데이터를 처리할 때 SparkSQL 또는 Dataset/DataFrame API 사용
- 여기서 DSL 연산자란 후자인 API 방식에서 사용하는 특정한 메서드(함수)들을 의미
    - **선택 및 필터링**: `select()`, `filter()`, `where()`
    - **그룹화 및 집계**: `groupBy()`, `agg()`, `count()`, `sum()`
    - **정렬 및 변환**: `orderBy()`, `withColumn()`, `drop()`
    - **조인**: `join()`

## 2.2 핵심적인 장점과 이득

- **최적화:** 구조화를 통해 사용자의 의도를 이해할 수 있기 때문에 효과적인 실행을 위해 연산들을 최적화하거나 적절하게 재배열 가능
- **가독성:** 읽기가 간단하다는 것 외에도 상위 수준 API는 컴포넌트들과 언어를 통틀어 일관성을 갖음
- **컴파일 타임 체크 가능:** SQL은 실행 전까지 오타를 잡기 어렵지만, DSL은 코드 작성 단계에서 문법 오류를 확인 가능
    - 컴파일: 인간이 작성한 고수중 언어를 컴퓨터가 이해할 수 있는 저수준 언어(기계어)로 번역하는 과정
- 비정형 하위수준의 RDD API도 사용 가능

<br>

# 3. 데이터 프레임 API

- 구조, 포맷 등 몇몇 특정 연산 등에 있어서 판다스 데이터 프레임에 영향을 받은 **스파크 데이터 프레임은 이름 있는 칼럼과 스키마를 가진 분산 인메모리 테이블처럼 동작**

## 3.1 스파크의 기본 데이터 타입

- 지원하는 프로그래밍 언어와 맞게 스파크는 기본적인 내부 데이터 타입을 지원
- 타입은 스파크 애플리케이션에서 선언할 수 있고, 스키마에서도 정의 가능

### 3.1.1 스파크의 기본 스칼라 데이터 타입

| **스파크 타입 (Spark SQL)** | **스칼라 타입 (Scala)** | **설명** |
| --- | --- | --- |
| **ByteType** | `Byte` | 1바이트 부호 있는 정수 (-128 ~ 127) |
| **ShortType** | `Short` | 2바이트 부호 있는 정수 (-32,768 ~ 32,767) |
| **IntegerType** | `Int` | 4바이트 부호 있는 정수 |
| **LongType** | `Long` | 8바이트 부호 있는 정수 |
| **FloatType** | `Float` | 4바이트 단정밀도 부동 소수점 |
| **DoubleType** | `Double` | 8바이트 배정밀도 부동 소수점 |
| **DecimalType** | `BigDecimal` | 임의 정밀도의 부호 있는 십진수 (금융 데이터 등에 사용) |
| **StringType** | `String` | 문자열 데이터 |
| **BinaryType** | `Array[Byte]` | 바이트 배열 (이미지나 직렬화된 데이터) |
| **BooleanType** | `Boolean` | 논리값 (True / False) |
| **TimestampType** | `java.sql.Timestamp` | 연, 월, 일, 시, 분, 초를 포함하는 시간 데이터 |
| **DateType** | `java.sql.Date` | 시/분/초가 없는 날짜 데이터 (연, 월, 일) |

### 3.1.2 스파크의 기본 파이썬 데이터 타입

| **스파크 타입 (Spark SQL)** | **파이썬 타입 (Python)** | **파이스파크 타입 클래스 (PySpark)** |
| --- | --- | --- |
| **ByteType** | `int` (또는 `bytes`) | `ByteType()` |
| **ShortType** | `int` | `ShortType()` |
| **IntegerType** | `int` | `IntegerType()` |
| **LongType** | `int` | `LongType()` |
| **FloatType** | `float` | `FloatType()` |
| **DoubleType** | `float` | `DoubleType()` |
| **DecimalType** | `decimal.Decimal` | `DecimalType()` |
| **StringType** | `str` | `StringType()` |
| **BinaryType** | `bytearray` | `BinaryType()` |
| **BooleanType** | `bool` | `BooleanType()` |
| **TimestampType** | `datetime.datetime` | `TimestampType()` |
| **DateType** | `datetime.date` | `DateType()` |

## 3.2 스파크의 정형화 타입과 복합 타입

### 3.2.1 스파크의 스칼라 정형화 데이터 타입

| **스파크 타입 (Spark SQL)** | **스칼라 타입 (Scala)** | **설명** |
| --- | --- | --- |
| **ArrayType(elementType, containsNull)** | `Seq[T]` (또는 `Array`) | 동일한 타입의 요소들을 담은 가변 길이 리스트 |
| **MapType(keyType, valueType, valueContainsNull)** | `Map[K, V]` | 키-값 쌍의 집합 (키는 null이 될 수 없음) |
| **StructType(List(StructField, ...))** | `Row` | 서로 다른 타입을 가질 수 있는 필드들의 집합 (객체/구조체) |

### 3.2.2 스파크의 파이썬 정형화 데이터 타입

| **스파크 타입 (Spark SQL)** | **파이썬 타입 (Python)** | **파이스파크 정의 (Example)** |
| --- | --- | --- |
| **ArrayType** | `list` | `ArrayType(StringType(), True)` |
| **MapType** | `dict` | `MapType(StringType(), IntegerType())` |
| **StructType** | `tuple` 또는 `list` | `StructType([StructField("name", StringType())])` |

## 3.3 스키마와 데이터 프레임 만들기

- 스파크에서 `스키마`는 데이터 프레임을 위해 **칼럼 이름과 연관된 데이터 타입을 정의**한 것
- 미리 스키마를 정의했을 때 장점:
    - 스파크가 데이터 타입을 추측하지 않아도 됌
    - 스파크가 스키마를 확정하기 위해 파일의 많은 부분을 읽어 들이려고 별도의 잡을 만드는 것을 방지
    - 데이터가 스키마와 맞지 않는 경우 조기에 문제 발견 가능

### 3.3.1 스키마를 정의하는 방법

- 프로그래밍 스타일로 정의(데이터 프레임 API 사용)
    - 스칼라 예제
    
    ```scala
    import org.apache.spark.sql.types._
    
    val schema = StructType(Array(StructField("author", StringType, false),
    	StructField("title", StringType, false),
    	StructField("pages", IntegerType, false)))
    ```
    
    - 파이썬 예제
    
    ```python
    from pyspark.sql.types import *
    
    schema = StructType([
        StructField("author", StringType(), False),
        StructField("title", StringType(), False),
        StructField("pages", IntegerType(), False)
    ])
    ```
    
- DDL 사용
    - 스칼라 예제
    
    ```scala
    val schema = "author STRING, title STRING, pages INT"
    ```
    
    - 파이썬 예제
    
    ```python
    schema = "author STRING, title STRING, pages INT"
    ```
    
- 스키마 추론
    
    ```python
    # 방법 1: option() 메서드 사용
    df = spark.read \
        .option("inferSchema", "true") \
        .option("samplingRatio", 0.1) \
        .json("data_path.json")
    
    # 방법 2: createDataFrame 사용 시 (RDD나 리스트로부터 생성할 때)
    df = spark.createDataFrame(data_rdd, samplingRatio=0.1)
    ```
    

## 3. 4 칼럼과 표현식

- 데이터 프레임에서 이름이 정해진 칼럼들은 개념적으로 판다스나 R에서의 데이터 프레임이나 RDBMS 테이블의 칼럼과 유사하게 어떤 특정한 타입의 필드를 나타내는 개념
- 스파크가 지우너한느 언어에서 칼럼은 공개 메소드를 가진 객체로 표현(칼럼 타입으로 표현)
- 데이터 프레임의 `Column` 객체는 단독으로 존재할 수 없음
- 각 칼럼은 한 레코드의 로우의 일부분, 모든 로우가 합쳐져서 하나의 데이터 프레임을 구성

## 3.5 로우(row)

- 스파크에서 하나의 행은 일반적으로 하나 이상의 컬럼을 갖고 있는 로우로 표현
- 각 칼럼은 동일한 칼럼 타입일 수도 있고, 다른 타입일 수 있음
- 로우는 스파크의 객체이고 순서가 있는 필드 집합 객체 → 0부터 시작하는 인덱스로 접근 가능

## 💡collect() 연산자의 작동 원리와 OOM 발생 원인

`collect()` 는 스파크의 액션 연산자 중 하나로 분산 **저장되어 있는 모든 데이터를 드라이버로 끌어모으는 역할**

- 작동 과정:
    - **분산 데이터 존재**: Spark는 대용량 데이터를 여러 서버(Executor)에 나누어 처리
    - **데이터 요청**: 드라이버가 각 Executor에게 흩어져 있는 모든 파티션 데이터를 보내달라고 요청
    - **데이터 전송**: 모든 Executor는 자신이 가진 데이터를 네트워크를 통해 드라이버에게 전송
    - **로컬 객체 변환**: 드라이버는 수집된 데이터를 메모리에 적재하여 해당 언어의 리스트(List)나 배열(Array) 형태로 변환
- collect() 사용 시 OOM 발생 이유:
    - **메모리 용량 초과**: 드라이버는 보통 클러스터의 개별 워커 노드보다 메모리가 작게 설정, 대용량 데이터를 그보다 작은 용량의 드라이버 메모리로 모으게되면 OOM 발생 가능
        - 드라이버는 데이터 자체가 아니라 “데이터를 어떻게 나누고 어디서 처리할지”에 대한 실행 계획(DAG)과 메타데이터만 관리하기 때문에 대량 메모리 불필요
    - **단일 지점 병목**: 스파크의 기본 철학은 “데이터가 있는 곳으로 연산을 보내는 것”인데 collect()는 반대로 “모든 데이터를 한 곳으로 모으는 것”
    - **네트워크 부하**: 대량의 데이터가 동시에 드라이버로 몰리면서 네트워크 대역폭을 초과하거나 직렬화/역직렬화 과정에서 과도한 CPU/메모리 부하를 일으킴

<br>

# 4. 데이터세트 API

- 데이터 프레임과 데이터세트 API를 유사한 인터페이스를 갖도록 정형화 API로 일원화
- 데이터세트는 정적 타입(typed) API와 동적 타입 (Untyped) API의 두 특성을 모두 가짐

## 4.1 정적 타입 객체, 동적 타입 객체, 포괄적인 Row

### 4.1.1 데이터 프레임

이름이 붙은 컬럼으로 구성된 데이터의 집합

- 파이썬의 Pandas나 R의 데이터프레임, 혹은 SQL의 테이블과 유사
- **비정형 타입(Untyped)**: 데이터 안에 무엇이 들어있는지는 알지만 **컴파일 시점에 그 타입이 맞는지 검사하지 않음**
- Python, Scala, Java, R에서 모두 동일하게 사용 가능
- 스파크 내부에서 `Row` 객체의 집한으로 관리

### 4.1.2 데이터세트

데이터 프레임의 장점에 강력한 타입 검사 기능을 더한 것

- 특정 클래스(Java/Scala의 Case Class)로 정의된 객체들의 리스트와 같음
- **정형 타입(Typed): 코드를 실행하기 전(컴파일 타임)에 데이터 타입이 맞는지 확인**
- Scala와 Java에서만 사용 가능
- `Dataset[T]` 형태로 정의, 여기서 `T`는 사용자가 정의한 데이터 구조(객체)

### 4.1.3 데이터세트 생성

- 스칼라: case class 사용
    
    ```scala
    import org.apache.spark.sql.SparkSession
    
    // 1. 데이터의 구조(Schema)를 case class로 정의합니다.
    // 각 필드의 이름과 타입이 곧 스파크 테이블의 컬럼명과 타입이 됩니다.
    case class User(id: Int, name: String, age: Int, city: String)
    
    object SparkSchemaExample {
      def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .appName("CaseClassSchemaExample")
          .master("local[*]")
          .getOrCreate()
    
        // 2. 스칼라의 기본 컬렉션을 데이터세트로 변환하기 위해 임포트가 필요합니다.
        import spark.implicits._
    
        // 3. 데이터를 생성합니다. (User 객체들의 시퀀스)
        val userData = Seq(
          User(1, "Alice", 30, "Seoul"),
          User(2, "Bob", 25, "Busan"),
          User(3, "Charlie", 35, "Incheon")
        )
    
        // 4. case class를 기반으로 Dataset[User]를 생성합니다.
        // 별도의 스키마 정의 없이도 User 클래스를 통해 스키마가 자동으로 추론됩니다.
        val userDS = userData.toDS()
    
        // 5. 결과 출력 및 스키마 확인
        userDS.show()
        userDS.printSchema()
    
        // 6. DSL 연산자를 사용한 타입 안전한 필터링 예시
        // 컴파일 시점에 'age' 컬럼이 Int 타입임을 알고 있으므로 안전하게 비교가 가능합니다.
        val filteredDS = userDS.filter(u => u.age > 28)
        filteredDS.show()
    
        spark.stop()
      }
    }
    ```
    
- 자바: javaBean 사용

### “데이터 세트가 사용되는 동안은 하부의 스파크 SQL 엔진이 JVM 객체의 생성, 변환, 직렬화, 역직렬화를 담당. 그리고 데이터세트 인코더의 도움을 받아 자바의 오프힙 메모리 관리 또한 하게됌”

- JVM 객체와 직렬화/역직렬화
    - JVM 객체: 자바 가상 머신(JVM) 메모리 안에서 살아잇는 일반적인 자바/스칼라 객체, 관리가 편하지만 메모리 사용량이 많음
    - 직렬화: 객체를 네트워크로 전송하거나 디스크에 저장하기 위해 이진 데이터(0과 1의 형태)로 변환하는 과정
    - 역직렬화: 이진 데이터를 다시 사용 가능한 자바 객체로 복구하는 과정
- 데이터세트 인코더
    - 사용자가 정의한 `case class`나 자바 객체를 스파크의 효율적인 내부 이진 형식(Tungsten)으로 변환해주는 역할
- 오프힙(Off-heap) 메모리
    - 온힙(On-heap): JVM이 직접 관리하는 메모리 영역, 가비지 컬렉션(GC)의 대상
    - 오프힙(Off-heap): JVM 외부의 메모리 영역, 스파크가 직접 관리하므로 대용량 데이터를 다룰 때 GC의 간섭을 받지 않아 훨씬 안정적이고 빠름

→ 데이터세트를 쓰면 스파크가 알아서 메모리 관리를 효율적으로 함을 의미

1. 엔진이 객체의 생존 주기 관리
    - 보통 자바 프로그램에서는 개발자가 객체를 생성 및 관리하지만 스파크 데이터세트를 이용하면 스파크 SQL 엔진(카탈리스트 & 텅스텐)이 개입 → 데이터를 언제 객체로 만들고 언제 이진 데이터로 압축할지 최적의 타이밍을 계산
2. 인코더를 통한 효율적인 데이터 변환
    - 일반 자바 객체보다 메모리를 훨씬 적게 차지
    - 데이터를 처리할 때마다 매번 무거운 ‘역직렬화’를 전체적으로 수행하지 않고 필요한 부분만 골라내서 처리
3. 오프힙 메모리 관리를 통한 안정성
    - 데이터가 수십 GB가 넘어가도 JVM 내부의 가비지 컬렉터가 작업을 중단(stop-the-world)시키는 일이 발생하지 않음
    - 결과적으로 대규모 데이터를 처리할 때 시스템이 훨씬 더 안정적

<br>

# 5. Best Practice

## 5.1 데이터프레임(DataFrame) 사용의 Best Practice

**가장 권장되는 일반적인 선택지**

- **비즈니스 로직 처리**: SQL 스타일의 연산(필터, 조인, 집계)이 주를 이룰 때 사.
- **언어 간 일관성**: Python(PySpark)이나 R을 사용한다면 사실상 데이터프레임이 최선
- **성능 최적화**: 텅스텐 엔진과 카탈리스트 옵티마이저가 가장 효율적으로 작동하는 영역
- **권장 상황**:
    - 데이터 분석 및 리포팅 작업을 할 때
    - 성능 최적화(코드 최적화)를 스파크 엔진에 맡기고 싶을 때
    - 데이터의 스키마를 이미 알고 있거나 추론 가능할 때

## 5.2 데이터세트(Dataset) 사용의 Best Practice

**안정성과 타입 안전성이 중요한 대규모 엔지니어링 프로젝트에 적합**

- **강력한 타입 체크**: 컴파일 시점에 오류를 잡아내야 하는 복잡한 데이터 파이프라인을 구축할 때 사용합니다.
- **객체 지향 코드**: 데이터를 단순한 로우(Row)가 아닌, 특정 비즈니스 도메인 객체(case class)로 다루고 싶을 때 유리
- **권장 상황**:
    - Scala나 Java를 주 언어로 사용하여 대규모 애플리케이션을 개발할 때
    - 복잡한 람다(Lambda) 함수나 사용자 정의 함수(UDF)를 많이 사용해야 할 때
    - 데이터의 타입 불일치로 인한 런타임 에러를 사전에 방지하고 싶을 때

## 5.3 RDD 사용의 Best Practice

**현대적인 스파크 개발에서는 거의 사용하지 않지만, 특수한 경우에만 사용**

- **저수준 제어**: 데이터의 세밀한 물리적 배치나 파티셔닝을 직접 제어해야 할 때 사용
- **비구조적 데이터**: 텍스트 파일이나 로그 파일 등 구조를 전혀 잡을 수 없는 원시 데이터를 다룰 때 사용
- **직렬화 가능 객체**: 데이터프레임이 지원하지 않는 특수한 형태의 객체를 다뤄야 할 때 사용
- **권장 상황**:
    - 기존에 작성된 오래된 스파크 코드를 유지보수할 때
    - 스파크의 고수준 API가 제공하지 않는 특수한 분산 알고리즘을 직접 구현해야 할 때
    - 데이터의 물리적인 '위치'나 '개수'를 하나하나 수동으로 조절해야 할 때

<br>

# 6. 스파크 SQL과 하부의 엔진

## 6.1 스파크 SQL과 하부 엔진의 구조

스파크 SQL은 사용자가 SQL 문장이나 데이터프레임 API를 통해 입력한 명령을 받아 내부적인 실행 계획으로 변환함. 이 과정의 중심에는 성능을 극대화하기 위한 두 가지 핵심 엔진이 있음

- **카탈리스트 옵티마이저(Catalyst Optimizer)**: 사용자의 코드를 분석하여 가장 효율적인 실행 계획을 수립하는 논리적 최적화 엔진
- **텅스텐(Tungsten) 엔진**: 하드웨어 성능을 한계까지 끌어올리기 위해 메모리 관리 및 코드 생성을 담당하는 물리적 실행 엔진

## 6.2 카탈리스트 옵티마이저 (Catalyst Optimizer)

카탈리스트는 **규칙 기반(Rule-based) 및 비용 기반(Cost-based) 최적화**를 통해 쿼리를 변환, 사용자가 "무엇을" 원하는지 선언하면, 카탈리스트는 이를 "어떻게" 수행할지 결정하는 역할을 함

### 6.2.1 최적화 4단계 과정

1. **분석(Analysis)**: 데이터프레임의 컬럼 이름이나 테이블 존재 여부를 카탈로그(메타데이터)와 대조하여 유효성 검사
2. **논리적 계획 수립(Logical Planning)**: 구체적인 실행 방법이 아닌 추상적인 **연산 순서를 정의**하며, 여기서 규칙 기반 최적화(예: Filter Pushdown)가 일어남
3. **물리적 계획 수립(Physical Planning)**: 실제로 데이터를 어떻게 읽고 조인할지(예: Broadcast Join vs SortMerge Join) 여러 전략을 세우고, 비용 모델을 통해 가장 빠른 최적의 계획 선택
4. **코드 생성(Code Generation)**: 선택된 계획을 실제 실행 가능한 자바 바이트코드로 변환 
    - 포괄 코드 생성(Whole-Stage Code Generation): 물리적 쿼리 최적화 단계로 전체 쿼리를 하나의 함수로 합치면서 가상 함수 호출이나 중간 데이터를 위한 CPU 레지스터 사용을 없앰

## 6.3 카탈리스트의 주요 최적화 기법 예시

- **조건절 푸시다운(Predicate Pushdown)**: 데이터를 모두 읽은 후 필터링하는 것이 아니라, **데이터를 읽는 시점에 필요한 조건만 골라 읽어** 네트워크와 메모리 부하를 줄임
- **열 선택(Column Pruning)**: 테이블의 수많은 컬럼 중 쿼리에 필요한 **특정 컬럼만 선택하여 읽기 효율 향상**
- **상수 접기(Constant Folding)**: `10 * 100`과 같은 연산을 실행 시점이 아닌 최적화 단계에서 미리 계산하여 반복 연산 제거
