# 1. 스파크 SQL의 특징

- 상위 수준의 정형화 API(데이터프레임, 데이터세트)가 엔진으로 제공
- 스파크 SQL은 파일 시스템(CSV, JSON, Parquet), 하이브(Hive) 테이블, 관계형 데이터베이스(JDBC/ODBC) 등 다양한 데이터 소스로부터 데이터를 일관된 방식으로 읽고 쓸 수 있음
- 스파크 애플리케이션에서 데이터베이스 안에 테이블 또는 뷰로 저장되어 있는 정형 데이터와 소통할 수 있도록 프로그래밍 인터페이스(API) 제공
- SQL 쿼리를 정형 데이터에 대해 실행할 수 있는 대화형 셸(spark-sql,pyspark 등) 제공
- ANSI SQL: 2003 호환 명령 및 HiveQL을 지원

## 💡ANSI SQL:2003 호환 명령과 HiveQL

1. ANSI SQL:2003 호환 및 ANSI 모드
- 스파크 SQL은 초기부터 ANSI SQL 표준을 지향해 왔으며, 특히 **스파크 3.0**부터는 표준 호환성을 대폭 강화한 ANSI 모드를 도입
    - 과거에는 잘못된 데이터 연산(0으로 나누기 등)시 결과값을 `NULL`로 반환하는 관대한(Permissive) 방식을 취했다면 **ANSI 모드에서는 표준에 따라 오류를 발생 시켜 데이터 정합성을 높임**
- 설정 방법: `spark.sql.ansi.enabled`
    - 스파크 3.x: 기본값 false
    - 스파크 4.0: 기본값 true
2. HiveQL 지원
- 스파크 SQL은 하이브에서 사용하던 기존 쿼리를 그대로 사용할 수 있도록 HiveQL 문법을 거의 완벽하게 지원
- 호환성: `SELECT`, `GROUP BY`, `JOIN` 같은 기본 쿼리는 물론, 하이브 전용 구문인 `CLUSTER BY`, `SORT BY`, `DISTRIBUTE BY` 등도 지원
- **하이브 메타스토어(Metastore) 연동**: 기존 하이브에 저장된 테이블 정의와 SerDe(직렬화/역직렬화) 라이브러리를 그대로 읽어올 수 있음
- **사용 조건**: 코드 내에서 `SparkSession`을 생성할 때 `.enableHiveSupport()` 메서드를 호출해야 하이브 전용 기능과 메타스토어에 접근 가능

<br>

# 2. SQL 테이블과 뷰

- 스파크는 각 테이블과 해당 데이터에 관련된 정보인 스키마, 설명, 테이블명, 데이터베이스명, 칼럼명, 파티션, 실제 데이터의 물리적 위치 등의 메타데이터를 가짐 → 모든 정보는 중앙 메타스토어에 저장
- 기본적으로 `/user/hive/warehouse`에 있는 아파치 하이브 메타스토어를 사용하여 테이블에 대한 모든 메타데이터를 유지
- `spark.sql.warehouse.dir` 을 로컬 또는 외부 분산 저장소로 설정하여 다른 위치로 기본 경로 변경 가능

## 2.1 관리형 테이블과 비관리형 테이블

### 2.1.1 관리형 테이블(Managed Table)

- 메타데이터와 파일 저장소의 데이터를 모두 관리
- 저장 위치: 별도의 경로를 지정하지 않으면 기본 웨어 하우스 디렉토리(`/user/hive/warehouse`)에 저장
- 파일 저장소: 로컬 파일 시스템, HDFS, Amazon S3, Azure Blob 등
- `DROP TABLE` 명령어 실행 시 메타데이터와 실제 데이터 파일 모두 삭제

### 2.1.2 비관리형 테이블(External Table)

- 메타데이터만 관리하고 실제 **데이터는 외부 경로에 있는 것을 참조**만 하는 테이블
- 이미 특정 경로에 데이터가 존재하거나, 다른 시스템과 데이터를 공유해야 할 때 사용
- `DROP TABLE` 명령어 실행 시 메타데이터만 삭제되고 실제 데이터 파일은 그대로 보존
- 명령어 작성시 `EXTERNAL` 또는 `LOCATION` 로 경로 명시

```sql
-- SQL 방식: CREATE EXTERNAL TABLE 형식을 사용하며 끝에 LOCATION을 추가
CREATE EXTERNAL TABLE external_users (id INT, name STRING)
LOCATION '/path/to/data';

-- DataFrame API 방식: option("path", "...")을 통해 경로를 지정하고 테이블로 저장
df.write.option("path", "/path/to/data").saveAsTable("external_users")
```

### 💡 생성된 테이블 구분 방법

- DESCRIBE EXTENDED 명령어를 사용하여 생선된 테이블 구분 가능

```sql
-- 아래 명령어를 실행후 Type 항목을 확인
DESCRIBE EXTENDED 테이블명;
```

- `Location` 항목을 통해 데이터가 스파크 기본 웨어하우스(`.../warehouse/...`)에 있는지, 아니면 사용자가 지정한 외부 경로에 있는지 확인하여 부가적인 판단 가능

## 2.2 뷰 생성하기

- 뷰는 전역 또는 세션 범위 일 수 있으며 일시적으로 스파크 애플리케이션이 종료되면 사라짐
    - 전역 임시 뷰: 스파크 애플리케이션 내의 여러 SparkSession에 연결되어 있음
        - 단일 스파크 애플리케이션 내에서 여러 SparkSession을 만들 수 있는데, 예를 들어 동일한 하이브 메타스토어 구성을 공유하지 않는 두 개의 서로 다른 SparkSession애서 같은 데이터에 액세스하고 결합하고자 할 때 유용
    - 임시 뷰: 스파크 애플리케이션 내의 단일 SparkSession에 연결
- 뷰를 생성한 후에는 테이블처럼 쿼리 가능
- SQL 예제:

```sql
-- 전역 뷰 생성
CREATE OR REPLACE GLOBAL TEMP VIEW

-- 일반 뷰 생성
CREATE OR REPLACE TEMP VIEW
```

- python 예제:

```python
# 전역 뷰 생성
df.createOrReplaceGlobalTempView

# 일반 뷰 생성
df.createOrReplaceTempView
```

- 스파크는 global_temp라는 전역 임시 데이터베이스에 전역 임시 뷰를 생성하므로 해당 뷰에 액세스할 때는 `global_temp.<view_name>` 접두사를 사용해야 함

```sql
SELECT * FROM global_temp.airport_sfo_tmp_view
```

- 뷰 drop 예제:

```sql
-- SQL 
DROP VIEW IF EXISTS airport_sfo_tmp_view

-- SCALA/PYTHON
spark.catalog.dropGlobalTempView("airport_sfo_tmp_view")
spark.catalog.dropTempView("airport_sfo_tmp_view")
```

## 2.3 메타데이터 보기

### 2.3.1 메타데이터에 포함되는 주요 정보

- **테이블 정의**: 테이블 이름, 데이터베이스 이름, 소유자 정보 등
- **스키마(Schema)**: 각 열(Column)의 이름, 데이터 타입(Integer, String 등), Null 허용 여부
- **물리적 위치**: 실제 데이터 파일이 저장된 경로 (예: `s3://bucket/data/` 또는 `hdfs:///user/hive/...`)
- **파티션 정보**: 데이터가 날짜나 지역 등으로 나뉘어 저장되어 있는 구조 정보
- **통계 정보**: 테이블의 전체 크기, 행(Row)의 수 등 (쿼리 최적화에 사용됨)

### 2.3.2 메타데이터의 저장소: 카탈로그(Catalog)와 메타스토어

스파크는 이 메타데이터를 관리하기 위해 카탈로그(Catalog)라는 인터페이스를 사용

- 스파크는 기본적으로 내부 메타스토어(기본적으로 Derby DB)를 가지지만, 이 방식은 해당 스파크 세션이나 클러스터가 종료되면 정보가 사라지거나 다른 클러스터와 공유하기 어렵다는 단점을 가짐
- **메모리 기반 카탈로그 (Session-based)**:
    - 스파크 세션이 유지되는 동안에만 존재하는 임시 테이블(Temporary View) 정보 등을 관리
- **하이브 메타스토어 (Hive Metastore)**:
    - 영구적인 테이블 정보를 저장하기 위해 가장 많이 사용되는 방식
    - 보통 MySQL이나 PostgreSQL 같은 외부 RDBMS를 저장소로 활용하며, 스파크 세션이 종료되어도 테이블 정보 유지

### 💡 Glue 카탈로그의 구체적인 역할

- **추상화 레이어:** 스파크 코드에서 `SELECT * FROM my_table`이라는 쿼리를 던지면, 스파크는 실제 데이터가 S3 어디에 있는지 알지 못함. 이때 글루 카탈로그에 "my_table의 위치와 스키마가 뭐야?"라고 물어보고 답변을 받아 S3에서 데이터를 읽어옴
- **공유 저장소:** EMR, AWS Glue ETL, Athena, Redshift Spectrum 등 다양한 서비스가 동일한 글루 카탈로그를 바라보게 설정 가능 → 스파크에서 만든 관리형/비관리형 테이블을 Athena에서도 바로 쿼리 가능
- **스키마 관리:** 테이블의 컬럼명, 데이터 타입, 파티션 정보 등을 버전별로 관리

**스파크에서** Glue **카탈로그 사용 시 주의점**

- 글루 카탈로그를 메타스토어로 사용하도록 설정하면 관리형/비관리형 테이블의 동작이 Glue 카탈로그에 기록

- **관리형 테이블 생성 시:** 글루 카탈로그에 테이블 정보가 등록되고, 데이터는 보통 S3의 특정 버킷(웨어하우스 경로)에 저장. 스파크에서 테이블을 `DROP`하면 글루 카탈로그의 메타데이터와 S3의 실제 데이터가 **모두 삭제**
- **비관리형 테이블 생성 시:** S3에 이미 있는 데이터 경로를 글루 카탈로그에 "등록"만 하는 개념. `DROP`을 해도 글루 카탈로그의 정보만 삭제될 뿐, **S3의 데이터는 그대로 유지**

### 2.3.3. 메타데이터가 활용되는 과정

사용자가 `SELECT * FROM users`라는 쿼리를 날리면, 스파크는 

1. **카탈로그 조회**: "users라는 테이블이 존재하는가?" 확인
2. **스키마 확인**: "users 테이블에 어떤 컬럼들이 있는가?"를 파악하여 쿼리의 유효성 검사
3. **데이터 로드**: 메타데이터에 적힌 '물리적 경로'를 찾아가서 실제 데이터 파일 읽기

## 2.4 SQL 테이블 캐싱하기

- 스파크 3.0에서는 테이블을 LAZY로 지정 가능: 테이블을 바로 캐싱하지 않고 처음 사용되는 시점에서 캐싱함을 의미

```sql
CACHE [LAZY] TABLE <table-name>
UNCACHE TABLE <table-name>
```

<br>

# 3. 데이터 프레임 및 SQL 테이블을 위한 데이터 소스

## 3.1 DataFrameReader

- 오직 **SparkSession 인스턴스를 통해서만 DataFrameReader에 엑세스 가능**
- `SparkSession.read`: 정적 데이터 소스 읽기
- `SparkSession.readStream`: 스트리밍 소스 읽기

```sql
spark.read.format(args).option("key", "value").schema(args).load()
```

### 3.1.1 핵심 함수

| **함수명** | **역할** | **주요 인수** |
| --- | --- | --- |
| **`format(source)`** | 읽어올 데이터 소스의 형식 지정 | `csv`, `json`, `parquet`, `orc`, `jdbc`, `text` 등 |
| **`option(key, value)`** | 읽기 설정(옵션)을 하나씩 지정 | 설정 키(`String`), 설정 값(`Any`) |
| **`options(map)`** | 여러 옵션을 맵 형태로 한 번에 지정 | `Map("header" -> "true", "sep" -> ",")` 등 |
| **`schema(schema)`** | 데이터의 구조를 강제로 지정 | `StructType` 객체 또는 DDL 문자열 (`id INT, name STRING`) |
| **`load(path)`** | 데이터를 읽어 DataFrame 생성 (최종 실행) | 데이터 파일이나 디렉토리의 경로 |
- 보통 parquet 메타데이터에는 스키마를 포함하므로 스키마 지정이 필요 없음

### 3.1.2 주요 옵션

| **포맷** | **옵션 키 (Key)** | **값 (Value)** | **설명** |
| --- | --- | --- | --- |
| **CSV** | `header` | `true` / `false` | 첫 줄을 컬럼명으로 인식할지 여부 |
|  | `inferSchema` | `true` / `false` | 데이터 타입을 자동으로 추론할지 여부 |
|  | `sep` | `,`, `\t`, `|` 등 | 데이터 간의 구분자 지정 |
|  | `nullValue` | `""`, `NA`, `null` | 특정 문자열을 null로 간주할지 설정 |
| **JSON** | `multiLine` | `true` / `false` | 한 레코드가 여러 줄로 되어 있는지 여부 |
|  | `allowSingleQuotes` | `true` / `false` | 작은따옴표(') 허용 여부 |
| **Common** | `mode` | `PERMISSIVE`, `FAILFAST`, 
`DROPMALFORMED` | 오류 데이터 발견 시 처리 방식 (허용/중단/ 버림) |
|  | `recursiveFileLookup` | `true` / `false` | 하위 폴더의 파일까지 읽을지 여부 |

## 3.2 DataFrameWriter

- SparkSession이 아닌 저장하려는 데이터프레임에서 인스턴스에 액세스가 가능

```sql
df.write.format(args).mode(args).option("key", "value").save()
```

### 3.2.1 핵심 함수

| **함수명** | **역할** | **주요 인수** |
| --- | --- | --- |
| **`format(source)`** | 저장할 데이터 형식 지정 | `parquet` (기본값), `csv`, `json`, `orc`, `jdbc` 등 |
| **`mode(saveMode)`** | 기존에 데이터가 존재할 경우의 처리 방식 지정 | `append`, `overwrite`, `ignore`, `errorifexists` |
| **`option(key, value)`** | 저장 시 필요한 세부 설정 지정 | `compression`, `header`, `path` 등 |
| **`partitionBy(*cols)`** | 지정한 컬럼을 기준으로 데이터를 물리적으로 분할 저장 | 컬럼명 (예: `year`, `month`) |
| **`save(path)`** | 데이터를 실제 경로에 저장(최종 실행) | 저장될 디렉토리 경로 |
| **`saveAsTable(name)`** | 메타스토어(Glue 등)에 테이블로 등록하며 저장 | 테이블 이름 |

### 3.2.2. 주요 옵션

| **포맷** | **옵션 키 (Key)** | **값 (Value)** | **설명** |
| --- | --- | --- | --- |
| **Common** | **`compression`** | `snappy`, `gzip`, `none` | 데이터 압축 코덱 지정 |
| **CSV** | **`header`** | `true` / `false` | 첫 줄에 컬럼명을 포함할지 여부 |
|  | **`sep`** | `,`, `\t` 등 | 데이터 구분자 지정 |
| **Parquet** | **`parquet.block.size`** | 숫자 (Bytes) | 파티션 블록 크기 조정 |
| **JDBC** | **`batchsize`** | 숫자 | 한 번에 INSERT 할 레코드 수 |
|  | **`truncate`** | `true` / `false` | 덮어쓰기 시 테이블 구조는 유지할지 여부 |

## 3.3 데이터 포맷별 특징 및 핵심 옵션

| **포맷** | **주요 특징** | **읽기/쓰기 핵심 옵션** |
| --- | --- | --- |
| **Parquet** | 컬럼 기반 저장 방식의 표준이며 압축률과 쿼리 성능이 매우 우수함 | `compression`: snappy, gzip, lzo 등 압축 코덱 지정, `mergeSchema`: 서로 다른 스키마를 가진 파일 통합 여부 |
| **ORC** | 하이브(Hive)에 최적화된 컬럼 포맷으로 강력한 압축과 인덱싱 제공 | `compression`: zlib, snappy 등 지정, `orc.bloom.filter.columns`: 필터링 성능 향상을 위한 인덱스 설정 |
| **Avro** | 행 기반 이진 포맷으로 스키마 변화(Evolution) 대응이 유연함 | `avroSchema`: 특정 스키마 정의 파일(.avsc) 경로 지정, `recordName`: 레코드 이름 설정 |
| **JSON** | 계층 구조 표현이 자유로운 반정형 텍스트 포맷임 | `multiLine`: 한 레코드가 여러 줄일 때 사용, `allowSingleQuotes`: 작은따옴표 허용 여부 |
| **CSV** | 사람이 읽기 쉬운 텍스트 포맷이나 타입 정보가 없어 무거움 | `header`: 첫 줄 컬럼명 사용 여부, `inferSchema`: 데이터 타입 자동 추론, `sep`: 구분자 지정 |
| **Binary** | 이미지, PDF 등 비정형 파일을 바이트 배열로 처리함 | `pathGlobFilter`: 특정 확장자나 패턴의 파일만 필터링, `recursiveFileLookup`: 하위 폴더 재귀적 탐색 여부 |
- ORC
    - 컬럼 기반 파일 형식
    - 벡터화된 리더 사용법
        - 벡터화된 리더는 한 번에 한 행을 읽는 것이 아닌 행 블록(블록당 1,024개)을 읽어 작업을 간소화하고 검색, 필터, 집계 및 조인과 같은 집중적인 작업에 대한 CPU 사용량을 줄임
        - `spark.sql.orc.impl = native`, `spark.sql.orc.enableVectorizedReader = true` 로 설정하면 스파크는 벡터화된 ORC 리더 사용
        - SQL 명령에서 `USING HIVE OPTIONS (fileFormat 'ORC')` 을 통해 생성된 하이브 ORC SerDe(직렬화 및 역직렬화) 테이블의 경우 스파크 구성 파라미터인 `spark.sql.hive.convertMetastoreOrc = true`로 설정되었을 때 벡터화된 리더가 사용
- Binary
    - 이미지, PDF, 오디오 파일처럼 텍스트로 읽을 수 없는 데이터를 스파크로 가져올 때 사용
    - 이진 파일 데이터 소스는 열이 있는 데이터 프레임 생성
        - 경로: StringType
        - 수정시간: TimestampType
        - 길이: LongType
        - 내용: BinaryType
    - 파일 읽기: 데이터 소스 형식을 `binaryFile` 로 지정

### 3.3.1 공통 실행 옵션 및 모드

1. 읽기 모드 (Read Mode)

- **PERMISSIVE**: 손상된 레코드를 `null` 처리하고 실행 지속
- **DROPMALFORMED**: 형식이 잘못된 레코드를 무시하고 로드
- **FAILFAST**: 비정상 데이터 발견 시 즉시 에러 발생 및 중단

2. 쓰기 모드 (Save Mode)

- **overwrite**: 기존 경로의 데이터를 삭제 후 새로 기록
- **append**: 기존 데이터 끝에 새로운 데이터 추가
- **ignore**: 데이터가 존재할 경우 아무 작업도 하지 않음
- **errorifexists**: 데이터 존재 시 에러 발생 (기본값)

3. 구조적 최적화 옵션

- **partitionBy**: 특정 컬럼 기준으로 디렉토리 분할 저장
- **bucketBy**: 해시 함수를 이용해 데이터를 지정된 개수의 버킷으로 분할 (주로 테이블 저장 시 사용)
