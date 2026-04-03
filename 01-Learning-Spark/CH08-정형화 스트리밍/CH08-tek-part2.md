# Learning Spark - 상태 정보 유지 스트리밍 처리

> 출처: Learning Spark (2nd Edition) | 실무 데이터 엔지니어링 관점 정리

---

## 목차

1. [Stateless vs Stateful 변환](#0-stateless-vs-stateful-변환)
2. [상태 정보 유지 스트리밍 집계](#1-상태-정보-유지-스트리밍-집계)
   - 시간과 연관 없는 집계
   - 이벤트 타임 윈도우에 의한 집계
3. [스트리밍 조인](#2-스트리밍-조인)
   - 스트림-정적 데이터 조인
   - 스트림-스트림 조인
4. [임의의 상태 정보 유지 연산](#3-임의의-상태-정보-유지-연산)
   - mapGroupsWithState()
   - 타임아웃을 통한 비활성 그룹 관리
   - flatMapGroupsWithState() 일반화
5. [성능 튜닝](#4-성능-튜닝)

---

## 전체 구조 개요

```
Structured Streaming - 변환 분류
==========================================================================

[Stateless 변환]                    [Stateful 변환]
 map / filter / select               groupBy().agg()
 flatMap / explode                   window() 집계
 단순 컬럼 연산                       스트림-스트림 조인
                                     mapGroupsWithState
                                     flatMapGroupsWithState

==========================================================================

입력 스트림 (Kafka, Kinesis, 파일 소스 등)
    |
    v
+-----------------------------------------------------------------------+
|                        Stateful 연산 레이어                             |
|                                                                       |
|   [집계]                [조인]               [임의 상태]                |
|   시간 무관 집계          스트림-정적 조인      mapGroupsWithState        |
|   윈도우 집계             스트림-스트림 조인    flatMapGroupsWithState    |
|                                                                       |
|   내부 상태 저장소: StateStore (기본: HDFS 기반, 옵션: RocksDB)          |
|   복구 보장:        체크포인트 (checkpointLocation 필수)                 |
+-----------------------------------------------------------------------+
    |
    v
출력 싱크 (Delta Lake, Kafka, JDBC, Console 등)
```

---

## 0. Stateless vs Stateful 변환

### 핵심 구분

```
Stateless 변환                      Stateful 변환
--------------------------------------------------------------
배치 간 상태 불필요                  배치 간 상태 유지 필요
이전 이벤트와 무관하게 처리           이전 이벤트 결과를 참조해 처리
각 마이크로 배치 독립 실행            StateStore에 중간 결과 누적

예: map, filter, select,            예: groupBy+agg, window,
    flatMap, explode                    stream-stream join,
                                        mapGroupsWithState
--------------------------------------------------------------
```

**왜 구분이 중요한가**

- Stateful 연산은 StateStore 메모리를 소비하므로 방치 시 OOM 발생
- Stateful 연산에는 반드시 체크포인트 경로 설정 필요
- 출력 모드(append / update / complete) 선택 가능 범위가 달라짐

---

## 1. 상태 정보 유지 스트리밍 집계

`groupBy().agg()` 집계 시, Spark는 그룹별 중간 상태를 StateStore에 보관하고 마이크로 배치마다 누적 갱신.

이 방식을 **관리되는(managed) 상태 연산**이라 부름 — Spark가 상태 정리 시점을 자동 결정.

### 출력 모드와 상태 정리 규칙

집계 결과를 어떻게 출력하느냐에 따라 StateStore 동작이 달라짐.

| 출력 모드 | 출력 내용 | 상태 정리 시점 | 주의사항 |
|---------|---------|-------------|---------|
| `update` | 변경된 그룹만 출력 | 워터마크 경과 후 해당 상태 삭제 | 다운스트림 싱크가 upsert 지원해야 함 |
| `complete` | 전체 결과 테이블 재출력 | **절대 정리 안 됨** | 워터마크 설정해도 state 무한 증가 위험 |
| `append` | 더 이상 변경 없는 행만 출력 | 워터마크 경과 후 출력 + 삭제 | 윈도우 집계 + 워터마크 조합에서만 사용 가능 |

---

### 1-1. 시간과 연관 없는 집계

- **목적** — 스트리밍 전체 기간에 걸친 누적 통계 계산 (총 판매액, 총 이벤트 수 등)
- **이유** — 윈도우 없이 groupBy만 사용하면 Spark는 모든 키에 대해 상태를 무기한 보관
- **결과** — StateStore 크기가 유니크 키 수에 비례해 계속 증가 (키 수가 제한적인 경우에만 안전)

```
이벤트 스트림 처리 흐름
+----------+----------+--------+
| user_id  | product  | amount |
+----------+----------+--------+   배치 1
| u001     | laptop   |  1200  |
| u002     | mouse    |    30  |
+----------+----------+--------+   배치 2
| u001     | keyboard |    80  |
| u003     | monitor  |   400  |
+----------+----------+--------+
         |
         | groupBy("user_id").sum("amount")
         v
+----------------------------------+
|          StateStore               |
|  배치 1:  u001=1200, u002=30      |
|  배치 2:  u001=1280(누적), u003=400|
+----------------------------------+
         |
         | outputMode("update") -> 변경된 행만 싱크로 전송
         v
+----------------------------------+
|  u001 | 1280  (변경됨 -> 출력)    |
|  u003 |  400  (신규 -> 출력)      |
|  u002 |   30  (미변경 -> 출력 안 됨)|
+----------------------------------+
```

```python
from pyspark.sql.functions import sum, count, from_json, col

orders = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "orders")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), order_schema).alias("data"))
    .select("data.*")
)

sales_agg = orders.groupBy("product_id").agg(
    sum("amount").alias("total_sales"),
    count("*").alias("order_count")
)

sales_agg.writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("checkpointLocation", "/delta/checkpoints/sales_agg") \
    .option("path", "/delta/sales_summary") \
    .start()
```

> **실무 주의** — 유저 ID, 세션 ID처럼 카디널리티가 높은 키를 시간 제한 없이 groupBy하면
> StateStore가 무한 증가. 이 경우 윈도우 집계로 전환하거나 임의 상태 연산(mapGroupsWithState)을 사용.

---

### 1-2. 이벤트 타임 윈도우에 의한 집계

- **목적** — 이벤트 발생 시각 기준의 시간 구간별 집계 (처리 시간이 아닌 실제 발생 시간 기준)
- **이유** — 네트워크 지연, 재처리 등으로 이벤트가 늦게 도착해도 올바른 시간 버킷에 집계
- **결과** — 지연 도착 이벤트까지 허용 범위 내에서 정확하게 반영, 워터마크 이후 상태 자동 정리

#### 윈도우 종류

```
텀블링 윈도우 (Tumbling Window)
- 고정 크기, 겹침 없음
- 각 이벤트는 정확히 하나의 윈도우에 속함

  |----10분----|----10분----|----10분----|
  * * *  * *   * * *   *    * *  *
  [  W1  집계 ] [  W2  집계 ] [  W3  집계 ]


슬라이딩 윈도우 (Sliding Window)
- 고정 크기이지만 일정 간격으로 이동
- 하나의 이벤트가 여러 윈도우에 중복 집계됨
- window(event_time, "10 minutes", "5 minutes")

  |------W1: 00~10------|
            |------W2: 05~15------|
                      |------W3: 10~20------|
  *  *    *    *    *    *
  (각 이벤트가 최대 2개 윈도우에 포함)
```

#### 워터마크(Watermark)

- **목적** — 지연 도착 이벤트 허용 범위 지정 + StateStore 정리 기준선 제공
- **이유** — 워터마크 없으면 Spark는 "언제까지나 늦게 오는 이벤트가 있을 수 있다"고 가정해 state를 절대 삭제하지 않음
- **결과** — 워터마크 기준선 이전 데이터 드롭 + 해당 윈도우 state 해제

```
이벤트 타임 흐름
--------------------------------------------------------------
  12:00  12:05  12:10  12:15  12:20 (최대 이벤트타임)
    *      *      *             *   <- 12:10에 발생했으나 12:20에 도착 (10분 지연)

withWatermark("event_time", "10 minutes") 설정 시:

  워터마크 기준선 = max_event_time - threshold
                 = 12:20 - 10분 = 12:10

  * 12:10 이벤트  ->  기준선과 같음, 처리 허용
  * 12:09 이벤트  ->  기준선 미만, 드롭 (늦은 데이터로 간주)
  * 12:10 이전 윈도우 상태  ->  StateStore에서 삭제 가능

출력 모드별 동작:
  update  모드  ->  윈도우 결과가 갱신될 때마다 출력, 기준선 초과 시 state 삭제
  append  모드  ->  워터마크가 윈도우 종료 시각을 지난 후에만 결과 출력 (늦은 데이터 보장)
  complete 모드 ->  매 배치마다 전체 윈도우 테이블 출력, state 절대 삭제 안 됨 (비권장)
```

```python
from pyspark.sql.functions import window, sum, count, col

# 텀블링 윈도우 - 5분 단위 집계
tx_agg = (
    transactions
    .withWatermark("tx_time", "10 minutes")        # 최대 10분 지연 허용
    .groupBy(
        window(col("tx_time"), "5 minutes"),        # 텀블링: 슬라이드 생략
        col("user_id")
    )
    .agg(count("*").alias("tx_count"), sum("amount").alias("total"))
)

# 이상 거래 탐지: 5분 내 10회 이상 결제
alerts = tx_agg.filter(col("tx_count") >= 10)

alerts.writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("checkpointLocation", "/delta/checkpoints/fraud_alerts") \
    .start()
```

> **append vs update 선택 기준**
>
> - 다운스트림이 upsert를 지원하면 `update` (더 빠른 결과)
> - "확정된 결과만" 내보내야 하면 `append` + 워터마크 (늦은 데이터 재처리 방지)

---

## 2. 스트리밍 조인

### 2-1. 스트림-정적 데이터 조인

- **목적** — 실시간 스트림 데이터에 변경이 드문 참조 데이터(상품 정보, 사용자 메타데이터 등)를 결합
- **이유** — 정적 DF는 마이크로 배치마다 재사용되므로 별도 상태 버퍼 불필요, StateStore 부담 없음
- **결과** — 각 마이크로 배치에서 스트리밍 데이터를 정적 DF와 일반 조인처럼 처리

```
마이크로 배치 처리 구조
+--------------------+        +----------------------+
| 스트리밍 DF         |        | 정적 DF              |
| (Kafka 실시간 주문) |  JOIN  | (Delta / Parquet)    |
|                    | -----> | 배치마다 재사용        |
| order_id           |        | product_id           |
| product_id         |        | product_name         |
| amount             |        | category             |
+--------------------+        +----------------------+
         |
         v
+---------------------------------------------+
| order_id | product_name | category | amount  |
+---------------------------------------------+
```

**지원하는 조인 타입**

| 조인 타입 | 스트림 위치 | 지원 여부 |
|---------|-----------|---------|
| inner | 스트림이 왼쪽/오른쪽 모두 | 지원 |
| left outer | 스트림이 왼쪽 | 지원 |
| right outer | 스트림이 오른쪽 | 지원 |
| left outer | 스트림이 오른쪽 | 미지원 |

```python
# 정적 상품 테이블 (쿼리 시작 시 1회 로드, 배치마다 재사용)
products_static = spark.read.format("delta").load("/delta/products")

enriched_orders = orders.join(
    products_static,
    orders.product_id == products_static.product_id,
    "left"
).select(
    orders.order_id,
    products_static.product_name,
    products_static.category,
    orders.amount
)

enriched_orders.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "/delta/checkpoints/enriched_orders") \
    .start()
```

> **실무 주의**
>
> - 정적 DF는 스트리밍 쿼리가 시작될 때 한 번만 로드됨
> - 참조 데이터가 자주 바뀐다면 쿼리를 재시작하거나, 조회 로직을 UDF로 이동해 매 배치마다 최신 데이터를 읽도록 설계
> - 데이터가 크다면 `broadcast(products_static)`로 모든 executor에 복제해 셔플 제거

---

### 2-2. 스트림-스트림 조인

- **목적** — 두 실시간 스트림의 이벤트를 키와 시간 조건으로 매칭 (광고 노출-클릭, 주문-결제 연결 등)
- **이유** — 양쪽 스트림 모두 지연 도착 가능성이 있어, 매칭되지 않은 이벤트를 StateStore 버퍼에 임시 보관
- **결과** — 양쪽 워터마크 + 시간 범위 조건으로 버퍼 크기를 제어하고 매칭 윈도우 경과 후 state 정리

```
스트림-스트림 조인 내부 동작
--------------------------------------------------------------
스트림 A (광고 노출)             스트림 B (광고 클릭)
  ad_id | show_time               ad_id | click_time
  A001  | 12:00:01                A001  | 12:00:35   <- 34초 후 클릭
  A002  | 12:00:10                A003  | 12:01:05
  A003  | 12:00:50

           +----------------------------------+
           |           StateStore              |
           |  A 버퍼: 미매칭 노출 이벤트 보관   |
           |  B 버퍼: 미매칭 클릭 이벤트 보관   |
           |  워터마크 초과 이벤트 -> 드롭       |
           +----------------------------------+
                        |
                        | 조인 조건 평가
                        v
           A001 매칭 성공 (show:12:00:01 + click:12:00:35)
           A002 미매칭 (클릭 없음, 워터마크 경과 후 드롭)

버퍼 최대 크기 공식 (inner join 기준):
  A 버퍼 보존 기간 = B 워터마크 지연 + 시간 범위 조건 상한
  B 버퍼 보존 기간 = A 워터마크 지연 + 시간 범위 조건 상한
```

**지원 조인 타입과 워터마크 요건**

| 조인 타입 | 워터마크 필요 여부 | 시간 범위 조건 필요 여부 | 비고 |
|---------|----------------|---------------------|-----|
| inner | 선택 (권장) | 선택 (권장) | 없으면 state 무한 증가 |
| left outer | 필수 | 필수 | 워터마크 없으면 실행 오류 발생 |
| right outer | 필수 | 필수 | 동일 |
| full outer | 미지원 | - | Spark 미지원 |

```python
from pyspark.sql.functions import expr

impressions = spark.readStream...   # 광고 노출 스트림
clicks = spark.readStream...        # 광고 클릭 스트림

# 양쪽 모두 워터마크 설정 필수
impressions_wm = impressions.withWatermark("show_time", "2 hours")
clicks_wm = clicks.withWatermark("click_time", "3 hours")

# 조인: 클릭은 노출 후 0~1시간 이내만 유효 (시간 범위 조건 필수)
joined = impressions_wm.join(
    clicks_wm,
    expr("""
        impression_id = click_id
        AND click_time >= show_time
        AND click_time <= show_time + interval 1 hour
    """),
    "leftOuter"
)
```

> **버퍼 크기 예측**
>
> - impressions 버퍼 보존 = clicks 워터마크(3h) + 조인 범위(1h) = 최대 4시간치 노출 데이터 보관
> - clicks 버퍼 보존 = impressions 워터마크(2h) = 최대 2시간치 클릭 데이터 보관
> - 이벤트 발생량이 높다면 메모리 사용량이 커질 수 있으므로 워터마크와 범위 조건을 최소화

---

## 3. 임의의 상태 정보 유지 연산

- **목적** — 내장 집계/조인으로 표현 불가능한 복잡한 비즈니스 로직을 직접 상태를 관리하며 구현
- **이유** — 세션 추적, 상태 머신, 복잡한 이벤트 시퀀스 감지 등은 단순 groupBy로 불가능
- **결과** — 그룹별 완전한 상태 제어 (생성/갱신/삭제)와 타임아웃 지원

이 연산들을 **비관리(unmanaged) 상태 연산**이라 부름 — 상태 정리를 사용자가 직접 책임짐.

```
API 비교
==========================================================================
                      mapGroupsWithState     flatMapGroupsWithState
--------------------------------------------------------------------------
반환 개수              정확히 1개             0개 이상 (Iterator)
중간 이벤트 방출        불가                   가능
타임아웃 시 결과 출력    불가 (상태만 정리)      가능 (출력 생성 후 상태 삭제)
지원 출력 모드          update 전용            update / append 모두 지원
주 사용 사례           실시간 현황 갱신         세션 종료 이벤트, 알림 발행
==========================================================================
```

> **PySpark API 안내**
>
> `mapGroupsWithState` / `flatMapGroupsWithState`는 Scala/Java의 typed Dataset API.
> PySpark에서는 Spark 3.4+부터 `applyInPandasWithState`로 동일한 패턴 구현.
> 두 API의 차이(1건 반환 vs 0건 이상 반환)는 yield하는 DataFrame 행 수로 동일하게 표현.

### GroupState API 주요 메서드 (PySpark 기준)

| 메서드 | 역할 |
|-------|-----|
| `state.exists` | 해당 그룹의 상태가 존재하는지 확인 (bool) |
| `state.get` | 현재 상태를 pandas DataFrame(1행)으로 반환 |
| `state.update(pandas_df)` | 상태 갱신 (state schema에 맞는 pandas DataFrame 전달) |
| `state.remove()` | 상태 삭제 (StateStore에서 제거) |
| `state.hasTimedOut` | 타임아웃으로 함수가 호출됐는지 여부 (bool) |
| `state.setTimeoutDuration("30m")` | ProcessingTime 타임아웃 설정 (문자열 또는 ms 정수) |
| `state.setTimeoutTimestamp(ms)` | EventTime 타임아웃 설정 (epoch milliseconds) |

---

### 3-1. mapGroupsWithState()

- **목적** — 그룹별 상태를 커스텀 로직으로 갱신하고, 매 배치마다 현재 상태 기반 결과 1건 출력
- **이유** — 내장 집계로 표현 불가능한 누적 로직 (예: 세션 내 이벤트 시퀀스 추적)
- **결과** — 각 배치에서 상태 함수가 호출되며 결과는 update 모드로 출력

```
세션 추적 흐름 예시
--------------------------------------------------------------
배치 1:
  u001, login,     12:00  ->  State: { start:12:00, pv:0, purchased:false }
  u001, page_view, 12:01  ->  State: { start:12:00, pv:1, purchased:false }

배치 2:
  u001, purchase,  12:05  ->  State: { start:12:00, pv:1, purchased:true  }
  u001, logout,    12:10  ->  State: { start:12:00, pv:1, purchased:true, end:12:10 }

출력 (매 배치 1건):
  배치 1: { user=u001, duration=1min,  purchased=false }
  배치 2: { user=u001, duration=10min, purchased=true  }
```

```python
# PySpark 3.4+ - applyInPandasWithState 사용
import time
import pandas as pd
from typing import Iterator, Tuple
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

# 상태 스키마 - StateStore에 저장할 데이터 구조
state_schema = StructType([
    StructField("session_start_ts", LongType()),
    StructField("page_view_count", LongType()),
    StructField("has_purchased", BooleanType()),
])

# 출력 스키마 - 매 배치마다 방출할 결과 구조
output_schema = StructType([
    StructField("user_id", StringType()),
    StructField("status", StringType()),
    StructField("page_view_count", LongType()),
    StructField("has_purchased", BooleanType()),
])

def update_session(
    key: Tuple,
    pdf_iter: Iterator[pd.DataFrame],
    state: GroupState
) -> Iterator[pd.DataFrame]:
    """
    mapGroupsWithState 패턴: 항상 1건 반환 (update 모드)
    - 타임아웃 시: 상태 삭제 후 종료 결과 1건 yield
    - 일반 배치 시: 상태 갱신 후 현재 상태 결과 1건 yield
    """
    user_id = key[0]

    if state.hasTimedOut:
        s = state.get   # pandas DataFrame (1행)
        state.remove()
        yield pd.DataFrame({
            "user_id": [user_id],
            "status": ["timeout_closed"],
            "page_view_count": [int(s["page_view_count"].iloc[0])],
            "has_purchased": [bool(s["has_purchased"].iloc[0])],
        })
        return

    # 기존 상태 로드 (없으면 초기값)
    if state.exists:
        s = state.get
        pv_count = int(s["page_view_count"].iloc[0])
        has_purchased = bool(s["has_purchased"].iloc[0])
        session_start = int(s["session_start_ts"].iloc[0])
    else:
        pv_count = 0
        has_purchased = False
        session_start = int(time.time() * 1000)

    # 현재 배치 이벤트 처리
    for pdf in pdf_iter:
        pv_count += len(pdf)
        if "purchase" in pdf["event_type"].values:
            has_purchased = True

    # 상태 갱신 + 타임아웃 설정 (30분 비활성 시 타임아웃)
    state.update(pd.DataFrame({
        "session_start_ts": [session_start],
        "page_view_count": [pv_count],
        "has_purchased": [has_purchased],
    }))
    state.setTimeoutDuration("30 minutes")

    # 항상 1건 반환 (mapGroupsWithState 패턴)
    yield pd.DataFrame({
        "user_id": [user_id],
        "status": ["active"],
        "page_view_count": [pv_count],
        "has_purchased": [has_purchased],
    })

result = (
    events_df
    .groupBy("user_id")
    .applyInPandasWithState(
        update_session,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="update",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )
)
```

---

### 3-2. 타임아웃을 통한 비활성 그룹 관리

- **목적** — 더 이상 이벤트가 오지 않는 그룹의 상태를 자동으로 정리해 StateStore 메모리 방지
- **이유** — unmanaged 연산이므로 Spark가 자동 정리하지 않음. 방치 시 state 무한 증가
- **결과** — 타임아웃 발생 시 함수가 `hasTimedOut=true`로 재호출되며, 이 시점에 상태 정리 로직 실행

```
타임아웃 종류
==========================================================================
ProcessingTimeTimeout                EventTimeTimeout
--------------------------------------------------------------------------
기준 시각   처리 시각 (시스템 클록)     이벤트 타임 (워터마크 기준)
설정 방법   setTimeoutDuration("30m")  setTimeoutTimestamp(epochMs)
워터마크    불필요                      필수
보장 수준   approximate (배치 지연 영향) 이벤트 타임 기준 정확
사용 사례   단순 비활성 감지             이벤트 타임 기반 세션 만료
==========================================================================

타임아웃 발생 흐름:
  마지막 이벤트 후 30분 경과 (ProcessingTimeTimeout 기준)
      |
      v
  다음 마이크로 배치에서 해당 그룹 함수 재호출
  state.hasTimedOut == true
      |
      v
  사용자 정의 정리 로직 실행
  state.remove()  ->  StateStore에서 상태 제거
```

> **주의** — 타임아웃은 배치가 실행될 때 체크됨. 배치 간격이 길면 타임아웃 감지도 늦어질 수 있음.
> 정확한 이벤트 타임 기반 만료가 필요하면 EventTimeTimeout + 워터마크 조합 사용.

---

### 3-3. flatMapGroupsWithState() 일반화

- **목적** — `mapGroupsWithState`의 확장판. 타임아웃 시에만 결과 출력하거나, 배치당 여러 이벤트 방출
- **이유** — "세션 종료 이벤트만 append 모드로 내보내는" 패턴은 `mapGroupsWithState`로 불가
- **결과** — 0개 이상의 결과를 Iterator로 반환. append 출력 모드와 함께 사용 가능

```
패턴별 활용 흐름
--------------------------------------------------------------
패턴 1: 중간 이벤트도 방출 (실시간 알림)
  이벤트 수신  ->  상태 갱신  ->  조건 충족 시 알림 이벤트 즉시 방출
  타임아웃    ->  상태 정리  ->  종료 이벤트 방출

패턴 2: 세션 종료 시에만 결과 방출 (append 모드 DW 적재)
  이벤트 수신  ->  상태 갱신  ->  yield 없음 (이번 배치 출력 0건)
  타임아웃    ->  상태 정리  ->  [최종 세션 집계 결과] yield 후 싱크 적재
--------------------------------------------------------------
```

```python
# PySpark 3.4+ - applyInPandasWithState 사용
from pyspark.sql.types import FloatType

state_schema_v2 = StructType([
    StructField("total_spend", FloatType()),
    StructField("page_view_count", LongType()),
])

output_schema_v2 = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("total_spend", FloatType()),
    StructField("page_view_count", LongType()),
])

def process_session(
    key: Tuple,
    pdf_iter: Iterator[pd.DataFrame],
    state: GroupState
) -> Iterator[pd.DataFrame]:
    """
    flatMapGroupsWithState 패턴: 0건 이상 반환 (append 모드)
    - 타임아웃 시: 세션 종료 이벤트 1건 yield -> DW 최종 적재
    - 일반 배치 시: 조건 충족 시에만 알림 yield, 아니면 아무것도 안 함
    """
    user_id = key[0]

    if state.hasTimedOut:
        s = state.get
        state.remove()
        # 세션 종료 이벤트 1건 방출 (append 모드 -> DW 최종 적재)
        yield pd.DataFrame({
            "user_id": [user_id],
            "event_type": ["session_end"],
            "total_spend": [float(s["total_spend"].iloc[0])],
            "page_view_count": [int(s["page_view_count"].iloc[0])],
        })
        return

    # 기존 상태 로드
    if state.exists:
        s = state.get
        total_spend = float(s["total_spend"].iloc[0])
        pv_count = int(s["page_view_count"].iloc[0])
    else:
        total_spend = 0.0
        pv_count = 0

    # 현재 배치 이벤트 처리
    for pdf in pdf_iter:
        total_spend += float(pdf["amount"].sum())
        pv_count += len(pdf)

    state.update(pd.DataFrame({
        "total_spend": [total_spend],
        "page_view_count": [pv_count],
    }))
    state.setTimeoutDuration("1 hour")

    # 고액 지출 조건 충족 시에만 알림 이벤트 방출 (0건 또는 1건)
    if total_spend > 10000:
        yield pd.DataFrame({
            "user_id": [user_id],
            "event_type": ["high_value_alert"],
            "total_spend": [total_spend],
            "page_view_count": [pv_count],
        })
    # else: yield 없음 = 이번 배치 출력 0건 (flatMapGroupsWithState 패턴)

result = (
    events_df
    .groupBy("user_id")
    .applyInPandasWithState(
        process_session,
        outputStructType=output_schema_v2,
        stateStructType=state_schema_v2,
        outputMode="append",      # 세션 종료 시에만 출력 -> append 모드
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )
)
```

---

## 4. 성능 튜닝

### 튜닝 포인트 맵

```
성능 병목 요인과 해결 방향
==========================================================================

[1] 셔플 파티션 수
    기본값 200은 스트리밍에 과다 -> 태스크 오버헤드 증가
    권장: spark.sql.shuffle.partitions = 코어 수 x 2~4
    예: 4 workers x 4 cores = 16코어 -> 32~64 설정

[2] StateStore 크기
    워터마크/타임아웃 기간 = state 보존 기간
    -> 워터마크와 타임아웃을 비즈니스 요구 최소한으로 설정
    -> RocksDB StateStore 사용 시 메모리 오버플로우를 디스크로 처리

[3] 트리거 간격
    기본(가능한 빠르게)은 마이크로 배치를 연속 실행 -> 드라이버 부하
    -> processingTime 트리거로 간격 제어

[4] 소스 처리율 제한 (역압 제어)
    Kafka: maxOffsetsPerTrigger 설정으로 배치당 읽는 오프셋 수 제한
    파일 소스: maxFilesPerTrigger 설정

[5] 체크포인트 I/O
    로컬 디스크 -> 느리고 복구 불가
    -> S3 / HDFS / ADLS 사용, 빠른 스토리지일수록 배치 지연 감소

[6] 다중 스트리밍 쿼리
    동일 SparkSession에서 여러 스트리밍 쿼리 실행 시
    -> 쿼리 간 리소스 경합 발생
    -> 리소스가 허용된다면 SparkSession 분리 고려
==========================================================================
```

### 핵심 설정값

```python
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "16") \
    .config(
        "spark.sql.streaming.stateStore.providerClass",
        # RocksDB: 대용량 state에서 메모리 효율 개선 (Spark 3.2+)
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    ) \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .getOrCreate()

query = result.writeStream \
    .trigger(processingTime="30 seconds") \              # 30초 간격 실행
    .option("checkpointLocation", "s3://bucket/cp/") \
    .option("maxOffsetsPerTrigger", "50000") \           # Kafka 역압 제어
    .outputMode("update") \
    .format("delta") \
    .start()
```

### 트리거 종류

| 트리거 | 동작 | 사용 시점 |
|-------|-----|---------|
| `processingTime="30 seconds"` | 30초마다 배치 실행 | 일반적인 스트리밍 처리 |
| 기본값 (트리거 미설정) | 이전 배치 완료 즉시 다음 배치 실행 | 최저 지연 요구 시 |
| `once=True` | 현재 데이터 처리 후 쿼리 종료 | 배치 대체, 테스트 |
| `availableNow=True` | 현재 사용 가능한 전체 데이터 처리 후 종료 | 증분 배치 처리 |
| `continuous="1 second"` | 밀리초 수준 저지연 (실험적 기능) | 집계 없는 단순 변환만 |

### 스트리밍 쿼리 모니터링

```python
# 현재 쿼리 상태
query.status
# {"message": "Processing new data", "isDataAvailable": True, "isTriggerActive": True}

# 최근 배치 진행 정보
query.lastProgress
# {
#   "batchId": 42,
#   "inputRowsPerSecond": 1200.0,
#   "processedRowsPerSecond": 980.5,
#   "durationMs": { "triggerExecution": 1250, "getBatch": 80, ... },
#   "stateOperators": [
#     { "numRowsTotal": 85000, "numRowsUpdated": 1200, "memoryUsedBytes": 52428800 }
#   ]
# }

# StateStore 메모리 누수 감지 패턴
for prog in query.recentProgress:
    for op in prog.get("stateOperators", []):
        print(f"state rows: {op['numRowsTotal']:,}, memory: {op['memoryUsedBytes'] / 1024**2:.1f} MB")
# numRowsTotal이 지속 증가 -> 워터마크 미설정 or 타임아웃 로직 미작동 의심
```

---

## 요약 - 상황별 API 선택

```
요구사항 -> API 선택
==========================================================================

단순 집계 (합계, 카운트, 평균)?
  |
  +-- 시간 구간 불필요, 전체 누적        ->  groupBy().agg()  (update 모드)
  |
  +-- 이벤트 타임 기반 시간 구간 집계    ->  withWatermark() + window()
      |
      +-- 결과를 매 배치 갱신            ->  update 모드
      +-- 확정된 결과만 append           ->  append 모드 (워터마크 경과 후 출력)

외부 데이터와 조인?
  |
  +-- 참조 데이터가 정적 (변경 드묾)    ->  스트림-정적 조인 (broadcast 고려)
  |
  +-- 양쪽 모두 실시간                 ->  스트림-스트림 조인
      조건: 양쪽 워터마크 + 시간 범위 조건 필수 (outer join 시)

복잡한 비즈니스 로직 (세션, 상태 머신, 이벤트 시퀀스)?
  |
  +-- 매 배치마다 현재 상태 출력 (1건)  ->  applyInPandasWithState (update 모드)
  |
  +-- 세션 종료 시에만 결과 출력        ->  applyInPandasWithState (append 모드)
  |
  +-- 조건 충족 시 즉시 알림 + 다건 출력 ->  applyInPandasWithState (update 모드)

==========================================================================
```
