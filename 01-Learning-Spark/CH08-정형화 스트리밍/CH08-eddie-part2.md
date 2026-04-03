# 01.데이터 트랜스포메이션
- Structured Streaming 분류
	- Stateful(상태정보유지)
	- Stateless(무상태)

## (1)상태값 
### a.처리 과정
1. **[Catalyst Optimizer]** DataFrame 연산을 받아 최적화된 논리 계획(Logical Plan)으로 변환
- 예) `df.groupBy("store_id").count()` → "store_id로 묶어서 count하라"는 논리 계획 생성

2. **[Spark SQL Planner]** 논리 계획을 받아 "이게 스트리밍 소스에서 오는 쿼리인가?" 판단
    - 예) 소스가 Kafka면 스트리밍, CSV 파일이면 배치로 판단
    
3. **[Spark SQL Planner]** 스트리밍 쿼리라면 일회성 물리 실행 계획 대신, 마이크로배치마다 실행할 물리 실행 계획 묶음 생성
    - 예) 배치: 실행 계획 1개 → 끝 / 스트리밍: 마이크로배치 1용, 2용, 3용... 계속 생성
    
4. **[각 마이크로배치 실행 계획]** 입력 스트림에서 새로 들어온 데이터 조각만 읽어서 처리
    - 예) 마이크로배치 2는 t=10~20초 사이 데이터만 읽음, t=0~10초 데이터 재처리 안 함
    
5. **[각 마이크로배치 실행 계획]** stateful 연산(집계 등)의 경우, StateStore에서 이전 마이크로배치의 부분 결과를 꺼내 새 데이터와 합산
    - 예) StateStore에 저장된 store_A: 3 꺼내서 새 데이터 2와 합산 → 5
    
6. **[각 마이크로배치 실행 계획]** 합산된 결과를 다시 StateStore에 저장하고, 결과 DataFrame을 누적 업데이트
    - 예) store_A: 5를 StateStore에 덮어쓰기 저장
    
7. **[Spark SQL Planner]** 다음 마이크로배치가 트리거되면 3번부터 반복

### b.상태값은 어디에 저장되는가

> StateStore에 저장하고, StateStore는 기본적으로 Executor 메모리에 올라가 있어.

**저장 위치 계층**
1. **1차 저장 - Executor 메모리**
    - 실제 연산할 때 StateStore를 메모리에 올려서 읽고 씀
    - 빠른 접근을 위해 메모리 우선
2. **2차 저장 - 체크포인트 디렉토리 (HDFS / S3)**
    - 마이크로배치가 끝날 때마다 메모리의 StateStore를 디스크에 백업(장애대응)
    - `.delta` 파일 (변경분), `.snapshot` 파일 (전체 스냅샷) 형태로 저장
    - 잡이 죽었다 살아났을 때 여기서 복구
3. 파티션 단위로 분산 저장
	- StateStore는 전체를 하나로 관리하는 게 아니라 파티션 단위로 쪼개져서 각 Executor에 분산 저장
	- 같은 키는 항상 같은 파티션으로 라우팅되어 같은 Executor의 StateStore에서 관리
	- 체크포인트도 파티션 단위로 S3/HDFS에 백업

```
Kafka 파티션 0 → Spark 파티션 0 → Executor 1의 StateStore
Kafka 파티션 1 → Spark 파티션 1 → Executor 2의 StateStore
Kafka 파티션 2 → Spark 파티션 2 → Executor 3의 StateStore
```

```
S3://checkpoint/state/
    partition=0/
        .delta
        .snapshot
    partition=1/
        .delta
        .snapshot
```

4. StateStore 메모리 부하 원인
    - 엔트리 = StateStore에 저장된 키-값 쌍 한 줄
    - 상태값 자체(숫자)가 아니라 키 × 엔트리 수만큼 메모리를 차지하는 게 문제
    - `groupBy("user_id").count()` 기준으로 유저 1명당 엔트리 1개 생성, 이벤트 추가 시 새 엔트리가 아니라 기존 엔트리 값만 업데이트

```
StateStore 내부:
user_001 → 3     ← 엔트리 1개
user_002 → 7     ← 엔트리 1개
user_003 → 12    ← 엔트리 1개
```

- DAU 1000만 서비스면 StateStore에 1000만 엔트리가 메모리에 상주
- 연산 복잡도에 따라 엔트리 하나의 크기도 증가

|연산|엔트리 구성|
|---|---|
|`count()`|키 + 숫자 1개|
|`avg()`|키 + sum + count 2개|
|`collect_list()`|키 + 리스트 전체|
|윈도우 집계|키 + 윈도우 범위 수만큼|

- watermark 미설정 시 엔트리가 만료되지 않고 무한정 쌓여 Executor OOM 발생


**흐름**
```
마이크로배치 실행
    ↓
Executor 메모리에서 StateStore 읽기/쓰기
    ↓
배치 완료 후 S3/HDFS 체크포인트에 백업
    ↓
다음 마이크로배치에서 메모리 StateStore 재사용
```


**엔지니어 독백**
> StateStore가 메모리에 있다는 게 핵심이야. DB처럼 디스크에서 매번 읽으면 마이크로배치마다 I/O 비용이 너무 커서 레이턴시를 맞출 수가 없어. 그래서 메모리 우선이고, 디스크는 장애 복구용으로만 쓰는 구조야. 근데 상태가 계속 쌓이면 Executor 메모리가 터져. 그래서 watermark랑 TTL로 오래된 상태를 주기적으로 정리해줘야 해.



## (2) Stateless 트랜스포메이션
### a.stateless 연산 종류
**stateful vs stateless 핵심 차이**
- stateless 연산은 이전 상태가 필요 없어서 StateStore를 거치지 않음 →  1~4번까지는 동일하고 5번부터 달라짐.
- stateless: 새 데이터만 보면 답이 나옴 → StateStore 불필요
- stateful: 이전 결과와 합산해야 답이 나옴 → StateStore 필수
- 대표적인 stateless 연산: `filter`, `map`, `select`, `flatMap`

|연산|예시|설명|
|---|---|---|
|`filter`|`df.filter(col("amount") > 1000)`|1000 초과 주문만 통과|
|`select`|`df.select("user_id", "amount")`|필요한 컬럼만 추출|
|`withColumn`|`df.withColumn("tax", col("amount") * 0.1)`|세금 컬럼 추가|
|`cast`|`df.withColumn("amount", col("amount").cast("double"))`|타입 변환|
|`when/otherwise`|`df.withColumn("grade", when(col("amount") > 1000, "VIP").otherwise("일반"))`|금액 기준 등급 분류|
|`from_json`|`df.withColumn("data", from_json(col("value"), schema))`|Kafka에서 온 JSON 문자열 파싱|
|`explode`|`df.withColumn("item", explode(col("items")))`|주문 내 상품 배열을 행으로 분해|
|`regexp_extract`|`df.withColumn("code", regexp_extract(col("log"), r"ERROR(\d+)", 1))`|로그에서 에러코드 추출|
|`to_timestamp`|`df.withColumn("ts", to_timestamp(col("event_time")))`|문자열 시간을 timestamp로 변환|
|`coalesce`|`df.withColumn("name", coalesce(col("name"), lit("unknown")))`|null이면 unknown으로 대체|
|`drop`|`df.drop("_corrupt_record")`|불필요한 컬럼 제거|
|`concat_ws`|`df.withColumn("full_name", concat_ws(" ", col("first"), col("last")))`|이름 합치기|
### b. outputMode(출력모드) 설정
> stateless는 outputMode 설정 시, 이전 상태가 없으니까 append만 의미있어.

```
df.filter(col("amount") > 1000) \
  .writeStream \
  .outputMode("append") \
  .format("console") \
  .start()
```

**stateless + 출력모드**

|모드|stateless 가능 여부|이유|
|---|---|---|
|`append`|가능|이번 배치 새 행만 출력하면 되니까|
|`update`|가능|변경된 행 = 이번 배치 새 행이랑 동일한 의미|
|`complete`|불가|누적 전체 결과를 저장해둔 곳이 없으니까|
- **실무에서 stateless는 append 고정**
- `update`가 기술적으로 가능하긴 한데, stateless에서 `update`랑 `append`는 결과가 동일
- `complete` 쓰려고 하면 Spark가 바로 에러 발생

> 실무에서 stateless는 Kafka에서 받은 raw 이벤트 파싱하고 필터링해서 S3나 다른 Kafka 토픽으로 내보내는 용도로 많이 써. 이 경우 항상 `append`야. 새로 들어온 이벤트만 내보내면 되니까 다른 모드 고민할 필요 자체가 없어.



## (3) Stateful 트랜스포메이션
> Stateful 트랜스포메이션은 이전 마이크로배치 결과를 StateStore에 유지하면서 새 데이터와 합산하는 연산이야.


**1. 주요 연산**

|연산|설명|
|---|---|
|`groupBy().count()`|키 기준 누적 카운트|
|`groupBy().agg()`|키 기준 집계 (sum, avg 등)|
|`window().groupBy()`|시간 윈도우 기준 집계|
|`dropDuplicates()`|중복 제거 (이전 배치 기억 필요)|
|스트림-스트림 `join`|양쪽 스트림 매칭될 때까지 버퍼링|



**2. Output 모드 설정**

|모드|사용 가능 여부|주 사용처|
|---|---|---|
|`append`|가능 (watermark 필수)|윈도우 집계 결과 적재|
|`update`|가능|실시간 집계 DB upsert|
|`complete`|가능|전체 랭킹, 전체 집계|

```
df.groupBy("store_id").count() \
  .writeStream \
  .outputMode("update") \
  .format("console") \
  .start()
```



**3. Spark 처리 과정**

`groupBy("store_id").count()` 예시

```
[마이크로배치 1] 새 데이터: store_A 3건, store_B 2건
    ↓
Catalyst → 논리 계획 생성
    ↓
Planner → 스트리밍 쿼리 판단 → 실행 계획 생성
    ↓
파티션별 Executor에서 처리
    ↓
StateStore에서 기존 엔트리 조회
  store_A → 없음 (첫 배치)
  store_B → 없음 (첫 배치)
    ↓
새 데이터 합산 후 StateStore 업데이트
  store_A → 3
  store_B → 2
    ↓
Sink 출력: store_A: 3, store_B: 2

[마이크로배치 2] 새 데이터: store_A 2건, store_C 1건
    ↓
StateStore에서 기존 엔트리 조회
  store_A → 3  (이전 배치 결과)
  store_C → 없음 (첫 등장)
    ↓
새 데이터 합산 후 StateStore 업데이트
  store_A → 5  (3 + 2)
  store_B → 2  (변경 없음)
  store_C → 1
    ↓
Sink 출력 (update 모드): store_A: 5, store_C: 1  ← 변경된 것만
```

---

**4. State값 관리 - Executor 메모리**

- Executor 메모리에 파티션 단위로 분산 보관
- 같은 키는 항상 같은 파티션 → 같은 Executor의 StateStore에서 관리

```
Executor 1 메모리 (partition=0 담당):
  store_A → 5
  store_C → 1

Executor 2 메모리 (partition=1 담당):
  store_B → 2
```

---

**5. State값 관리 - 체크포인트 기록 방식**

마이크로배치가 완료될 때마다 두 가지 파일로 S3/HDFS에 백업

- `.delta` : 해당 배치에서 변경된 엔트리만 기록 (증분)
- `.snapshot` : 일정 배치 주기마다 StateStore 전체를 스냅샷으로 기록

```
S3://checkpoint/state/0/   ← 쿼리 0번
  0/                       ← partition=0
    1.delta                ← 마이크로배치 1 변경분
    2.delta                ← 마이크로배치 2 변경분
    2.snapshot             ← 마이크로배치 2 전체 스냅샷
    3.delta
    4.delta
    4.snapshot
  1/                       ← partition=1
    1.delta
    2.delta
    ...
```

- 복구 시: 가장 최근 `.snapshot` 로드 → 그 이후 `.delta` 순서대로 재적용
- `.snapshot` 없으면 처음부터 `.delta` 전부 재적용 (느림)

---

**6. 체크포인트 확인 방법**

체크포인트 경로 설정 후 실행

```
df.groupBy("store_id").count() \
  .writeStream \
  .outputMode("update") \
  .option("checkpointLocation", "/tmp/checkpoint") \
  .format("console") \
  .start()
```

로컬에서 디렉토리 구조 확인

```
find /tmp/checkpoint -type f
```

실제 delta 파일 내용 확인 (바이너리라 직접 읽기 어려움)

```
ls -lh /tmp/checkpoint/state/0/0/
```

Spark UI에서 확인 (가장 실용적)

- `http://localhost:4040` 접속
- Structured Streaming 탭 → 쿼리 선택
- `stateOperators` 항목에서 현재 StateStore 엔트리 수, 메모리 사용량 확인 가능

---

**엔지니어 독백**

> 체크포인트 파일 직접 열어봐야 바이너리라 읽을 수 없어. 실무에서 StateStore 상태 확인할 때는 Spark UI의 Streaming 탭이 제일 실용적이야. `numRowsTotal`로 현재 StateStore에 엔트리 몇 개 쌓였는지 바로 보여줘.

> `.delta`랑 `.snapshot` 구조가 중요한 이유는 복구 속도 때문이야. `.snapshot` 없이 `.delta`만 100개 쌓여있으면 복구할 때 100개를 순서대로 다 재적용해야 해서 느려. 그래서 Spark가 일정 주기마다 자동으로 `.snapshot` 찍어두는 거야.

> Stateful 파이프라인 처음 프로덕션 올릴 때 watermark 꼭 설정해야 해. 안 걸면 엔트리가 무한 누적돼서 며칠 뒤 Executor OOM으로 잡 죽어. 로컬 테스트에서 안 터진다고 안심하면 안 돼.


**7. StateStore 정리 방식**

**메모리 정리**

- 자동 관리 - 개발자가 직접 건드릴 필요 없음
- 단, watermark를 설정해야 자동 정리가 작동함

```
df.withWatermark("event_time", "10 minutes") \
  .groupBy(window("event_time", "5 minutes"), "store_id") \
  .count()
```

- watermark 설정 시: event_time 기준으로 10분 지난 엔트리는 Spark가 자동으로 메모리에서 제거
- watermark 미설정 시: 엔트리가 만료되지 않고 무한 누적 → Executor OOM

```
watermark = "10 minutes" 설정 시:

현재 이벤트 최대 시각: 12:00
삭제 기준선: 11:50

11:50 이전 엔트리 → 자동 삭제
11:50 이후 엔트리 → 유지
```

**체크포인트 정리**

- 자동 관리 - Spark가 오래된 `.delta` / `.snapshot` 파일 자동 삭제
- `spark.sql.streaming.minBatchesToRetain` 설정값 기준으로 최근 N개 배치만 보관 (기본값 100)

```
S3://checkpoint/state/0/0/
  1.delta   ← 100개 초과 시 자동 삭제
  2.delta   ← 자동 삭제
  ...
  98.delta  ← 유지
  99.delta  ← 유지
  100.snapshot ← 유지
  101.delta ← 유지
```

- 왜 전부 안 지우냐: 잡 죽었을 때 최근 N개 배치로 복구해야 하니까 일정량은 반드시 보관해야 함
- 왜 무한정 안 쌓냐: S3/HDFS 스토리지 비용 때문에 오래된 건 자동 삭제



**8. 관리형 vs 비관리형 Stateful 연산**

**핵심 차이**

| 구분          | 관리형                                   | 비관리형                                           |
| ----------- | ------------------------------------- | ---------------------------------------------- |
| State 관리 주체 | Spark가 자동 관리                          | 개발자가 직접 관리                                     |
| 만료 처리       | watermark로 자동 삭제                      | 개발자가 직접 삭제 로직 작성                               |
| 유연성         | 제한적                                   | 완전 자유                                          |
| 난이도         | 낮음                                    | 높음                                             |
| 대표 연산       | `groupBy`, `window`, `dropDuplicates` | `flatMapGroupsWithState`, `mapGroupsWithState` |

---

**관리형 - groupBy().count() 예시**

Spark가 StateStore 생성/업데이트/삭제를 전부 자동 처리

```
df.withWatermark("event_time", "10 minutes") \
  .groupBy("store_id") \
  .count()
```

```
내부 동작:
마이크로배치 1:
  store_A → 3   ← Spark가 알아서 StateStore에 저장

마이크로배치 2:
  store_A → 5   ← Spark가 알아서 업데이트

watermark 만료:
  store_A 엔트리 → Spark가 알아서 삭제
```

- 개발자가 할 일: watermark 설정만 해주면 끝
- 나머지는 Spark가 전부 처리

---

**비관리형 - flatMapGroupsWithState 예시**

세션 추적 파이프라인 - 유저가 30분 이상 이벤트 없으면 세션 종료

```
def update_session(user_id, events, state):
    if state.hasTimedOut:
        # 30분 이상 이벤트 없음 → 세션 종료 처리
        session = state.get
        state.remove()              # 개발자가 직접 삭제
        yield (user_id, "session_end", session["start_time"])
    else:
        # 새 이벤트 들어옴 → 세션 업데이트
        if not state.exists:
            state.update({"start_time": current_time})  # 개발자가 직접 생성
        state.setTimeoutDuration("30 minutes")          # 개발자가 직접 타임아웃 설정
        yield (user_id, "active", state.get["start_time"])

df.groupBy("user_id") \
  .flatMapGroupsWithState(
      outputMode=OutputMode.Append(),
      timeoutConf=GroupStateTimeout.ProcessingTimeTimeout()
  )(update_session)
```

```
내부 동작:
유저 이벤트 들어옴:
  → 개발자가 작성한 update_session 함수 호출
  → 함수 안에서 state.update() 직접 호출해야 저장됨
  → state.remove() 직접 호출해야 삭제됨
  → setTimeoutDuration() 직접 설정해야 만료됨
```

- 개발자가 할 일: 상태 생성 / 업데이트 / 삭제 / 만료 전부 직접 작성

---

**언제 뭘 쓰나**

- 단순 집계 (count, sum, avg) → 관리형
- 복잡한 비즈니스 로직이 필요한 경우 → 비관리형
    - 세션 추적 (일정 시간 이상 이벤트 없으면 세션 종료)
    - 이상 탐지 (유저별 패턴 직접 추적)
    - 커스텀 만료 조건 (단순 시간이 아닌 조건 기반 삭제)

---

**엔지니어 독백**

> 실무에서 비관리형은 최대한 안 써. 상태 관리 로직을 직접 짜야 하니까 버그 포인트가 많아. `state.remove()` 빠뜨리면 메모리 누수 생기고, 타임아웃 설정 잘못하면 좀비 세션이 StateStore에 계속 쌓여.

> 그래도 세션 분석이나 복잡한 이벤트 시퀀스 추적처럼 관리형으로 못 짜는 경우엔 어쩔 수 없이 써. 이럴 때는 반드시 `state.remove()` 조건 꼼꼼히 검토하고, Spark UI에서 StateStore 엔트리 수 모니터링 걸어놔야 해.







# 02. Stateful 집계 - 상태값 유형별


## (1) 상태값이 될 수 있는 것

### **1. 이벤트란**

- 이벤트 = 스트리밍 소스에서 들어오는 데이터 한 줄
- 이벤트 = 스트리밍 처리 잡의 입력 단위 하나

Kafka에서 들어오는 주문 이벤트

```
{
  "user_id": "user_001",
  "store_id": "store_A",
  "amount": 15000,
  "event_time": "2024-01-01 12:03:22"
}
```

- 유저가 주문 버튼 누르는 순간 이 데이터 한 줄이 Kafka로 전송됨
- 이게 Spark로 들어오는 이벤트 1개

| 서비스    | 이벤트 발생 시점 | 이벤트 내용                      |
| ------ | --------- | --------------------------- |
| 이커머스   | 유저가 주문    | user_id, amount, 주문시각       |
| 앱 서비스  | 유저가 화면 클릭 | user_id, screen_name, 클릭시각  |
| 결제 시스템 | 결제 승인/거절  | user_id, 금액, 결제결과, 시각       |
| IoT    | 센서 측정값 전송 | device_id, 온도, 측정시각         |
| 로그 수집  | 서버 에러 발생  | server_id, error_code, 발생시각 |


### 2. 상태값의 대상이 되는 것

상태값 = 마이크로배치가 끝나도 다음 배치에서 다시 참조해야 하는 중간 계산값

|대상|예시|
|---|---|
|키 기준 누적값(합산)|유저별 누적 구매금액, 스토어별 주문 수|
|키 기준 집합(str배열)|유저별 방문한 페이지 목록|
|키 기준 최신값|유저별 마지막 접속 시각|
|세션 정보|유저별 현재 세션 시작 시각, 이벤트 수|
|중복 판단 기준|이미 처리한 이벤트 ID 목록|

---

### 3. 시간 연관 여부로 나누는 기준

|구분|기준|이유|
|---|---|---|
|시간 연관 있음|상태가 삭제되는 조건이 시간임|윈도우가 끝나거나, 일정 시간 이벤트 없으면 상태 만료|
|시간 연관 없음|상태가 삭제되는 조건이 시간이 아님|특정 이벤트 발생, 조건 충족 시 상태 만료|

---

### 4. 시간 연관 있는 케이스

- 5분 윈도우 내 주문 건수 → 5분이 지나면 윈도우 삭제
- 30분 이상 이벤트 없으면 세션 종료 → 시간 기준으로 상태 만료
- 1시간 내 동일 유저 중복 이벤트 제거 → 1시간 지나면 중복 판단 기준 삭제

```
# 5분 윈도우 주문 건수

12:00 ~ 12:05 윈도우:
  store_A에서 이벤트 3개 들어옴 → store_A: 3 (StateStore 유지)

12:05 넘어서 watermark 만료:
  store_A → 삭제 (12:00~12:05 윈도우에 새 이벤트 올 가능성 없음)
```

→ watermark 또는 timeout 설정으로 상태 자동 만료 가능

---

### 5. 시간 연관 없는 케이스

- 유저별 누적 구매금액 → 삭제 기준이 없음, 계속 누적
- 유저별 등급 (구매금액 기준) → 이벤트 들어올 때마다 업데이트, 만료 없음
- 실시간 재고 수량 → 입고/출고 이벤트마다 업데이트, 만료 없음

```
# 유저별 누적 구매금액

user_001이 12:03에 15,000원 주문  → 이벤트 1개 → user_001: 15,000
user_001이 14:22에 32,000원 주문  → 이벤트 1개 → user_001: 47,000
user_001이 내일  9,000원 주문     → 이벤트 1개 → user_001: 56,000
→ user_001은 언제든 새 이벤트 보낼 수 있어서 삭제 타이밍 없음
```

→ 상태가 만료되지 않음, StateStore에 영구 상주

---

### 6. 왜 이렇게 나누냐

- 시간 연관 있는 것 = 상태 만료 시점이 명확 → watermark로 자동 정리 가능 → 메모리 관리 예측 가능
- 시간 연관 없는 것 = 상태 만료 시점이 없음 → StateStore에 계속 쌓임 → 키 카디널리티가 메모리를 결정

```
시간 연관 없는 케이스 메모리 계산:
유저별 누적 구매금액
→ 활성 유저 1000만명 = StateStore 엔트리 1000만개 상시 유지
→ 키 카디널리티가 곧 메모리 사용량
```

---

### 7. 엔지니어 판단 기준 및 사고 과정

새 스트리밍 파이프라인 설계 요청 들어오면 제일 먼저 이걸 따져.
"이 상태값, 언제 삭제해도 되는 시점이 있어?"

```
상태 삭제 시점 있음?
    ↓ Yes
시간 기준?
    ↓ Yes → watermark → 관리형 연산
    ↓ No  → 비관리형 연산 (flatMapGroupsWithState)
			(삭제 시점 없나 →  영구 누적되므로 외부 툴 사용 필수!)
			    ↓ No
			키 카디널리티 높음?
			    ↓ Yes → 외부 DB 위임 (Redis / RDS)
			    ↓ No  → StateStore에 그냥 들고 있어도 됨
```

카디널리티 낮으면 (수천~수만) → StateStore에 들고 있어도 부담 없음 → 스트리밍으로 처리

카디널리티 높으면 (수백만~) → 외부 DB 위임 고려
- 빠른 읽기/쓰기 필요 → Redis
- 정합성, 트랜잭션 필요 → RDS
- 대용량 집계 결과 보관 → DynamoDB, Cassandra

---

**엔지니어 독백**
> 실무에서 이 구분이 중요한 이유는 인프라 설계 때문이야. 시간 연관 있는 케이스는 watermark 잘 잡으면 StateStore 크기가 예측 가능해. 근데 시간 연관 없는 케이스는 유저 수, 상품 수 같은 키 카디널리티가 그대로 메모리 사용량이 돼.

> 시간 연관 없는 집계를 스트리밍으로 처리하는 게 맞는지 먼저 따져봐야 해. 유저별 누적 구매금액 같은 건 굳이 스트리밍 StateStore에 들고 있을 필요 없이, Redis나 RDS에 upsert하는 게 더 안정적이야. StateStore는 휘발성이라 잡 재시작하면 체크포인트에서 복구해야 하는데, 외부 DB는 그런 걱정이 없어.

키 카디널리티 계산도 습관적으로 해.

```
유저별 누적 구매금액
→ MAU 500만 → StateStore 상시 500만 엔트리
→ 엔트리당 평균 50바이트 → 약 250MB 상시 점유
→ Executor 메모리 넉넉하면 스트리밍으로 처리 가능
→ 근데 잡 재시작 시 체크포인트 복구 시간도 고려해야 함
```

결국 StateStore에 들고 있는 게 맞는지, 외부 DB로 빼는 게 맞는지는 카디널리티 + 복구 시간 + 메모리 비용 세 가지를 같이 보고 판단해.



## (2) 시간 연관 없는 집계


**핵심 설정값**

- watermark 없음 - 삭제 시점이 없으니까
- `outputMode("update")` - 변경된 것만 Sink로 내보냄
- 외부 DB 연동 시 `foreachBatch` 사용

---

**케이스 1 - 키 기준 누적 집계**

유저별 누적 구매금액 (주문/환불 이벤트 처리)

```
df.withColumn(
    "amount_delta",
    when(col("type") == "order",  col("amount"))
    .when(col("type") == "refund", -col("amount"))
    .otherwise(0)
) \
  .groupBy("user_id") \
  .agg(sum("amount_delta").alias("total_amount")) \
  .writeStream \
  .outputMode("update") \
  .format("console") \
  .start()
```

- `withColumn("amount_delta", ...)` → 기존 df에 `amount_delta`라는 새 컬럼을 추가
- `when/otherwise` 로 이벤트 타입별 부호 분기
	- type이 `"order"` → amount 그대로
	- type이 `"refund"` → amount에 음수 부호
- `sum("amount_delta")` 로 StateStore에 누적 합산
- `.alias("total_amount")` → 결과 컬럼명 지정
- `.writeStream` → Batch write가 아닌 Streaming write 선언
- `outputMode("update")` 로 변경된 user_id 행만 출력(complete면 전체 state 매번 출력)
- watermark 없음 → StateStore에 영구 상주
- `.start()` → 스트리밍 잡 실행 시작 (비동기, 백그라운드에서 계속 실행)

```
# 동작 흐름

마이크로배치 1:
  user_001, order,  15000 → amount_delta: +15000
  user_001, refund, 15000 → amount_delta: -15000
  StateStore: user_001 → 0

마이크로배치 2:
  user_001, order, 32000 → amount_delta: +32000
  StateStore: user_001 → 32000
  Sink 출력 (update): user_001: 32000  ← 변경된 것만
```

**엔지니어 독백**

> 이 패턴은 StateStore가 영구 누적이라 카디널리티 먼저 따져야 해. MAU 500만이면 엔트리 500만개가 Executor 메모리에 항상 올라가 있어. 엔트리당 50바이트만 잡아도 250MB야. Executor 메모리 설계할 때 이걸 반드시 반영해야 해.

> 콘솔 Sink는 로컬 디버깅용이고, 실제로는 아래 케이스 2처럼 외부 DB로 빼는 게 맞아.

---

**케이스 2-1 — Delta Lake upsert (merge)**

```
from delta.tables import DeltaTable

def upsert_to_delta(batch_df, batch_id):
    delta_table = DeltaTable.forPath(spark, "/delta/user_stats")
    
    delta_table.alias("target") \
        .merge(
            batch_df.alias("source"),
            "target.user_id = source.user_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

df.groupBy("user_id") \
  .agg(sum("amount").alias("total_amount")) \
  .writeStream \
  .outputMode("update") \
  .foreachBatch(upsert_to_delta) \
  .start()
```

- `DeltaTable.forPath(spark, "/delta/user_stats")` → 기존 Delta 테이블을 대상으로 지정
- `.alias("target")` → 기존 Delta 테이블을 target으로 명명
- `.merge(..., "target.user_id = source.user_id")` → user_id 기준으로 기존 데이터와 배치 데이터 매칭
- `whenMatchedUpdateAll()` → user_id 일치하면 전체 컬럼 update
- `whenNotMatchedInsertAll()` → user_id 없으면 새 row insert
- `.execute()` → merge 실행

동작 흐름

```
[Kafka / Source]
  이벤트 수신: user_001 order 10000, user_003 order 5000

[Spark Structured Streaming - StateStore]
  groupBy + sum 집계
  user_001: 기존 32000 + 10000 = 42000  (변경됨)
  user_002: 15000                        (변경 없음, 이번 배치에 없음)
  user_003: 없음 → 신규 5000            (신규)

[outputMode("update") → batch_df]
  변경된 row만 내려옴
  user_001: 42000
  user_003: 5000

[foreachBatch → upsert_to_delta]
  Delta Table (S3/HDFS) 기준 merge
  user_001: target에 존재 → whenMatchedUpdateAll → 32000 → 42000
  user_003: target에 없음 → whenNotMatchedInsertAll → 신규 insert 5000

[Delta Table 최종 상태]
  user_001: 42000  (updated)
  user_002: 15000  (unchanged)
  user_003: 5000   (inserted)
```

엔지니어 독백 
> S3나 HDFS 기반 데이터 레이크에 저장할 때는 Delta Lake merge가 표준이야. 파일 기반 스토리지는 원래 upsert 개념이 없어서 덮어쓰거나 새 파일을 추가하는 방식밖에 없었는데, Delta Lake가 트랜잭션 로그로 이걸 해결했거든. staging 테이블도 필요 없고, DB 커넥션도 필요 없어. Spark 안에서 전부 처리되니까 훨씬 깔끔하지. EMR이나 Databricks 환경이면 이게 무조건 1순위야.

---

**케이스 2-2 — PostgreSQL upsert (ON CONFLICT)**

```
def upsert_to_db(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host/db") \
        .option("dbtable", "user_stats_staging") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    conn = psycopg2.connect("host=host dbname=db user=user password=pw")
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO user_stats (user_id, total_amount)
        SELECT user_id, total_amount FROM user_stats_staging
        ON CONFLICT (user_id)
        DO UPDATE SET total_amount = EXCLUDED.total_amount
    """)
    conn.commit()
    cur.close()
    conn.close()

df.groupBy("user_id") \
  .agg(sum("amount").alias("total_amount")) \
  .writeStream \
  .outputMode("update") \
  .foreachBatch(upsert_to_db) \
  .start()
```

- `batch_df.write ... .option("dbtable", "user_stats_staging")` → 본 테이블이 아닌 staging 테이블에 먼저 적재
- `.mode("overwrite")` → 매 배치마다 staging을 새로 덮어씀, 이전 배치 데이터 섞임 방지
- `psycopg2.connect(...)` → Python에서 PostgreSQL 직접 연결하는 표준 드라이버
- `ON CONFLICT (user_id)` → user_id PK 충돌 시 update, 없으면 insert
- `EXCLUDED.total_amount` → staging에서 온 값을 참조하는 PostgreSQL 문법
- `conn.commit()` → 트랜잭션 확정, 이 시점에 본 테이블에 반영

동작 흐름

```
[Kafka / Source]
  이벤트 수신: user_001 order 10000, user_003 order 5000

[Spark Structured Streaming - StateStore]
  groupBy + sum 집계
  user_001: 기존 32000 + 10000 = 42000  (변경됨)
  user_002: 15000                        (변경 없음, 이번 배치에 없음)
  user_003: 없음 → 신규 5000            (신규)

[outputMode("update") → batch_df]
  변경된 row만 내려옴
  user_001: 42000
  user_003: 5000

[foreachBatch → upsert_to_db]
  step1. batch_df → user_stats_staging 테이블에 overwrite
    staging:
      user_001: 42000
      user_003: 5000

  step2. PostgreSQL 내부에서 ON CONFLICT 실행
    user_001: staging → user_stats 존재 → UPDATE 32000 → 42000
    user_003: staging → user_stats 없음 → INSERT 5000

[PostgreSQL user_stats 최종 상태]
  user_001: 42000  (updated)
  user_002: 15000  (unchanged)
  user_003: 5000   (inserted)
```

엔지니어 독백 
> 결과를 RDS에 직접 서빙해야 하는 케이스야. 예를 들어 유저 대시보드에서 실시간으로 total_amount를 조회해야 한다면 S3 Delta 파일을 읽을 수가 없잖아. PostgreSQL에 써야 앱 서버가 바로 쿼리할 수 있거든. 근데 JDBC write는 upsert를 지원 안 해. 그래서 staging에 한번 밀어넣고 PostgreSQL 내부에서 ON CONFLICT로 처리하는 거야. 번거롭긴 한데 RDS 직접 서빙이 필요하면 이 방법밖에 없어.

---

**케이스 3 - 키 기준 최신값 유지**

유저별 마지막 접속 시각

```
df.groupBy("user_id") \
  .agg(max("event_time").alias("last_seen")) \
  .writeStream \
  .outputMode("update") \
  .foreachBatch(upsert_to_db) \
  .start()
```

- `max("event_time")` 으로 항상 가장 최신 시각만 StateStore에 유지
- 새 이벤트 들어올 때마다 StateStore의 last_seen 덮어씀
- `foreachBatch` 로 RDS에 upsert

```
# 동작 흐름

마이크로배치 1: user_001 → last_seen: 12:03
마이크로배치 2: user_001 → last_seen: 14:22  ← max로 항상 최신값만 유지
```

**엔지니어 독백**

last_seen 같은 최신값 유지는 Redis가 더 어울려. RDS는 row lock이 걸리면 동시 upsert 느려지는데, Redis는 `SET user:001:last_seen 14:22` 한 줄로 끝이야. 읽기도 빠르고. 단, Redis는 영속성이 약하니까 중요한 데이터면 RDS에도 같이 써두는 이중화 구조가 실무 표준이야.

---

**케이스별 설정값 요약**

|케이스|watermark|outputMode|Sink|
|---|---|---|---|
|누적 집계 (내부 유지)|없음|`update`|console / memory|
|누적 집계 (외부 DB)|없음|`update`|`foreachBatch` + JDBC|
|최신값 유지|없음|`update`|`foreachBatch` + Redis|
|실시간 재고 수량|없음|`update`|`foreachBatch` + Redis|

---



## (3) 시간 연관 있는 집계

---

**핵심 설정값**

- `withWatermark("event_time", "지연 허용 시간")` : 늦게 도착한 이벤트 얼마나 기다릴지
- `window("event_time", "윈도우 크기")` : 집계 시간 단위
- `outputMode("append")` : 윈도우 닫힌 것만 출력
- 결과는 S3 (Delta Lake / Parquet) 또는 Kafka로 내보냄

---

**케이스 1 - 고정 윈도우 집계**

5분 단위 스토어별 주문 건수 → S3 Delta Lake 적재

```
df.withWatermark("event_time", "10 minutes") \
  .groupBy(
      window("event_time", "5 minutes"),
      "store_id"
  ) \
  .count() \
  .writeStream \
  .outputMode("append") \
  .format("delta") \
  .option("checkpointLocation", "s3://bucket/checkpoint/store_order_count") \
  .option("path", "s3://bucket/store_order_count") \
  .start()
```

- `withWatermark("event_time", "10 minutes")` : 10분까지 늦게 도착한 이벤트 허용
- `window("event_time", "5 minutes")` : 5분 단위 윈도우 생성
- `outputMode("append")` : 윈도우 닫혀야만 S3에 적재
- `format("delta")` : Delta Lake 포맷으로 S3에 저장
- `checkpointLocation` : 잡 재시작 시 오프셋 + StateStore 복구 위치

```
# 동작 흐름

이벤트: store_A, 12:03
  → 12:00~12:05 윈도우 StateStore: store_A: 1

이벤트: store_A, 12:04
  → 12:00~12:05 윈도우 StateStore: store_A: 2

현재 최대 event_time: 12:15
watermark 기준선: 12:05  (12:15 - 10분)
  → 12:00~12:05 윈도우 닫힘 → S3 Delta Lake 적재 → StateStore 삭제
```

**실무 구축 시 고려사항**

1. checkpointLocation 필수
    - 없으면 잡 재시작 시 Kafka 오프셋 처음부터 읽어서 중복 처리 발생
    - S3 경로는 쿼리마다 고유하게 설정해야 함, 공유하면 충돌남
2. watermark 값 결정
    - Kafka 컨슈머 지연 모니터링해서 p99 지연값 기준으로 설정
    - 너무 짧으면 늦은 이벤트 유실, 너무 길면 StateStore 메모리 낭비
    - 실무에서 평균 지연 2분이면 watermark 10분으로 넉넉하게 잡음
3. append 모드와 윈도우 닫힘 타이밍
    - 윈도우가 닫혀야 S3에 적재되니까 실시간성이 watermark만큼 지연됨
    - watermark 10분이면 집계 결과가 최소 10분 뒤에 S3에 써짐
    - 이 지연이 허용되는지 비즈니스 요구사항 확인 필수

**엔지니어 독백**

신입들이 가장 많이 놓치는 게 checkpointLocation이야. 없이 올렸다가 잡 재시작하면 Kafka 오프셋 처음부터 읽어서 데이터 중복 적재돼. 프로덕션에서 이거 터지면 S3 데이터 다 지우고 다시 돌려야 하는 최악의 상황 생겨.

watermark 지연도 간과하는 케이스 많아. "실시간 집계 해주세요" 요청에 watermark 10분 잡으면 결과가 10분 뒤에 나와. 기획자한테 이 지연 미리 설명 안 하면 나중에 "왜 실시간이 아니냐"는 소리 들어.

---

**케이스 2 - 슬라이딩 윈도우 집계**

5분마다 최근 10분 데이터 집계 → Kafka로 다운스트림 전달

```
df.withWatermark("event_time", "10 minutes") \
  .groupBy(
      window("event_time", "10 minutes", "5 minutes"),
      "store_id"
  ) \
  .count() \
  .writeStream \
  .outputMode("append") \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-broker:9092") \
  .option("topic", "store_order_sliding") \
  .option("checkpointLocation", "s3://bucket/checkpoint/sliding") \
  .start()
```

- `window("event_time", "10 minutes", "5 minutes")` : 윈도우 10분, 5분마다 슬라이딩
- `format("kafka")` : 집계 결과를 다운스트림 Kafka 토픽으로 전달
- 이벤트 하나가 여러 윈도우에 동시 집계됨

```
# 윈도우 구조

12:00~12:10 윈도우
12:05~12:15 윈도우
12:10~12:20 윈도우

store_A 12:07 이벤트
  → 12:00~12:10 윈도우에 집계
  → 12:05~12:15 윈도우에도 집계  ← 동시에 두 윈도우에 포함
```

**실무 구축 시 고려사항**

1. StateStore 크기 급증
    - 이벤트 하나가 윈도우크기/슬라이딩간격 개수만큼 윈도우에 집계됨
    - 10분/5분이면 이벤트당 윈도우 2개, StateStore가 고정 윈도우 대비 2배
    - 슬라이딩 간격 짧게 잡으면 메모리 급증하니까 반드시 계산하고 써야 함
2. Kafka Sink 직렬화
    - Kafka로 내보낼 때 결과를 JSON 직렬화해서 `value` 컬럼에 넣어야 함
    - `to_json(struct(*))` 으로 전체 컬럼을 JSON 문자열로 변환 후 전송

**엔지니어 독백**

슬라이딩 윈도우는 쓸 이유가 명확할 때만 써. "최근 10분 데이터를 5분마다 보고 싶다"는 명확한 요구사항 없으면 그냥 고정 윈도우 써. StateStore가 배수로 커지는 비용을 정당화할 수 있어야 해.

---

**케이스 3 - 중복 이벤트 제거**

Kafka 재전송으로 인한 중복 이벤트 제거 후 Delta Lake 적재

```
df.withWatermark("event_time", "1 hour") \
  .dropDuplicates(["event_id", "event_time"]) \
  .writeStream \
  .outputMode("append") \
  .format("delta") \
  .option("checkpointLocation", "s3://bucket/checkpoint/dedup") \
  .option("path", "s3://bucket/events_deduped") \
  .start()
```

- `dropDuplicates(["event_id", "event_time"])` : event_id + event_time 조합으로 중복 판단
- watermark 1시간 → 1시간 지난 event_id는 StateStore에서 삭제
- 삭제 후 동일 event_id 들어오면 신규 이벤트로 처리

```
# 동작 흐름

이벤트: event_id=abc, 12:03 → StateStore에 abc 기록 → Delta Lake 적재
재수신: event_id=abc, 12:03 → StateStore에 abc 있음 → 드롭

watermark 1시간 지남:
  → abc StateStore 삭제
  → 이후 abc 들어오면 신규 이벤트로 처리
```

**실무 구축 시 고려사항**

1. event_id 설계
    - 업스트림에서 event_id를 UUID로 생성해서 이벤트에 포함시켜야 함
    - event_id 없으면 중복 판단 기준이 없어서 dropDuplicates 못 씀
    - 신규 파이프라인 구축 시 업스트림 팀과 event_id 스펙 협의 필수
2. watermark 길이와 StateStore 크기 트레이드오프
    - watermark 길수록 더 오래된 중복 잡을 수 있지만 StateStore 메모리 증가
    - 업스트림 retry 정책 보고 결정, 보통 retry 최대 시간 + 여유분으로 설정

**엔지니어 독백**

중복 제거는 파이프라인 처음 설계할 때 넣어야 해. 나중에 "중복 데이터 있어요" 리포트 받고 나서 추가하려면 이미 쌓인 데이터 정합성 맞추는 작업이 훨씬 커. 처음부터 방어 코드로 넣는 게 맞아.

event_id 없는 업스트림 만나는 경우 많아. 그럼 event_time + user_id + amount 조합으로 복합키 만들어서 중복 판단하는데, 이건 완벽하지 않아. 동일 유저가 동시각에 동일 금액 주문하면 중복으로 잘못 판단할 수 있어. 그래서 업스트림에 event_id 추가 요청하는 게 맞는 방향이야.

---

**케이스별 설정값 요약**

|케이스|윈도우 타입|watermark|outputMode|Sink|
|---|---|---|---|---|
|고정 윈도우 집계|`window()`|필수|`append`|Delta Lake (S3)|
|슬라이딩 윈도우 집계|`window()` 3번째 인자|필수|`append`|Kafka|
|세션 윈도우 집계|`session_window()`|필수|`append`|Delta Lake (S3)|
|중복 제거|없음|필수|`append`|Delta Lake (S3)|

# 03. 스트리밍 조인 / 임의 상태 유지 연산 / 성능 튜닝

## (4) 스트리밍 조인

---

### 배경 - 왜 스트리밍 조인이 필요한가

기존 배치 처리에서는 조인이 단순했다. 두 테이블 모두 메모리에 올려놓고 한 번에 처리하면 끝이었다. 하지만 스트리밍에서는 한쪽 또는 양쪽이 무한히 들어오는 데이터라서 기존 방식을 그대로 쓸 수 없다.

문제:

- 스트림은 끝이 없어서 "전체를 메모리에 올린다"는 개념이 성립 안 됨
- 어느 시점의 이벤트와 조인할지 기준이 없으면 StateStore가 무한 증가
- 늦게 도착한 이벤트와 이미 처리된 이벤트를 어떻게 매칭할지 기준 필요

Spark Structured Streaming은 이를 세 가지 방식으로 해결했다.

---

**핵심 설정값**

- 스트림-정적 조인: watermark 불필요, 정적 데이터는 매 배치마다 재참조
- 스트림-스트림 조인: `withWatermark` 양쪽 모두 필수, 조인 조건에 시간 범위 명시
- 외부 조인: watermark 필수, 없으면 `AnalysisException` 발생

---

### 케이스 1 - 스트림-정적 데이터 조인

#### 한계 배경

실시간 주문 이벤트에 상품명, 카테고리 같은 메타데이터를 붙이고 싶은데, 이건 DB에 있는 정적 데이터다. 매 이벤트마다 DB 쿼리를 날리면 부하가 너무 크고, 배치로 미리 조인해두면 실시간성이 없다. Spark는 이 문제를 "정적 DataFrame은 드라이버에서 한 번 읽어서 Executor에 브로드캐스트" 방식으로 해결했다.

주문 스트림 + 상품 메타데이터 조인 → S3 Delta Lake 적재

```
# 정적 데이터 (상품 메타데이터) - S3 또는 Glue에서 읽기
product_df = spark.read \
    .format("delta") \
    .load("s3://bucket/product_meta")

# 스트리밍 데이터 (주문 이벤트) - Kafka에서 읽기
order_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "orders") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), order_schema).alias("data")) \
    .select("data.*")

# 조인: 스트림 + 정적 DataFrame
result = order_stream.join(
    broadcast(product_df),       # 정적 DF를 브로드캐스트로 Executor 전달
    on="product_id",
    how="left"                   # 상품 메타 없어도 주문 이벤트 유실 방지
)

result.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoint/order_enriched") \
    .option("path", "s3://bucket/order_enriched") \
    .start()
```

- `spark.read` → 스트리밍 아님, 한 번만 읽는 정적 DataFrame
- `broadcast(product_df)` → 정적 DF를 모든 Executor 메모리에 복사, 셔플 없이 조인 가능
- `how="left"` → 상품 메타 없는 주문 이벤트도 드롭하지 않고 null로 통과

```
# 동작 흐름

[잡 시작 시]
  product_df → S3 Delta에서 한 번 읽음 → 드라이버 → 모든 Executor에 브로드캐스트

[마이크로배치 1]
  order: product_id=A001, amount=15000
  → Executor 로컬 브로드캐스트 테이블에서 product_id=A001 조회
  → product_name="맥북프로", category="전자기기" 붙여서 출력

[마이크로배치 2, 3, ...]
  정적 DF 재로드 없이 브로드캐스트 그대로 재사용
```

**실무 구축 시 고려사항**

1. 정적 데이터 갱신 문제
    - product_df는 잡 시작 시 한 번만 읽힘
    - 상품 메타 업데이트돼도 실시간 반영 안 됨
    - 해결: 잡 주기적 재시작 또는 `foreachBatch` 안에서 매 배치마다 정적 DF 재로드
2. 브로드캐스트 크기 제한
    - `spark.sql.autoBroadcastJoinThreshold` 기본 10MB
    - 상품 메타 수십만 건이면 초과 가능 → 수동으로 `broadcast()` 명시하거나 임계값 조정
    - 너무 크면 Executor OOM 위험

**엔지니어 독백**

> 스트림-정적 조인에서 제일 많이 실수하는 게 정적 DF 갱신 타이밍이야. "실시간으로 상품 이름이 바뀌어야 한다"는 요구사항이 생기면 그때부터 설계가 달라져. 잡 재시작 주기랑 메타 업데이트 주기 맞추는 게 현실적이고, 1분마다 메타 바뀌는 케이스면 아예 스트림-스트림 조인으로 구조를 바꾸는 게 나아.

---

### 케이스 2 - 스트림-스트림 조인

#### 한계 배경

광고 클릭 이벤트와 주문 이벤트 두 스트림을 조인하고 싶다. 유저가 광고 클릭 후 30분 안에 구매했으면 전환으로 집계해야 한다. 배치라면 두 테이블 그냥 조인하면 되지만, 스트리밍에서는 광고 클릭 이벤트가 아직 안 온 상태에서 구매 이벤트가 먼저 올 수 있다. 양쪽 이벤트를 StateStore에 버퍼링해뒀다가 매칭해야 하는데, 버퍼를 언제까지 들고 있어야 할지 기준이 필요하다. 그 기준이 watermark다.

광고 클릭 → 30분 이내 구매 전환 집계

```
# 광고 클릭 스트림
click_stream = spark.readStream \
    .format("kafka") \
    .option("subscribe", "ad_clicks") \
    .load() \
    .select(...) \
    .withWatermark("click_time", "30 minutes")   # 클릭 이벤트 30분까지 대기

# 구매 스트림
order_stream = spark.readStream \
    .format("kafka") \
    .option("subscribe", "orders") \
    .load() \
    .select(...) \
    .withWatermark("order_time", "30 minutes")   # 구매 이벤트 30분까지 대기

# 스트림-스트림 조인: 클릭 후 30분 이내 구매
result = click_stream.join(
    order_stream,
    expr("""
        click_stream.user_id = order_stream.user_id
        AND order_time BETWEEN click_time AND click_time + INTERVAL 30 MINUTES
    """),
    how="inner"
)

result.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoint/ad_conversion") \
    .option("path", "s3://bucket/ad_conversion") \
    .start()
```

- `withWatermark("click_time", "30 minutes")` → 클릭 이벤트를 StateStore에 최대 30분 버퍼링
- `withWatermark("order_time", "30 minutes")` → 구매 이벤트를 StateStore에 최대 30분 버퍼링
- `order_time BETWEEN click_time AND click_time + INTERVAL 30 MINUTES` → 조인 조건에 시간 범위 명시 필수, 없으면 StateStore 무한 증가
- `how="inner"` → 매칭된 쌍만 출력, 전환 없는 클릭은 드롭

```
# 동작 흐름

[StateStore 버퍼링]
  click: user_001, 12:00 → click StateStore에 적재
  order: user_001, 12:20 → order StateStore에 적재

[조인 매칭 시도]
  user_001 click 12:00 + user_001 order 12:20
  → 12:20 BETWEEN 12:00 AND 12:30 → 조건 충족 → 전환 이벤트 출력

[watermark 기준 StateStore 정리]
  현재 최대 event_time: 12:45
  watermark 기준선: 12:15  (30분 적용)
  → 12:15 이전 click/order 이벤트 StateStore에서 삭제
```

**실무 구축 시 고려사항**

1. StateStore 크기 = 양쪽 스트림의 watermark 윈도우 내 이벤트 수 합산
    - 트래픽 많은 스트림 둘 다 30분 버퍼링하면 메모리 압박 큼
    - watermark 최소화하되 비즈니스 요구 충족하는 값 협의 필요
2. 조인 조건에 시간 범위 반드시 명시
    - `user_id`만으로 조인하면 Spark가 StateStore 만료 시점을 계산 못 함
    - `AnalysisException` 또는 무한 StateStore 증가 발생

---

### 케이스 3 - 워터마킹을 이용한 외부 조인

#### 한계 배경

inner 조인은 양쪽 매칭이 안 되면 결과에서 드롭된다. 광고 클릭했지만 구매 안 한 유저도 집계에 포함시켜야 한다면 left outer 또는 full outer 조인이 필요하다. 하지만 outer 조인에서는 "매칭 없음" 판단을 언제 해야 할지 기준이 필요하다. 구매 이벤트가 아직 안 온 건지, 영원히 안 오는 건지 알 수 없기 때문이다. watermark가 이 시점을 결정한다.

클릭했지만 전환 없는 유저도 포함한 광고 성과 집계

```
# watermark 없으면 outer 조인에서 AnalysisException 발생
click_stream = click_stream.withWatermark("click_time", "1 hour")
order_stream = order_stream.withWatermark("order_time", "1 hour")

result = click_stream.join(
    order_stream,
    expr("""
        click_stream.user_id = order_stream.user_id
        AND order_time BETWEEN click_time AND click_time + INTERVAL 30 MINUTES
    """),
    how="left_outer"    # 클릭은 있고 구매 없으면 구매 컬럼 null로 출력
)

result.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoint/ad_performance") \
    .option("path", "s3://bucket/ad_performance") \
    .start()
```

- `how="left_outer"` → 클릭 이벤트는 무조건 출력, 구매 매칭 없으면 null
- watermark 1시간 → 1시간 지나도 매칭 구매 없으면 null로 확정 출력
- `outputMode("append")` 필수 → outer 조인 결과는 watermark 지난 후에만 확정

```
# 동작 흐름

click: user_001, 12:00 → StateStore 버퍼링
  → 1시간 내 구매 이벤트 기다리는 중

watermark 기준선이 13:00 넘음:
  → user_001 클릭에 매칭 구매 없음 확정
  → user_001, click_time=12:00, order_time=null 출력
  → StateStore에서 삭제

click: user_002, 12:10, order: user_002, 12:35 → 정상 매칭
  → user_002, click_time=12:10, order_time=12:35 출력
```

**실무 구축 시 고려사항**

1. outer 조인 결과 출력 지연 = watermark 시간
    - watermark 1시간이면 "전환 없음" 판정이 1시간 후에 나옴
    - 실시간 대시보드에 이 지연 반영해야 함
2. full_outer는 양쪽 모두 watermark 필수
    - left_outer는 왼쪽(click_stream) watermark만 있어도 동작하나 양쪽 다 설정 권장

**엔지니어 독백**

> outer 조인에서 watermark 빠뜨리면 Spark가 "이 row의 null 판정 언제 해요?" 를 알 수가 없어서 바로 에러 뱉어. 그래서 outer 조인은 watermark 없이 쓸 수 있는 방법이 없어. 이게 inner 조인이랑 가장 큰 차이점이야.

> 실무에서 outer 조인은 광고 전환율, 이탈 분석 같은 케이스에 많이 써. "액션 A를 했는데 액션 B를 안 한 유저" 패턴이면 전부 outer 조인으로 풀 수 있어. 근데 watermark를 짧게 잡으면 전환 가능 시간 놓치고, 길게 잡으면 StateStore 크고 결과 지연이 길어져서 비즈니스 팀이랑 허용 지연 협의가 먼저야.

---

**조인 유형별 설정값 요약**

|조인 유형|watermark|조인 조건 시간 범위|outputMode|StateStore 특징|
|---|---|---|---|---|
|스트림-정적|불필요|불필요|`append`|정적 DF는 브로드캐스트|
|스트림-스트림 inner|양쪽 필수|필수|`append`|양쪽 이벤트 버퍼링|
|스트림-스트림 left outer|양쪽 권장|필수|`append`|watermark 후 null 확정|
|스트림-스트림 full outer|양쪽 필수|필수|`append`|양쪽 null 확정|

---

## (5) 임의의 상태 유지 연산

---

### 배경 - 왜 mapGroupsWithState / flatMapGroupsWithState가 필요한가

`groupBy + agg` 조합은 Spark가 상태를 내부적으로 관리해줘서 편하다. 하지만 표현할 수 있는 집계가 제한적이다. "유저가 5분 안에 3번 이상 로그인 시도하면 알람", "이벤트 시퀀스가 A → B → C 순서로 발생했을 때만 출력" 같은 복잡한 상태 전이 로직은 `groupBy + agg` 로 표현 불가능하다.

mapGroupsWithState / flatMapGroupsWithState는 개발자가 상태의 정의, 갱신 로직, 삭제 조건을 직접 제어할 수 있는 저수준 API다. 내부적으로는 동일하게 StateStore를 사용하지만 Spark가 자동으로 관리하지 않고 개발자 코드가 상태를 직접 읽고 쓴다.

---

**핵심 설정값**

- `mapGroupsWithState` : 키당 반드시 출력 1개 (상태 유무 무관)
- `flatMapGroupsWithState` : 키당 출력 0개 이상 (Iterator 반환, 조건부 출력 가능)
- 타임아웃: `GroupStateTimeout.ProcessingTimeTimeout` 또는 `EventTimeTimeout`
- `outputMode("update")` 권장 (append는 타임아웃 핸들링 있는 경우만 가능)

---

### 케이스 1 - mapGroupsWithState로 세션 추적

#### 한계 배경

유저별 세션을 추적하고 싶다. 세션은 "마지막 이벤트로부터 30분 이상 이벤트 없으면 종료"라는 규칙인데, 이건 `window()` 함수로 표현 불가능하다. `window()`는 고정된 시간 범위로 집계하는 거고, 세션은 이벤트 발생 여부에 따라 동적으로 길이가 달라지기 때문이다.

유저별 세션 상태 추적 (활성 세션 정보 실시간 유지)

```
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from typing import Iterator, Tuple
import dataclasses

@dataclasses.dataclass
class SessionState:
    session_start: str    # 세션 시작 시각
    event_count: int      # 세션 내 이벤트 수
    last_event: str       # 마지막 이벤트 시각

def update_session(
    user_id: str,
    events: Iterator,
    state: GroupState
) -> Tuple:
    
    # 타임아웃 발생 시 (30분 이상 이벤트 없음) → 세션 종료
    if state.hasTimedOut:
        current = state.get                        # 현재 상태 읽기
        state.remove()                             # 상태 삭제
        return (user_id, current.session_start, current.event_count, "closed")
    
    # 상태 초기화 or 기존 상태 로드
    if state.exists:
        current = state.get
    else:
        first_event = next(iter(events))
        current = SessionState(
            session_start=first_event.event_time,
            event_count=0,
            last_event=first_event.event_time
        )
    
    # 이벤트 처리 → 상태 갱신
    for event in events:
        current = dataclasses.replace(
            current,
            event_count=current.event_count + 1,
            last_event=event.event_time
        )
    
    # 갱신된 상태 저장 + 타임아웃 갱신 (30분)
    state.update(current)
    state.setTimeoutDuration("30 minutes")         # 처리 시간 타임아웃 재설정
    
    return (user_id, current.session_start, current.event_count, "active")

# 스키마 정의 필수 (반환 타입 명시)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

output_schema = StructType([
    StructField("user_id", StringType()),
    StructField("session_start", StringType()),
    StructField("event_count", IntegerType()),
    StructField("status", StringType())
])

state_schema = StructType([
    StructField("session_start", StringType()),
    StructField("event_count", IntegerType()),
    StructField("last_event", StringType())
])

df.groupBy("user_id") \
  .mapGroupsWithState(
      func=update_session,
      outputStructType=output_schema,
      stateStructType=state_schema,
      timeoutConf=GroupStateTimeout.ProcessingTimeTimeout   # 처리 시간 기준 타임아웃
  ) \
  .writeStream \
  .outputMode("update") \
  .format("delta") \
  .option("checkpointLocation", "s3://bucket/checkpoint/session") \
  .start()
```

- `state.hasTimedOut` → 타임아웃 발생 여부, True면 타임아웃 핸들러 분기
- `state.exists` → 기존 상태 존재 여부
- `state.get` → 현재 상태값 읽기
- `state.update(current)` → 갱신된 상태를 StateStore에 씀
- `state.remove()` → StateStore에서 해당 키 삭제
- `state.setTimeoutDuration("30 minutes")` → 처리 시간 기준으로 30분 후 타임아웃 발생 예약
- `GroupStateTimeout.ProcessingTimeTimeout` → 실제 시각(벽시계) 기준 타임아웃
- `outputMode("update")` → 매 배치마다 변경된 키만 출력

```
# 동작 흐름

마이크로배치 1:
  user_001, 12:00 이벤트
  → state 없음 → SessionState(session_start=12:00, event_count=1) 생성
  → setTimeoutDuration(30분) → 12:30까지 이벤트 없으면 타임아웃
  → 출력: user_001, 12:00, 1, "active"

마이크로배치 2:
  user_001, 12:15 이벤트
  → state 있음 → event_count=2, last_event=12:15
  → setTimeoutDuration(30분) 재설정 → 12:45까지 연장
  → 출력: user_001, 12:00, 2, "active"

12:45 이후 user_001 이벤트 없음 → 타임아웃 발생:
  → state.hasTimedOut = True
  → state.remove()
  → 출력: user_001, 12:00, 2, "closed"
```

**실무 구축 시 고려사항**

1. 스키마 명시 필수
    - `outputStructType`, `stateStructType` 모두 반드시 명시
    - 없으면 런타임 에러 발생
2. mapGroupsWithState는 반드시 1개 출력
    - 타임아웃 발생 키든 이벤트 처리 키든 무조건 row 1개 반환해야 함
    - "이 배치에 특정 조건 충족 안 하면 출력 없음" 이 필요하면 flatMapGroupsWithState 사용

---

### 케이스 2 - 타임아웃 유형 선택

#### 처리 시간 타임아웃 (ProcessingTimeTimeout)

```
state.setTimeoutDuration("30 minutes")
# GroupStateTimeout.ProcessingTimeTimeout
```

- 벽시계(실제 시각) 기준
- 이벤트 타임스탬프와 무관하게 잡이 실제로 실행되는 시각 기준
- 네트워크 지연, 지각 이벤트 영향 없음
- 세션 관리, 비활성 감지처럼 실제 경과 시간이 중요한 케이스에 적합

#### 이벤트 타임 타임아웃 (EventTimeTimeout)

```
state.setTimeoutTimestamp(event.event_time_ms + 30 * 60 * 1000)
# GroupStateTimeout.EventTimeTimeout
# withWatermark 필수
```

- 이벤트 타임스탬프 기준
- watermark 진행에 따라 타임아웃 발생 → 늦게 도착한 이벤트도 올바른 시각 기준으로 처리
- 이벤트 시각 기준 집계가 중요할 때 사용
- `withWatermark` 없으면 타임아웃 발생 안 함 (watermark가 시간 기준선 제공)

**타임아웃 유형 비교**

|구분|기준|watermark|적합한 케이스|
|---|---|---|---|
|ProcessingTimeTimeout|실제 시각|불필요|세션 관리, 비활성 감지|
|EventTimeTimeout|이벤트 타임스탬프|필수|이벤트 시각 기준 집계|

**엔지니어 독백**

> 타임아웃 유형 선택이 생각보다 중요해. 실무에서 세션 관리는 거의 ProcessingTimeTimeout 써. 유저가 진짜 30분 동안 아무것도 안 했는지 판단해야 하는데, 이건 이벤트 시각 기준이 아니라 실제 경과 시간 기준이어야 하거든. EventTimeTimeout은 재처리(backfill) 시에 유용해. 과거 데이터를 다시 흘릴 때 실제 시각이 아닌 이벤트 시각 기준으로 세션을 나눠야 하면 EventTimeTimeout이 맞아.

---

### 케이스 3 - flatMapGroupsWithState로 이상 감지

#### mapGroupsWithState와의 차이

`mapGroupsWithState`는 키당 반드시 1개 row를 반환해야 한다. 조건이 충족됐을 때만 알람을 내보내고 싶은데, 매 배치마다 "정상" row를 출력하면 다운스트림이 오염된다. 이때 `flatMapGroupsWithState`를 쓰면 출력 개수를 0개 이상으로 자유롭게 제어할 수 있다.

5분 안에 3번 이상 로그인 실패 시 알람 이벤트 출력

```
@dataclasses.dataclass
class LoginFailState:
    fail_count: int
    window_start: str

def detect_brute_force(
    user_id: str,
    events: Iterator,
    state: GroupState
) -> Iterator:     # Iterator 반환 → 0개 이상 출력 가능
    
    if state.hasTimedOut:
        state.remove()
        return     # 타임아웃 시 출력 없이 상태만 삭제
    
    current = state.get if state.exists else LoginFailState(0, "")
    
    for event in events:
        if event.event_type == "login_fail":
            if current.fail_count == 0:
                current = dataclasses.replace(current, window_start=event.event_time)
            current = dataclasses.replace(current, fail_count=current.fail_count + 1)
    
    state.update(current)
    state.setTimeoutDuration("5 minutes")
    
    # 조건 충족 시에만 출력 (yield → Iterator)
    if current.fail_count >= 3:
        state.remove()                 # 알람 발생 후 상태 초기화
        yield (user_id, current.window_start, current.fail_count, "ALERT")
    # 조건 미충족 시 yield 없음 → 출력 0개

df.groupBy("user_id") \
  .flatMapGroupsWithState(
      func=detect_brute_force,
      outputMode="append",
      stateStructType=state_schema,
      outputStructType=output_schema,
      timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
  ) \
  .writeStream \
  .outputMode("append") \
  .format("kafka") \
  .option("topic", "security_alerts") \
  .start()
```

- `Iterator` 반환 타입 → `yield`로 출력 개수 자유롭게 제어
- 조건 충족 시만 `yield` → 조건 미충족 시 출력 0개 (배치에서 해당 키 결과 없음)
- 알람 발생 후 `state.remove()` → 카운터 초기화, 다음 5분 윈도우 새로 시작

```
# 동작 흐름

마이크로배치 1:
  user_001 login_fail → fail_count=1
  → 조건 미충족 → 출력 없음

마이크로배치 2:
  user_001 login_fail × 2 → fail_count=3
  → 조건 충족 → ALERT 출력 → state.remove()

마이크로배치 3:
  user_001 login_fail → state 없음 → fail_count=1 재시작
```

**엔지니어 독백**

> flatMapGroupsWithState는 알람, 이상 감지, 이벤트 시퀀스 매칭에 최적이야. "A → B → C 순서 발생 시 출력"도 이 패턴으로 구현할 수 있어. 상태에 시퀀스 단계를 저장해두고, 이벤트 들어올 때마다 단계 진행하고, 완성되면 yield하고 state.remove()하면 돼. mapGroupsWithState 쓰다가 "이 배치에 출력 안 하고 싶다"는 생각이 들면 flatMapGroupsWithState로 갈아타는 게 맞아.

---

**API 비교 요약**

|구분|출력 개수|적합한 케이스|outputMode|
|---|---|---|---|
|`mapGroupsWithState`|키당 정확히 1개|세션 상태, 실시간 집계|`update`|
|`flatMapGroupsWithState`|키당 0개 이상|이상 감지, 알람, 시퀀스 매칭|`append` / `update`|

---

## (6) 성능 튜닝

---

### 배경 - 스트리밍 성능 튜닝이 배치와 다른 이유

배치 처리는 한 번 실행하고 끝이라서 느려도 기다리면 된다. 스트리밍은 24/7 계속 실행되기 때문에 메모리 누수, 셔플 비용, 처리량 한계 같은 문제가 시간이 지날수록 누적된다. 클러스터 자원 설계를 잘못하면 Executor가 죽고, 셔플 파티션을 잘못 잡으면 태스크 스큐가 생기고, 처리량 제한 없으면 Kafka 오프셋이 밀려서 복구 불가 상태에 빠진다.

---

**핵심 설정값**

- `spark.executor.memory`, `spark.executor.cores` : Executor 자원 설계
- `spark.sql.shuffle.partitions` : 셔플 파티션 수 (기본 200, 스트리밍엔 과도)
- `maxOffsetsPerTrigger` : Kafka 소스 처리량 제한
- 동일 SparkSession에서 복수 쿼리 실행 시 스레드 풀 공유 주의

---

### 케이스 1 - 클러스터 자원 배치

#### 설계 기준

스트리밍 잡의 자원 설계는 "이 잡이 최악의 트래픽 spike 때도 버텨야 한다"는 전제로 시작한다.

```
# EMR / Spark Submit 기준 설정값 예시

spark.executor.memory=8g         # Executor 힙 메모리
spark.executor.memoryOverhead=2g # off-heap (Python UDF, 셔플 버퍼)
spark.executor.cores=4           # Executor당 코어 수
spark.driver.memory=4g           # 드라이버 메모리
```

- `spark.executor.memory` → JVM 힙, StateStore + 처리 중 데이터 담당
- `spark.executor.memoryOverhead` → Python UDF, 네이티브 라이브러리, 셔플 버퍼 담당. 기본값은 executor.memory의 10% (최소 384MB). pandas UDF 많이 쓰면 반드시 늘려야 함
- `spark.executor.cores` → 코어 많을수록 태스크 병렬도 높지만 GC 경합 증가. 실무에서 4~5가 안정적

**StateStore 메모리 계산 예시**

```
# 유저별 세션 상태 유지 (mapGroupsWithState)
# 활성 유저 100만명, 상태 엔트리당 200 bytes

100만 × 200 bytes = 200MB  → StateStore 상시 점유
Executor 수 10개           → Executor당 20MB StateStore
+ 처리 중 데이터 버퍼       → executor.memory 6~8GB 권장

# 엔트리당 크기 계산: 실제로는 Java 객체 오버헤드 포함해서 명시 크기보다 2~3배 커짐
```

**실무 구축 시 고려사항**

1. Dynamic Allocation은 스트리밍에서 주의
    - `spark.dynamicAllocation.enabled=true`이면 Executor 수가 자동 조정됨
    - Executor 반환 시 StateStore가 해당 Executor에 있으면 체크포인트에서 복구 필요 → 지연 발생
    - 스트리밍 잡은 고정 Executor 수 권장: `spark.dynamicAllocation.enabled=false`
2. GC 튜닝
    - StateStore가 크면 GC 압박 커짐
    - G1GC 사용 권장: `spark.executor.extraJavaOptions=-XX:+UseG1GC`

**엔지니어 독백**

> 스트리밍 잡에서 Dynamic Allocation 켜두고 운영하다가 새벽에 Executor 반환되면서 StateStore 복구하는 데 배치가 밀리는 사고를 한 번 겪어봐야 알아. 그 이후로 스트리밍은 무조건 고정 Executor로 운영해. 자원 낭비처럼 보여도 안정성이 훨씬 중요해.

---

### 케이스 2 - 셔플을 위한 파티션 수

#### 한계 배경

`spark.sql.shuffle.partitions` 기본값은 200이다. 배치에서는 대용량 데이터를 처리하기 위한 값이지만, 스트리밍의 마이크로배치는 보통 수초~수분 간격의 소량 데이터다. 파티션 200개를 만들면 빈 파티션이 대부분이고 태스크 스케줄링 오버헤드만 생긴다.

```
# Spark 설정
spark.conf.set("spark.sql.shuffle.partitions", "10")

# 또는 SparkSession 생성 시
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()
```

- 스트리밍에서 권장 파티션 수: `Executor 수 × Executor당 코어 수` 기준
- 예: Executor 5개 × 코어 4개 = 20 파티션

**파티션 수 판단 기준**

```
# 트리거 간격이 짧고 (수초), 이벤트 수 적음 (수천~수만)
→ 파티션 수 낮게 (10~20)

# 트리거 간격 길고 (수분), 이벤트 수 많음 (수십만 이상)
→ 파티션 수 높게 (50~100)

# AQE 활성화 시 런타임에 자동 조정 가능
spark.conf.set("spark.sql.adaptive.enabled", "true")
# AQE가 빈 파티션 합치고 스큐 파티션 분할해줌
```

- `spark.sql.adaptive.enabled` → AQE(Adaptive Query Execution) 활성화, 런타임에 파티션 수 자동 최적화

**엔지니어 독백**

> 스트리밍 처음 올릴 때 shuffle.partitions를 기본 200으로 두는 팀 많아. Spark UI에서 Stage 보면 태스크 200개인데 190개는 0 records야. 태스크 스케줄링 오버헤드만 잡아먹는 거거든. AQE 켜두면 어느 정도 자동으로 처리해주긴 하는데, 명시적으로 낮게 잡는 게 더 안정적이야.

---

### 케이스 3 - 안정성을 위한 소스 처리량 제한

#### 한계 배경

Kafka에 데이터가 갑자기 폭증하면 Spark는 최대한 많이 읽으려 한다. 배치라면 그게 좋지만, 스트리밍에서는 한 배치가 너무 커지면 처리 시간이 트리거 간격을 초과해서 다음 배치가 밀리고, 결국 지연이 누적되어 Kafka 오프셋 lag이 걷잡을 수 없이 커진다.

```
# Kafka 소스 처리량 제한
order_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "orders") \
    .option("maxOffsetsPerTrigger", 100000) \   # 배치당 최대 10만 메시지
    .option("startingOffsets", "latest") \
    .load()
```

- `maxOffsetsPerTrigger` → 마이크로배치당 Kafka에서 읽어올 최대 오프셋 수
    - 배치 크기를 고정하여 처리 시간 예측 가능하게 만듦
    - lag 폭증 시 점진적으로 소화, 갑작스러운 OOM 방지

**처리량 제한값 계산**

```
# 목표 트리거 간격: 30초
# Executor 처리 능력: 초당 5,000 메시지

30초 × 5,000 = 150,000
→ maxOffsetsPerTrigger=150000 으로 설정

# 안전 여유 20% 적용 → 120,000으로 설정
# 처음엔 낮게 잡고 Spark UI에서 배치 처리 시간 보면서 올리는 게 실무 방식
```

**실무 구축 시 고려사항**

1. lag 모니터링 필수
    - `maxOffsetsPerTrigger`이 너무 낮으면 Kafka lag이 계속 쌓임
    - Grafana + Kafka Consumer Lag Exporter로 lag 추이 모니터링
    - lag이 계속 증가 → 처리량 제한 높이거나 Executor 추가
2. 초기 기동 시 주의
    - `startingOffsets=latest`면 기동 시 최신부터 읽어서 괜찮음
    - `startingOffsets=earliest`면 기동 시 오래된 데이터 한 번에 밀어넣음 → 첫 배치 거대해짐 → `maxOffsetsPerTrigger`이 특히 중요

**엔지니어 독백**

> 처리량 제한 없이 스트리밍 올리면 평소엔 괜찮다가 이벤트 세일 같은 트래픽 spike 때 첫 배치가 수백만 메시지로 커져서 OOM 나고 잡 죽는 거 봤어. maxOffsetsPerTrigger 설정 자체는 한 줄인데 안 해서 장애 나는 케이스가 너무 많아. 처음 설계할 때 무조건 넣는 게 맞아.

---

### 케이스 4 - 동일 Spark 앱에서 다중 스트리밍 쿼리 실행

#### 배경

하나의 SparkSession에서 여러 스트리밍 쿼리를 동시에 실행할 수 있다. 인프라 비용 절감, 공통 설정 공유, 관리 단순화가 장점이다. 하지만 하나의 쿼리가 자원을 독점하면 다른 쿼리가 굶는 문제가 생긴다.

```
# 동일 SparkSession에서 복수 쿼리 실행
spark = SparkSession.builder \
    .config("spark.scheduler.mode", "FAIR") \      # FAIR 스케줄러 활성화
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

# 쿼리 1: 주문 집계
query1 = order_stream \
    .groupBy("store_id") \
    .count() \
    .writeStream \
    .queryName("order_count") \    # 쿼리 이름 지정 (Spark UI 식별용)
    .outputMode("update") \
    .format("console") \
    .start()

# 쿼리 2: 유저 세션 추적
query2 = event_stream \
    .groupBy("user_id") \
    .mapGroupsWithState(...) \
    .writeStream \
    .queryName("user_session") \
    .outputMode("update") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoint/session") \
    .start()

# 두 쿼리 모두 완료될 때까지 대기
spark.streams.awaitAnyTermination()
```

- `spark.scheduler.mode=FAIR` → 여러 쿼리가 자원을 공평하게 나눠씀 (기본값 FIFO는 먼저 들어온 쿼리가 자원 독점)
- `.queryName("order_count")` → Spark UI에서 쿼리 식별자로 표시됨, 디버깅 필수
- `spark.streams.awaitAnyTermination()` → 어느 쿼리든 하나가 종료되면 드라이버 프로세스 종료

**실무 구축 시 고려사항**

1. 쿼리 격리 vs 통합 트레이드오프
    
    |방식|장점|단점|
    |---|---|---|
    |단일 앱에 여러 쿼리|비용 절감, 관리 단순|하나 죽으면 전체 영향|
    |쿼리별 별도 앱|완전 격리, 독립 배포|인프라 비용 증가|
    
2. 단일 앱 권장 케이스
    
    - 쿼리들이 같은 Kafka 토픽을 읽는 경우 (소스 공유)
    - 쿼리 처리량 비슷하고 중요도 동일한 경우
3. 별도 앱 권장 케이스
    
    - 중요도 다른 쿼리 혼재 (결제 집계 + 로그 집계)
    - 쿼리별 독립 배포, 독립 재시작이 필요한 경우

**엔지니어 독백**

> 단일 앱에 쿼리 여러 개 넣으면 비용은 절감되는데, 장애 영향 범위가 커져. 결제 관련 스트리밍이랑 로그 분석 스트리밍을 같은 앱에 넣었다가 로그 쿼리 버그로 OOM 나면서 결제 집계까지 같이 죽은 케이스 봤어. 중요도가 다른 쿼리는 무조건 분리하는 게 맞아. FAIR 스케줄러도 OOM 앞에서는 의미 없거든.

---

**성능 튜닝 설정값 요약**

|항목|설정값|기본값|권장값|
|---|---|---|---|
|셔플 파티션|`spark.sql.shuffle.partitions`|200|Executor × 코어 수|
|Dynamic Allocation|`spark.dynamicAllocation.enabled`|true|false (스트리밍)|
|Kafka 처리량 제한|`maxOffsetsPerTrigger`|무제한|배치 처리 시간 기준 계산|
|스케줄러|`spark.scheduler.mode`|FIFO|FAIR (다중 쿼리)|
|AQE|`spark.sql.adaptive.enabled`|false (3.0 이전)|true|
|GC|`spark.executor.extraJavaOptions`|-|`-XX:+UseG1GC`|
