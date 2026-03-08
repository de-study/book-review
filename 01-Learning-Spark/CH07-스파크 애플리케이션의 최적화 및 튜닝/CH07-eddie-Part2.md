# 01.Spark 조인 
## (1)조인 처리 종류  
```
join 실행
├── CASE01.한쪽 테이블이 작다 (기본 10MB 이하)
│   └── 브로드캐스트 해시 조인
├── CASE02.테이블이 메모리에 올라갈 만하다
│   └── 셔플 해시 조인
├── CASE03.둘 다 크다 + 등호 조건 있다
│   └── 셔플 소트 머지 조인
└── CASE04.등호 조건 없다 (부등호, LIKE, 크로스)
    ├── 한쪽 작다 → 브로드캐스트 네스티드 루프 조인
    └── 둘 다 크다 → 셔플 복제 네스티드 루프 조인
```

**1. 브로드캐스트 해시 조인**
- 셔플: X
- 조건: 한쪽 테이블이 작을 때(10MB이하)
- 동작: 작은 테이블을 해시맵으로 만들어 모든 Executor에 복사 → 큰 테이블이 로컬에서 해시맵 조회
- 관련 설정: `spark.sql.autoBroadcastJoinThreshold` → 기본값 10MB
- 실무: 가장 빠른 전략. 가능하면 이걸 유도하는 게 목표
```
Executor A                    Executor B
큰 테이블 Partition 1         큰 테이블 Partition 2
작은 테이블 해시맵 (복사본)    작은 테이블 해시맵 (복사본)
→ 로컬에서 바로 조회           → 로컬에서 바로 조회
```



**2. 셔플 해시 조인**
- 셔플: O
- 조건: 한쪽이 broadcast 임계값보단 크지만 메모리에 올라갈 만할 때
- 동작: 같은 키끼리 같은 파티션으로 셔플 → 작은 쪽을 해시맵으로 만들어 큰 쪽과 비교
- 실무: 브로드캐스트보단 느리지만 소트 머지보단 빠름. 메모리 충분할 때 유리
```
셔플로 같은 키 모음
→ Partition 1: user_id 1~1000
   작은 테이블 해시맵 구성
   큰 테이블에서 해시맵 조회
```



**3. 셔플 소트 머지 조인**
- 셔플: O
- 조건: 양쪽 다 크고 등호 조건 있을 때
- 동작: 같은 키끼리 셔플 → 양쪽 모두 정렬 → 정렬된 상태로 순차 비교
- 실무: 대용량 join의 기본 전략. 안정적이지만 정렬 비용 있음
```
셔플로 같은 키 모음
→ 양쪽 정렬
→ 포인터 두 개로 순차 비교 (머지)
   A: [1, 2, 3, 4, 5]
   B: [1, 2, 3, 4, 5]
       ↑  같으면 매칭, 작으면 전진
```

> - 브로드캐스트 해시 조인
> - 셔플 해시 조인
> - 셔플 소트 머지 조인
> 위 3개중 하나임(99%)

**4. 브로드캐스트 네스티드 루프 조인**
- 셔플: X
- 조건: 등호 조건 없을 때 + 한쪽 작을 때
- 동작: 작은 테이블 broadcast → 큰 테이블 각 row마다 작은 테이블 전체를 순회하며 비교
- 실무: 거의 안 씀. 부등호 join, cross join 시 등장


**5. 셔플 복제 네스티드 루프 조인**
- 셔플: O
- 조건: 등호 조건 없을 때 + 양쪽 다 클 때
- 동작: 셔플 후 각 row마다 상대 파티션 전체 순회
- 실무: 가장 느린 전략. 이게 선택됐다면 쿼리 설계를 재검토해야 함


**실무 핵심 포인트**
- Spark UI → SQL 탭에서 어떤 join 전략이 선택됐는지 확인 가능
- 브로드캐스트 유도가 최우선 목표
- 소트 머지가 선택됐는데 느리다면 버켓팅으로 셔플 제거 가능


## (2) 예시

### [1]브로드캐스트 해시 조인

시나리오:
```
orders 테이블 (500GB, 5억 건)     country_codes 테이블 (50KB, 250건)
Executor 1~100에 분산              Driver가 전체 보유 가능한 크기
```


**Step 1: 초기 상태 - 데이터 분산**
```
Executor 1                Executor 2                Executor 3
orders Partition 1        orders Partition 2        orders Partition 3
order_id | country_code   order_id | country_code   order_id | country_code
---------|------------    ---------|------------    ---------|------------
1        | KR             4        | US             7        | JP
2        | US             5        | KR             8        | KR
3        | JP             6        | JP             9        | US
```

country_codes는 250건짜리 작은 테이블:
```
Driver
country_code | country_name
-------------|-------------
KR           | Korea
US           | USA
JP           | Japan
...
```


**Step 2: 브로드캐스트 - 작은 테이블을 해시맵으로 변환 후 전송**
```
Driver가 country_codes를 해시맵으로 변환:
{
  "KR": "Korea",
  "US": "USA",
  "JP": "Japan"
}

→ 이 해시맵을 모든 Executor에 복사 (broadcast)
```

```
Executor 1                Executor 2                Executor 3
orders Partition 1        orders Partition 2        orders Partition 3
+                         +                         +
country_codes 해시맵      country_codes 해시맵      country_codes 해시맵
(복사본)                  (복사본)                  (복사본)
```


**Step 3: 각 Executor가 로컬에서 join 수행**
```
Executor 1
order_id=1, country_code=KR → 해시맵에서 KR 조회 → Korea
order_id=2, country_code=US → 해시맵에서 US 조회 → USA
order_id=3, country_code=JP → 해시맵에서 JP 조회 → Japan

결과:
order_id | country_code | country_name
---------|--------------|-------------
1        | KR           | Korea
2        | US           | USA
3        | JP           | Japan
```

셔플 없이 각 Executor가 독립적으로 로컬에서 처리 완료


**핵심**
- 네트워크 전송이 딱 한 번: 작은 테이블을 각 Executor에 복사할 때
- orders 데이터는 한 번도 이동 안 함
- 해시맵이라 조회가 O(1)

---

### [2]셔플 소트 머지 조인
시나리오:

```
orders 테이블 (500GB, 5억 건)      shipments 테이블 (400GB, 4억 건)
둘 다 커서 broadcast 불가
join 조건: orders.order_id = shipments.order_id
```


**Step 1: 초기 상태 - 데이터 분산**
```
Executor 1                    Executor 2                    Executor 3
orders Partition 1            orders Partition 2            orders Partition 3
order_id | amount             order_id | amount             order_id | amount
---------|-------             ---------|-------             ---------|-------
1        | 3000               4        | 1000               2        | 5000
6        | 2000               3        | 4000               5        | 3000

shipments Partition 1         shipments Partition 2         shipments Partition 3
order_id | status             order_id | status             order_id | status
---------|--------            ---------|--------            ---------|--------
3        | delivered          1        | pending            5        | delivered
5        | shipped            6        | delivered          2        | shipped
```

같은 order_id가 orders와 shipments에서 다른 Executor에 있음 join하려면 같은 order_id끼리 같은 Executor로 모아야 함


**Step 2: 셔플 - 같은 order_id를 같은 파티션으로**
```
order_id를 해시값으로 변환해서 파티션 결정
hash(order_id) % shuffle_partition_수 = 파티션 번호

order_id=1 → hash(1) % 3 = Partition 1으로
order_id=2 → hash(2) % 3 = Partition 2으로
order_id=3 → hash(3) % 3 = Partition 3으로
...
```

셔플 후:
```
Shuffle Partition 1           Shuffle Partition 2           Shuffle Partition 3
orders:                       orders:                       orders:
order_id=1, amount=3000       order_id=2, amount=5000       order_id=3, amount=4000
order_id=4, amount=1000       order_id=5, amount=3000       order_id=6, amount=2000

shipments:                    shipments:                    shipments:
order_id=1, status=pending    order_id=2, status=shipped    order_id=3, status=delivered
order_id=4, (없음)            order_id=5, status=delivered  order_id=6, status=delivered
```


**Step 3: 소트 - 각 파티션 내에서 order_id 기준 정렬**
```
Shuffle Partition 1
orders 정렬:     [1, 4]
shipments 정렬:  [1]

Shuffle Partition 2
orders 정렬:     [2, 5]
shipments 정렬:  [2, 5]

Shuffle Partition 3
orders 정렬:     [3, 6]
shipments 정렬:  [3, 6]
```


**Step 4: 머지 - 포인터 두 개로 순차 비교**
```
Partition 2 기준:
orders   포인터 →  [2, 5]
shipments 포인터 → [2, 5]

1. orders=2, shipments=2 → 같음 → 매칭
   결과: order_id=2, amount=5000, status=shipped

2. orders 포인터 전진, shipments 포인터 전진
   orders=5, shipments=5 → 같음 → 매칭
   결과: order_id=5, amount=3000, status=delivered
```



**브로드캐스트 해시 조인과 핵심 차이**
```
브로드캐스트 해시 조인:
- 작은 테이블 → 해시맵으로 전 Executor 복사
- 셔플 없음
- 조회 O(1)

셔플 소트 머지 조인:
- 양쪽 다 셔플 발생
- 양쪽 다 정렬 비용 발생
- 포인터로 순차 비교
- 대신 메모리 부담 없이 대용량 처리 가능
```

---


### [3]셔플 해시 조인

시나리오:
```
orders 테이블 (500GB, 5억 건)
products 테이블 (500MB, 100만 건)
→ products가 broadcast 임계값(10MB)은 넘지만 파티션 단위로 메모리에 올라갈 만한 크기
```


**Step 1: 초기 상태**
```
Executor 1                    Executor 2                    Executor 3
orders Partition 1            orders Partition 2            orders Partition 3
order_id | product_id         order_id | product_id         order_id | product_id
---------|----------          ---------|----------          ---------|----------
1        | P3                 4        | P1                 7        | P2
2        | P1                 5        | P3                 8        | P1
3        | P2                 6        | P2                 9        | P3

products Partition 1          products Partition 2          products Partition 3
product_id | name             product_id | name             product_id | name
-----------|-----             -----------|-----             -----------|-----
P3         | shoes            P1         | shirt            P2         | pants
...                           ...                           ...
```


**Step 2: 셔플 - 소트 머지와 동일**
```
hash(product_id) % 3 으로 파티션 결정
→ 같은 product_id끼리 같은 파티션으로 이동
```

```
Shuffle Partition 1           Shuffle Partition 2           Shuffle Partition 3
orders:                       orders:                       orders:
product_id=P1 rows            product_id=P2 rows            product_id=P3 rows

products:                     products:                     products:
P1 rows                       P2 rows                       P3 rows
```


**Step 3: 해시맵 구성 - 여기서 소트 머지와 갈린다**
```
소트 머지: 양쪽 다 정렬 → 포인터로 순차 비교
해시 조인: 작은 쪽(products)만 해시맵으로 구성 → 큰 쪽(orders)에서 조회
```

```
Shuffle Partition 1
products 파티션으로 해시맵 구성:
{
  "P1": {name: "shirt", price: 30000}
}

orders 각 row에서 product_id로 해시맵 조회:
order_id=2, product_id=P1 → 해시맵["P1"] → shirt
order_id=4, product_id=P1 → 해시맵["P1"] → shirt
order_id=8, product_id=P1 → 해시맵["P1"] → shirt
```


**3가지 전략 비교**
```
브로드캐스트 해시 조인
→ 셔플 없음, 작은 테이블 전체를 해시맵으로 전 Executor 복사

셔플 해시 조인
→ 셔플 있음, 파티션 단위로 작은 쪽을 해시맵 구성

셔플 소트 머지 조인
→ 셔플 있음, 양쪽 정렬 후 포인터 비교
```


**셔플 해시 조인이 소트 머지보다 빠른 이유와 한계**
빠른 이유:
- 정렬 비용 없음
- 해시맵 조회가 O(1)

한계:
- 파티션 단위 해시맵이 메모리에 올라가야 함
- 메모리 부족 시 Spill 발생 → 오히려 느려짐
- 그래서 Optimizer가 메모리 여유 있을 때만 선택

--- 
## (3)버켓팅
> 근데 레이지 액션이면 미리 조인 고려해서 파티셔닝 해놓으면 안되나 ?
> 그게 버켓팅이다.  


### 개념
**문제 상황**
소트 머지 조인은 매번 실행할 때마다:
```
읽기 → 셔플 (네트워크 전송) → 정렬 → 조인
```
같은 테이블을 매일 조인하는 파이프라인이면 매번 셔플 비용을 지불한다.



**버켓팅 개념**
테이블 저장 시 미리 특정 컬럼 기준으로 정렬+파티셔닝해서 저장해두는 것. 다음번 조인 시 셔플과 정렬 단계가 생략된다.
```
일반 저장:
저장: 그냥 저장
조인 시: 읽기 → 셔플 → 정렬 → 조인

버켓팅 저장:
저장: order_id 기준으로 정렬+파티셔닝해서 저장
조인 시: 읽기 → 조인 (셔플, 정렬 생략)
```



**조건**
버켓팅 효과가 나려면:
- 양쪽 테이블 모두 같은 컬럼, 같은 버켓 수로 버켓팅되어 있어야 함
- 버켓 수가 다르면 셔플 다시 발생

**실무 사용 시점**
- 매일 반복 실행되는 대용량 join
- 항상 같은 키로 join하는 테이블 (user_id, order_id 등)
- 한번 저장 비용을 감수하고 이후 조인 비용을 없애는 트레이드오프

### 사용법
> 버켓팅은 자동으로 되지 않는다. 저장 시 명시적으로 지정해야 한다.  
> 왜냐?  
> 버켓팅은 테이블 설계 단계에서 결정하는 것이다. 
> "이 테이블은 앞으로 항상 이 컬럼으로 조인할 것이다"라는 판단이 선행되어야 하기 때문에 Optimizer가 자동으로 결정할 수 없다.  
> AQE나 Catalyst Optimizer는 실행 중 플랜을 최적화하지만, 이미 저장된 파일의 구조 자체를 바꾸진 못한다.  
```
자동 (Optimizer/AQE가 알아서):
- 브로드캐스트 조인 전략 선택
- 셔플 파티션 수 조정
- skew 파티션 분할

수동 (엔지니어가 명시):
- 버켓팅 저장
- repartition / coalesce
- broadcast hint
- cache / persist
```

**코드. 버켓팅으로 저장**
```
# orders 테이블 버켓팅 저장
orders.write \
    .bucketBy(200, "order_id") \
    .sortBy("order_id") \
    .saveAsTable("orders_bucketed")

# shipments 테이블 버켓팅 저장
# 반드시 같은 컬럼, 같은 버켓 수
shipments.write \
    .bucketBy(200, "order_id") \
    .sortBy("order_id") \
    .saveAsTable("shipments_bucketed")
```



**버켓팅된 테이블 조인**
```
orders = spark.table("orders_bucketed")
shipments = spark.table("shipments_bucketed")

# 셔플, 정렬 없이 바로 조인
result = orders.join(shipments, "order_id")
result.explain()
```

explain() 결과 비교:
```
# 버켓팅 전
== Physical Plan ==
SortMergeJoin
+- Exchange (셔플 발생)
+- Exchange (셔플 발생)

# 버켓팅 후
== Physical Plan ==
SortMergeJoin
+- (셔플 없음)
+- (셔플 없음)
```



**버켓 수 결정 기준**
```
# 버켓 수 = spark.sql.shuffle.partitions 값과 맞추는 게 일반적
# 또는 전체 데이터 크기 / 128MB

# 500GB 테이블
# 500000MB / 128MB = 약 3900 → 200~400으로 잡는 경우 많음
orders.write \
    .bucketBy(200, "order_id") \
    .sortBy("order_id") \
    .saveAsTable("orders_bucketed")
```



**주의점**
```
# 버켓 수가 다르면 셔플 다시 발생 → 의미 없음
orders.write.bucketBy(200, "order_id")...   # 200개
shipments.write.bucketBy(100, "order_id")... # 100개 → 셔플 발생

# 반드시 같은 버켓 수, 같은 컬럼으로 저장해야 함
```


**실무 트레이드오프**
```
버켓팅 저장 시:
- 저장 시간 증가 (정렬 비용)
- 저장 공간 증가 (메타데이터)

버켓팅 조인 시:
- 셔플 없음
- 정렬 없음
- 매일 반복 실행되는 파이프라인이면 누적 절감 효과 큼
```


## (4)확인방법
> Spark UI 또는 explain()으로 확인한다.

**explain()으로 실행 계획 확인**
```
orders.join(products, "product_id").explain()
```

출력 결과에서 조인 전략 확인:
```
== Physical Plan ==
*(2) BroadcastHashJoin [product_id#1], [product_id#2], Inner, BuildRight
:- *(2) Scan parquet orders
+- BroadcastExchange HashedRelationBroadcastMode
   +- *(1) Scan parquet products
```

키워드로 판단:
```
BroadcastHashJoin        → 브로드캐스트 해시 조인
ShuffledHashJoin         → 셔플 해시 조인
SortMergeJoin            → 셔플 소트 머지 조인
BroadcastNestedLoopJoin  → 브로드캐스트 네스티드 루프 조인
CartesianProduct         → 셔플 복제 네스티드 루프 조인
```



**explain() 상세 모드**
```
# 더 자세한 정보
orders.join(products, "product_id").explain("extended")

# 포맷된 형태로 보기 (Spark 3.0+)
orders.join(products, "product_id").explain("formatted")
```


**실무에서는 Spark UI가 더 직관적**
- 브라우저에서 `localhost:4040` 접속
- SQL 탭 → 실행된 쿼리 클릭
- DAG 시각화에서 조인 전략 바로 확인 가능



# 02.스파크UI
## (1) UI 보는법

**Spark UI에서 보는 순서**
```
Jobs 탭
→ 느린 Job 확인

Stages 탭
→ 해당 Job의 느린 Stage 확인
→ Shuffle Spill 수치 확인

Stage 상세
→ Task 시간 분포 확인 (skew 여부)
→ GC Time 확인

Executors 탭
→ GC Time 비율 확인
→ OOM으로 죽은 Executor 확인
```

엔지니어 독백:
> "순서가 중요해. Jobs → Stages → Tasks 순으로 좁혀가는 거야. 전체 Job 느리면 Stage 레벨로 내려가고, Stage 느리면 Task 레벨로 내려가. Task 레벨에서 특정 Task만 느리면 skew, 전체 Task가 느리면 메모리/shuffle 문제야."



**접속**
```
로컬: http://localhost:4040
클러스터: http://<driver-node>:4040
```
---

**탭 구성과 역할**
1. Jobs
    - 전체 Job 목록과 상태 (실행중/완료/실패)
    - 각 Job의 Stage 수, Task 수, 소요 시간
    - 여기서 어떤 Action이 느린지 파악
2. Stages
    - 전체 Stage 목록
    - 각 Stage의 Task 분포, 셔플 읽기/쓰기 크기
    - 여기서 셔플이 얼마나 발생했는지, skew 있는지 파악
    - Task 완료 시간이 특정 Task만 튀면 skew
3. Tasks (Stage 클릭 시)
    - Stage 안의 개별 Task 상세
    - Duration, GC Time, Shuffle Read/Write, Spill 크기
    - 여기서 특정 Task만 오래 걸리는지 확인
4. Storage
    - cache()/persist()된 DataFrame 목록
    - 메모리/디스크 점유 크기
    - 캐시가 얼마나 메모리를 쓰는지 확인
5. Environment
    - 현재 적용된 Spark 설정값 전체
    - spark.sql.shuffle.partitions, executor.memory 등 실제 적용값 확인
    - 설정이 제대로 반영됐는지 검증할 때 사용
6. Executors
    - 각 Executor의 상태, 메모리 사용량, GC 시간
    - Task 성공/실패 수
    - 특정 Executor만 GC 시간이 길면 메모리 부족 신호
7. SQL
    - DataFrame/SQL 쿼리의 실행 계획 시각화
    - 어떤 조인 전략이 선택됐는지 확인
    - explain()의 시각화 버전

**실무 디버깅 순서**
```
느린 Job 발견 (Jobs 탭)
→ 어떤 Stage가 느린지 확인 (Stages 탭)
→ 해당 Stage에서 특정 Task만 느린지 확인 (Tasks 탭)
→ 셔플 크기, Spill 여부 확인
→ 조인 전략 확인 (SQL 탭)
→ 메모리/GC 상태 확인 (Executors 탭)
```

### 예시 

```
1. Jobs 탭
새벽 2시. 배치 파이프라인이 평소보다 3배 느리다는 알람이 왔다.

Jobs 탭 열었다.
Job 목록 보니까 Job 12번이 2시간째 실행 중이다.
나머지 Job들은 다 5분 이내에 끝났는데.

Description 컬럼 보니까 "csv at pipeline.py:47" 이다.
47번째 줄... write() 호출하는 부분이네.

Job 12번 클릭해서 Stage로 들어가자.




2. Stages 탭
Job 12번 안에 Stage가 4개다.
Stage 0, 1, 2는 다 2분 이내로 끝났다.
Stage 3이 1시간 58분째 실행 중이다.

Stage 3 보니까:
- Tasks: 199/200 succeeded
- 1개 Task가 아직 실행 중

Shuffle Read Size 컬럼 보니까:
- 일반 Task들: 평균 128MB
- 실행 중인 Task: 47GB

skew다. 특정 키에 데이터가 몰렸다.
Stage 3 클릭해서 Task 상세 보자.
   
   
   
   
3.Tasks탭
Task 목록 Duration 기준으로 정렬했다.

Task 187번:
- Duration: 1h 58m
- Shuffle Read: 47GB
- GC Time: 23m
- Spill (disk): 12GB

나머지 Task들:
- Duration: 평균 1m 30s
- Shuffle Read: 평균 128MB
- GC Time: 거의 없음
- Spill: 없음

GC Time이 23분이고 Spill이 12GB면
메모리가 모자라서 디스크로 넘치고 있는 거다.

groupBy 키가 뭔지 코드 다시 봐야겠다.
아 user_id로 groupBy하고 있네.
user_id=null이 전체 데이터의 30%를 차지하고 있었던 거다.
데이터 확인해보자.



4.Storage탭
다른 파이프라인 디버깅 중이었다.
중간 집계 DataFrame을 cache()했는데 전체 Job이 점점 느려지고 있었다.

Storage 탭 열었다.
RDD 목록 보니까:

DataFrame 1: 45GB / 메모리 45GB (100% cached)
DataFrame 2: 38GB / 메모리 20GB, 디스크 18GB (일부만 메모리)
DataFrame 3: 52GB / 디스크 52GB (전부 디스크)

아. 캐시를 너무 많이 걸어놨다.
총 135GB인데 Executor 메모리가 80GB밖에 안 된다.

DataFrame 1이 제일 먼저 캐시됐고
DataFrame 2, 3이 캐시되면서 1을 메모리에서 밀어냈겠다.
DataFrame 3은 디스크에만 있으니까 읽을 때마다 디스크 I/O 발생하는 거다.

unpersist() 순서 조정하고
꼭 필요한 것만 캐시하도록 코드 수정해야겠다.




5. Environment 탭
신규 클러스터로 마이그레이션 후 갑자기 Job이 느려졌다.

코드는 동일한데 왜 느리지?
Environment 탭에서 설정값 확인해봤다.

spark.sql.shuffle.partitions = 200
spark.sql.adaptive.enabled = false
spark.executor.memory = 4g

아. AQE가 꺼져 있다.
이전 클러스터는 Spark 3.2였는데 이건 Spark 2.4네.
AQE 자체가 없는 버전이다.

그리고 executor.memory가 4g밖에 안 된다.
이전 클러스터는 16g였는데.

클러스터 설정 다시 잡아야겠다.




6. Executors 탭
Job은 끝나는데 유독 특정 시간대에 느리다는 제보가 들어왔다.

Executors 탭 열었다.

Executor 목록:
executor 0: GC Time 2m / Task Time 45m → 정상
executor 1: GC Time 2m / Task Time 43m → 정상
executor 2: GC Time 31m / Task Time 45m → 이상
executor 3: GC Time 28m / Task Time 44m → 이상
executor 4: GC Time 3m  / Task Time 46m → 정상

executor 2, 3번이 GC Time이 전체 시간의 60%가 넘는다.
메모리가 부족해서 GC가 계속 돌고 있는 거다.

Tasks 컬럼 보니까 executor 2, 3번에 큰 파티션이 몰리고 있다.
skew + 메모리 부족의 복합 문제다.

executor.memory 올리거나 repartition으로 데이터 균등 분배해야겠다.



7. SQL 탭
join 쿼리가 예상보다 훨씬 느리다.

SQL 탭 열었다.
쿼리 클릭하니까 DAG가 나온다.

SortMergeJoin이 찍혀 있다.
products 테이블인데 왜 SortMergeJoin이지?

DAG에서 Exchange (셔플) 노드가 양쪽에 다 있다.
products 테이블 크기 확인해봤더니 800MB다.

autoBroadcastJoinThreshold가 기본값 10MB라서
800MB짜리를 broadcast 못 하고 SortMergeJoin 선택한 거다.

임계값 올려주거나 hint 줘서 broadcast 유도하자.   
```


### 언급된 설정값 
**1. spark.sql.shuffle.partitions**
- 의미: 셔플 발생 후 생성되는 파티션 수
- 기본값: 200
- 독백 상황: Environment 탭에서 200으로 되어있던 것 확인
- 변경:
```
spark.conf.set("spark.sql.shuffle.partitions", "400")
```
- 이유: 데이터가 크면 200개 파티션으로는 파티션당 크기가 너무 커서 Spill 발생



**2. spark.sql.adaptive.enabled**
- 의미: AQE 전체 on/off
- 기본값: Spark 3.0+ 에서 true, 2.x에서 없음
- 독백 상황: 마이그레이션 후 Spark 2.4 클러스터에서 false 확인
- 변경:
```
spark.conf.set("spark.sql.adaptive.enabled", "true")
```
- 이유: AQE 꺼지면 셔플 파티션 수 고정, 자동 skew 처리 안 됨, 브로드캐스트 자동 전환 안 됨



**3. spark.executor.memory**
- 의미: Executor 하나에 할당되는 메모리
- 기본값: 1g
- 독백 상황: 신규 클러스터에서 4g로 설정되어 있었고 이전 클러스터는 16g였음
- 변경:
```
spark.conf.set("spark.executor.memory", "16g")
```
- 이유: 메모리 부족 시 GC 과부하, Spill 발생 → 처리 속도 급감



**4. spark.sql.autoBroadcastJoinThreshold**
- 의미: 이 크기 이하인 테이블은 자동으로 브로드캐스트 해시 조인 선택
- 기본값: 10MB (10485760 bytes)
- 독백 상황: products 테이블이 800MB라서 기본값 10MB 초과 → SortMergeJoin 선택됨
- 변경:
```
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1g")
```
- 이유: 800MB짜리 테이블을 broadcast 가능하게 임계값 올림. 단, Executor 메모리 여유 있을 때만 올려야 함


**5. spark.memory.fraction**
- 의미: Executor 메모리 중 Spark 실행/저장에 쓰는 비율
- 기본값: 0.6 (60%)
- 독백 상황: 직접 언급은 안 됐지만 GC Time 과다, Spill 발생 시 연관된 설정
- 변경:
```
spark.conf.set("spark.memory.fraction", "0.7")
```
- 이유: GC가 과도하게 발생하면 Spark 실행 메모리 비율을 높여 Spill 줄임. 단, 너무 높이면 사용자 코드 메모리 부족



**6. spark.memory.storageFraction**
- 의미: spark.memory.fraction 중 캐시/저장에 쓰는 비율
- 기본값: 0.5 (실행 메모리와 저장 메모리 반반)
- 독백 상황: Storage 탭에서 캐시가 메모리를 과도하게 점유해서 실행 메모리를 밀어내는 상황
- 변경:
```
spark.conf.set("spark.memory.storageFraction", "0.3")
```
- 이유: 캐시보다 실행 메모리가 더 중요한 상황에서 실행 메모리 비율을 높임



**설정값 간 메모리 구조 관계**
```
Executor Memory (spark.executor.memory = 16g)
├── Reserved Memory: 300MB (고정, 시스템용)
└── Usable Memory: 15.7g
    ├── Spark Memory (fraction=0.6): 9.4g
    │   ├── Storage Memory (storageFraction=0.5): 4.7g → 캐시
    │   └── Execution Memory (나머지): 4.7g → 셔플, 조인, 정렬
    └── User Memory (나머지 0.4): 6.3g → UDF, 사용자 코드
```
