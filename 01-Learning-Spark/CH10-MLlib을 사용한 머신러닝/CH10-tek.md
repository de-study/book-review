# Learning Spark - MLlib을 사용한 머신러닝

> 출처: Learning Spark (2nd Edition) | 실무 데이터 엔지니어링 관점 정리

---

## 목차

1. [머신러닝이란 무엇인가?](#1-머신러닝이란-무엇인가)
   - 지도 학습
   - 비지도 머신러닝
   - 왜 머신러닝을 위한 스파크인가?
2. [머신러닝 파이프라인 설계](#2-머신러닝-파이프라인-설계)
   - 데이터 수집 및 탐색
   - 학습 및 테스트 데이터세트 생성
   - 변환기를 사용하여 기능 준비
   - 선형 회귀 이해하기
   - 추정기를 사용하여 모델 구축
   - 파이프라인 생성
   - 모델 평가
   - 모델 저장 및 로드
3. [하이퍼파라미터 튜닝](#3-하이퍼파라미터-튜닝)
   - 트리 기반 모델
   - K-폴드 교차 검증
   - 파이프라인 최적화
4. [요약](#4-요약)

---

## 전체 구조 개요

```
MLlib 머신러닝 파이프라인 전체 흐름
==========================================================================

[원시 데이터]
    │
    ▼
[데이터 수집 및 탐색]
  - 스키마 확인, 결측값 처리, 분포 파악
    │
    ▼
[학습/테스트 분리]
  - randomSplit([0.8, 0.2])
    │
    ▼
[변환기 (Transformer)]
  - StringIndexer, VectorAssembler, StandardScaler 등
  - fit() 없이 transform()만 수행
    │
    ▼
[추정기 (Estimator)]
  - LinearRegression, RandomForestClassifier 등
  - fit()으로 학습 → 모델(Transformer) 반환
    │
    ▼
[파이프라인 (Pipeline)]
  - 변환기 + 추정기를 단일 객체로 묶음
  - pipeline.fit(train) → pipelineModel
    │
    ▼
[모델 평가]
  - RegressionEvaluator, BinaryClassificationEvaluator 등
    │
    ▼
[하이퍼파라미터 튜닝]
  - CrossValidator + ParamGridBuilder
    │
    ▼
[모델 저장 / 로드]
  - pipelineModel.save() / PipelineModel.load()

==========================================================================

                      [MLlib 주요 알고리즘 분류]

    지도 학습                              비지도 학습
    ├─ 회귀: LinearRegression              ├─ 클러스터링: KMeans
    ├─ 분류: LogisticRegression            ├─ 차원 축소: PCA
    └─ 앙상블: RandomForest, GBT           └─ 연관 규칙: FPGrowth
```

---

## 1. 머신러닝이란 무엇인가?

머신러닝은 명시적인 규칙 없이 데이터에서 패턴을 학습하여 예측이나 결정을 내리는 기술.
스파크 MLlib은 분산 환경에서 대규모 데이터셋에 머신러닝을 적용할 수 있도록 설계된 라이브러리.

### 1-1. 지도 학습

레이블(정답)이 있는 데이터를 사용해 입력과 출력 사이의 관계를 학습하는 방식.

- **목적**
  - 새로운 입력 데이터에 대해 정확한 출력값(레이블)을 예측
  - 분류(Classification)와 회귀(Regression) 두 유형으로 나뉨
- **이유**
  - 역사적으로 레이블된 데이터가 존재하는 경우, 명시적 규칙보다 패턴 학습이 더 정확한 예측 가능
  - 데이터가 많을수록 모델이 일반화되어 미지 데이터에도 잘 작동
- **결과**
  - 분류: 이메일 스팸 여부, 이탈 사용자 예측 등 이산 레이블 출력
  - 회귀: 주택 가격, 매출 예측 등 연속 수치 출력
- **장점**
  - 명확한 평가 기준(정확도, RMSE 등)이 존재해 모델 성능 측정이 직관적
  - 레이블 정보를 활용하므로 비지도 대비 예측 정확도가 높음
- **단점**
  - 레이블 생성 비용이 큼 (전문가 검토, 직접 수집 등)
  - 레이블 품질이 나쁘면 모델 성능이 크게 저하됨

```
지도 학습 유형 비교
--------------------------------------------------------------
유형        | 출력          | 알고리즘 예시
-----------|--------------|-----------------------------------
분류        | 이산 레이블     | LogisticRegression, Naive Bayes
회귀        | 연속 수치      | LinearRegression
--------------------------------------------------------------
```

### 1-2. 비지도 머신러닝

레이블 없이 데이터 자체의 구조나 패턴을 발견하는 방식.

- **목적**
  - 데이터 내에 숨겨진 그룹, 구조, 패턴을 자동으로 탐색
  - 레이블 확보가 어렵거나 불가능한 상황에서의 분석
- **이유**
  - 실무에서 레이블 없는 데이터가 훨씬 많음
  - 클러스터링으로 고객 세분화, 이상 탐지 등 레이블 없이도 유의미한 인사이트 도출 가능
- **결과**
  - 클러스터링: 유사한 데이터끼리 그룹화 (예: KMeans)
  - 차원 축소: 고차원 데이터를 저차원으로 압축 (예: PCA)
  - 연관 규칙: 함께 등장하는 항목 패턴 발견 (예: FPGrowth)
- **장점**
  - 레이블 없이도 데이터 탐색 가능
  - 전처리/EDA 단계에서 데이터 구조 파악에 유용
- **단점**
  - 평가 기준이 주관적이고 명확하지 않음
  - 결과 해석에 도메인 지식이 필요

### 1-3. 왜 머신러닝을 위한 스파크인가?

단일 머신으로는 처리하기 어려운 수백 GB ~ 수 TB 규모의 학습 데이터를 분산 처리하기 위해.
`spark.ml`을 사용하면 데이터 과학자는 단일 시스템에 맞게 데이터 다운샘플링할 필요 없이 데이터 준비 및 모델 구축을 하나의 에코시스템으로 사용 가능.
`spark.ml`은 모델이 보유한 데이터 포인트 수에 따라 선형으로 확장되는 O(n) 확장에 중점을 두어 방대한 양의 데이터로 확장 가능.

- **목적**
  - 대규모 데이터셋에서 머신러닝 모델을 학습시키는 분산 컴퓨팅 환경 제공
  - 데이터 전처리부터 모델 학습, 서빙까지 하나의 플랫폼에서 처리
- **이유**
  - 단일 머신의 메모리 한계를 넘어서는 데이터는 분산 학습이 필수
  - 이미 Spark로 ETL 파이프라인이 구축된 환경에서 별도 시스템 없이 MLlib 활용 가능
- **결과**
  - 수십억 건의 학습 데이터를 클러스터 전체 메모리를 활용해 처리
  - Spark DataFrame과 완전히 통합되어 전처리-학습-평가를 하나의 코드베이스에서 처리 가능
- **장점**
  - 데이터 이동 없이 Spark 클러스터에서 바로 학습 가능 (ETL → 학습 연속 처리)
  - Structured Streaming과 통합하여 실시간 스코어링 파이프라인 구성 가능
  - DataFrame API 기반으로 SQL, 전처리, 학습을 동일한 코드 스타일로 작성
- **단점**
  - 딥러닝 지원이 제한적 (TensorFlow/PyTorch 수준 미달)
  - 단일 머신 scikit-learn 대비 오버헤드가 있어 소규모 데이터에서는 비효율

---

## 2. 머신러닝 파이프라인 설계

MLlib 파이프라인은 변환기(Transformer)와 추정기(Estimator)를 순서대로 연결한 워크플로우.
각 단계를 파이프라인으로 묶으면 학습/추론 과정을 단일 객체로 관리할 수 있음.

### 2-1. 데이터 수집 및 탐색

모델 학습 전, 데이터의 분포와 품질을 파악하는 단계.
잘못된 데이터 위에 쌓은 모델은 운영에서 반드시 문제를 일으킴.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan

spark = SparkSession.builder.appName("MLPipeline").getOrCreate()

# 데이터 로드
df = spark.read.format("parquet").load("/data/airbnb/listings/")

# 스키마 및 기본 통계 확인
df.printSchema()
df.describe().show()

# 결측값 컬럼별 확인
df.select([
    count(when(col(c).isNull() | isnan(c), c)).alias(c)
    for c in df.columns
]).show()

# 타겟 변수 분포 확인 (price)
df.select("price").summary().show()
```

> **트러블 로그** — EDA 없이 바로 모델을 돌리면 결측값이나 이상치가 학습 데이터에 그대로 섞임.
> 예: price 컬럼에 0이나 999999 같은 이상값이 포함된 상태로 LinearRegression을 학습하면
> RMSE가 실제보다 수십 배 크게 나와 모델이 쓸모없어지는 경우가 흔함.
> 학습 전 `describe()`와 결측값 비율 확인을 습관화할 것.

### 2-2. 학습 및 테스트 데이터세트 생성

모델의 일반화 성능을 측정하기 위해 데이터를 학습용과 테스트용으로 분리.
테스트 세트는 최종 평가 시점까지 절대 모델 학습에 사용하지 않아야 함.

- **목적**
  - 학습 데이터로 만든 모델이 미지 데이터에서도 잘 동작하는지 검증
  - 과적합(overfitting) 여부를 객관적으로 측정
- **이유**
  - 학습 데이터로만 평가하면 모델이 암기한 것인지 일반화된 것인지 구분 불가
  - 테스트 세트를 별도로 확보해야 실제 운영 성능을 사전에 추정 가능
- **결과**
  - 지정한 비율(예: 8:2)로 무작위 분할된 두 개의 DataFrame 반환
  - `seed` 고정으로 재현 가능한 분할 보장
- **장점**
  - `randomSplit` 한 줄로 분할 가능, 별도 라이브러리 불필요
  - `seed` 지정으로 동일 데이터셋 재생성 가능 (실험 재현성)
- **단점**
  - 단순 랜덤 분할은 클래스 불균형 문제를 고려하지 않음
  - 시계열 데이터에는 랜덤 분할 대신 시간 기준 분할 사용 필요

```python
# 결측값 제거 후 분할
df_clean = df.na.drop()

# 8:2 비율로 학습/테스트 분리 (seed 고정으로 재현성 확보)
train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)

print(f"학습 데이터: {train_df.count()}건")
print(f"테스트 데이터: {test_df.count()}건")
```

> **트러블 로그** — `seed`를 고정하지 않으면 실행할 때마다 분할이 달라져 실험 재현이 불가.
> 예: 동료에게 "테스트 RMSE가 150이었다"고 공유했는데 동료가 재실행하면 다른 RMSE가 나와
> 모델 성능 논의가 어긋나는 상황이 발생함.
> 항상 `seed=42` 같은 고정값을 사용하고, 분할 결과 건수를 로그로 남길 것.

```
[이그제큐터 수 변경 시 randomSplit 재현성 문제]
--------------------------------------------------------------
핵심: seed=42는 "전체 데이터를 동일하게 섞어라"가 아니라
      "각 파티션 안에서 동일하게 섞어라"는 의미

데이터: row1~row8 (총 8행), randomSplit([0.8, 0.2], seed=42)

[케이스 1] 이그제큐터 2개 → 파티션 2개
  파티션A: [row1, row2, row3, row4]
  파티션B: [row5, row6, row7, row8]

  seed=42 적용 (각 파티션 내에서 독립적으로)
    파티션A → train: row1,2,3  /  test: row4
    파티션B → train: row5,6,7  /  test: row8

  최종: train={row1,2,3,5,6,7}  test={row4,row8}

[케이스 2] 이그제큐터 4개로 변경 → 파티션 4개
  파티션A: [row1, row2]       ← 파티션이 잘게 나뉨
  파티션B: [row3, row4]          파티션 안의 행 구성이 달라짐
  파티션C: [row5, row6]
  파티션D: [row7, row8]

  seed=42 적용 (각 파티션 내에서 독립적으로)
    파티션A → train: row1  /  test: row2   ← 같은 seed인데 결과가 다름!
    파티션B → train: row3  /  test: row4
    파티션C → train: row5  /  test: row6
    파티션D → train: row7  /  test: row8

  최종: train={row1,3,5,7}  test={row2,4,6,8}

→ seed는 동일한 42인데, 파티션 구조가 달라져 완전히 다른 분할 결과
--------------------------------------------------------------

해결책: 한 번만 분할하고 파일로 저장
--------------------------------------------------------------
[최초 1회]
  df.randomSplit([0.8, 0.2], seed=42)
       │
       ├─► train_df.write.parquet("/data/split/train/")
       └─► test_df.write.parquet("/data/split/test/")

[이후 매 실험]
  train_df = spark.read.parquet("/data/split/train/")
  test_df  = spark.read.parquet("/data/split/test/")
       │
       └─► 파일에 행이 고정되므로 클러스터 구성이 바뀌어도 항상 동일한 결과
--------------------------------------------------------------
```

```python
# 최초 1회: 분할 후 저장
train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
train_df.write.mode("overwrite").parquet("/data/split/train/")
test_df.write.mode("overwrite").parquet("/data/split/test/")

# 이후 매 실험: 저장된 파일에서 로드
train_df = spark.read.parquet("/data/split/train/")
test_df  = spark.read.parquet("/data/split/test/")
```

### 2-3. 변환기를 사용하여 기능 준비

변환기(Transformer)는 `transform()` 메서드로 DataFrame을 다른 DataFrame으로 변환하는 컴포넌트.
`fit()` 과정 없이 규칙 기반으로 변환하거나, `fit()`으로 통계를 학습한 후 적용하는 두 종류가 있음.

- **목적**
  - 원시 컬럼을 머신러닝 알고리즘이 소비할 수 있는 수치형 벡터로 변환
  - 문자열 → 인덱스, 다중 컬럼 → 단일 벡터, 수치 정규화 등을 처리
- **이유**
  - MLlib 알고리즘은 입력으로 단일 벡터 컬럼(`features`)을 요구
  - 스케일이 다른 피처를 그대로 넣으면 특정 피처가 모델을 지배하는 문제 발생
- **결과**
  - 각 변환기가 새 컬럼을 DataFrame에 추가하는 방식으로 동작
  - 최종적으로 `VectorAssembler`가 모든 수치 피처를 하나의 벡터로 합침
- **장점**
  - 파이프라인에 포함시키면 학습/추론 시 동일한 변환 자동 적용
  - 각 변환기를 독립적으로 교체/추가 가능
- **단점**
  - `StringIndexer`는 학습 시 없던 카테고리 값을 추론 시 만나면 에러 발생
  - 피처 수가 많아질수록 `VectorAssembler` 설정이 복잡해짐

```python
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler

# 문자열 컬럼 → 숫자 인덱스 변환
indexer = StringIndexer(
    inputCol="room_type",
    outputCol="room_type_index",
    handleInvalid="skip",  # 학습 시 없던 카테고리는 스킵
)

# 여러 피처 컬럼 → 단일 벡터
assembler = VectorAssembler(
    inputCols=["room_type_index", "accommodates", "bathrooms", "bedrooms"],
    outputCol="features",
)

# 피처 정규화 (평균 0, 표준편차 1)
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withStd=True,
    withMean=True,
)
```

| 변환기 | 입력 | 출력 | 주요 용도 |
|--------|------|------|----------|
| StringIndexer | 문자열 컬럼 | 숫자 인덱스 | 카테고리 변수 인코딩 |
| VectorAssembler | 다수 수치 컬럼 | 벡터 컬럼 | 피처 결합 |
| StandardScaler | 벡터 컬럼 | 정규화 벡터 | 스케일 통일 |
| OneHotEncoder | 숫자 인덱스 | 이진 벡터 | 순서 없는 카테고리 처리 |

### 2-4. 선형 회귀 이해하기

선형 회귀는 입력 피처(X)와 연속 출력값(y) 사이의 선형 관계를 학습하는 가장 기본적인 회귀 알고리즘.
실무에서는 복잡한 모델 이전에 베이스라인으로 먼저 시도함.

- **목적**
  - 입력 피처와 타겟 변수 사이의 선형 관계를 수식으로 모델링
  - 복잡한 모델 이전 베이스라인 성능 측정
- **이유**
  - 단순하고 해석 가능해 각 피처의 영향력을 계수(coefficient)로 직접 확인 가능
  - 학습 비용이 낮아 빠르게 첫 번째 모델을 얻을 수 있음
- **결과**
  - 각 피처에 대한 계수(coefficient)와 절편(intercept) 학습
  - 예측값 = 피처₁ × 계수₁ + 피처₂ × 계수₂ + ... + 절편
- **장점**
  - 해석이 직관적 (계수가 클수록 해당 피처의 영향이 큼)
  - 학습/추론 속도가 빠르고 메모리 효율적
- **단점**
  - 비선형 관계를 표현하지 못함
  - 이상치에 민감하게 반응해 회귀선이 크게 왜곡될 수 있음

모델에 대한 계수와 절편을 추정하는 프로세스: 모델에 대한 매개변수 학습 (=피팅)
```
선형 회귀 모델 구조
--------------------------------------------------------------
예측값 = β₀ + β₁·accommodates + β₂·bedrooms + β₃·bathrooms + ...
        ↑    ↑   ↑             ↑   ↑          ↑   ↑
       절편  계수 피처            계수 피처        계수 피처

데이터는 고정, 계수는 학습으로 결정
  - β₁, β₂, β₃ (계수) 
    — 학습 대상 파라미터. 모델이 학습하면서 최적값을 찾아가는 값. 처음엔 0이었다가 fit() 과정에서 RSS를 최소화하도록 조정됨.
  - accommodates, bedrooms, bathrooms 
    — 입력 피처(데이터). 우리가 가진 데이터프레임의 컬럼값. 학습/추론 시 그대로 읽어서 수식에 넣는 값. 모델이 바꾸지 않음.

목적함수: 잔차제곱합(RSS) 최소화
  RSS = Σ(실제값 - 예측값)²

정규화 옵션:
  - elasticNetParam=0.0 → Ridge (L2): 계수를 0에 가깝게 축소
  - elasticNetParam=1.0 → Lasso (L1): 중요하지 않은 계수를 정확히 0으로
--------------------------------------------------------------
```

### 2-5. 추정기를 사용하여 모델 구축

추정기(Estimator)는 `fit()` 메서드로 데이터를 학습하고 Transformer(모델)를 반환하는 컴포넌트.
MLlib의 모든 학습 알고리즘은 추정기 인터페이스를 구현함.

- **목적**
  - 학습 데이터에서 모델 파라미터(계수, 트리 구조 등)를 최적화
  - 학습 완료 후 반환된 모델 객체로 새 데이터에 대한 예측 수행
- **이유**
  - 변환기와 추정기를 동일한 인터페이스로 통일해 파이프라인에 혼용 가능
  - `fit()` 결과로 모델 객체를 얻으면 이후 `transform()`으로 추론
- **결과**
  - `estimator.fit(train_df)` → `Model` 객체 반환
  - `model.transform(test_df)` → 예측값이 담긴 DataFrame 반환
- **장점**
  - 학습/추론 인터페이스 통일로 코드 구조가 단순
  - 모델 객체를 저장하고 로드하여 학습과 추론을 분리 가능
- **단점**
  - `fit()`은 전체 학습 데이터를 소비하므로 데이터 크기에 따라 시간이 길어짐
  - 하이퍼파라미터 기본값이 항상 최적은 아니며, 튜닝 없이는 성능이 제한적

```python
from pyspark.ml.regression import LinearRegression

lr = LinearRegression(
    featuresCol="scaled_features",
    labelCol="price",
    maxIter=100,
    regParam=0.1,          # 정규화 강도
    elasticNetParam=0.5,   # L1/L2 혼합 비율
)

# 학습: Estimator → Model(Transformer)
lr_model = lr.fit(train_df)

# 계수 확인
print(f"계수: {lr_model.coefficients}")
print(f"절편: {lr_model.intercept}")
```

### 2-6. 파이프라인 생성

파이프라인은 여러 변환기와 추정기를 순서대로 연결한 단일 워크플로우 객체.
`pipeline.fit(train)` 한 번으로 모든 단계의 학습과 적용이 순서대로 처리됨.

- **목적**
  - 전처리 → 학습 단계를 하나의 객체로 묶어 관리
  - 학습/추론 시 동일한 전처리 로직이 자동으로 적용되도록 보장
- **이유**
  - 전처리와 모델을 분리하면 추론 시 전처리 누락 실수가 발생하기 쉬움
  - 파이프라인으로 묶으면 학습 데이터의 통계(스케일러 평균/표준편차 등)가 모델에 함께 저장됨
- **결과**
  - `pipeline.fit(train_df)` 실행 시 각 단계를 순서대로 처리
  - 변환기는 `transform()`, 추정기는 `fit()` 후 `transform()` 자동 적용
  - 반환된 `PipelineModel`은 추론 시 `transform()` 하나로 동일 변환 재현
- **장점**
  - 학습/추론 코드 일관성 보장 (전처리 누락 방지)
  - 파이프라인 전체를 하나의 파일로 저장/로드 가능
- **단점**
  - 중간 단계 디버깅이 어려움 (어느 단계에서 문제가 생겼는지 추적 필요)
  - 파이프라인 내 특정 단계만 교체하려면 전체 재정의 필요

```python
from pyspark.ml import Pipeline

# 파이프라인 구성: 순서대로 실행됨
pipeline = Pipeline(stages=[
    indexer,          # StringIndexer
    assembler,        # VectorAssembler
    scaler,           # StandardScaler
    lr,               # LinearRegression (추정기)
])

# 학습: 모든 단계를 순서대로 처리
pipeline_model = pipeline.fit(train_df)

# 추론: 동일한 전처리 + 예측 한 번에 처리
predictions = pipeline_model.transform(test_df)
predictions.select("price", "prediction").show(5)
```

```
파이프라인 fit() 내부 동작
--------------------------------------------------------------
stage 1: StringIndexer   → fit(train) + transform(train)
stage 2: VectorAssembler → transform(train)          ← 변환기는 fit 없음
stage 3: StandardScaler  → fit(train) + transform(train)
stage 4: LinearRegression → fit(train) → lr_model 반환

PipelineModel.transform(test):
  → 위 4단계를 저장된 파라미터로 순서대로 재실행
--------------------------------------------------------------
```

#### 원-핫 인코딩(OHE)을 포함한 복잡한 파이프라인

`StringIndexer`만으로는 범주형 변수에 순서 관계가 생기는 문제가 있음.
예: dog=0, cat=1, fish=2 로 인코딩하면 선형 모델이 "fish > cat > dog" 라는 잘못된 순서를 학습할 수 있음.
`OneHotEncoder`를 추가하면 각 범주를 독립적인 차원으로 표현해 이 문제를 해결함.

```
StringIndexer + OneHotEncoder 변환 과정 (Animal 컬럼 예시)
--------------------------------------------------------------
원본: Animal = [dog, cat, fish]

[1단계] StringIndexer
  dog  → 0
  cat  → 1
  fish → 2

[2단계] OneHotEncoder
  0 (dog)  → [1, 0]   ← 범주 수(3) - 1 = 2차원 벡터
  1 (cat)  → [0, 1]      마지막 범주(fish)는 [0, 0]으로 표현
  2 (fish) → [0, 0]      (다중공선성 방지를 위해 하나 제거)

결과: 순서 관계 없이 각 범주가 독립적인 차원으로 표현됨
--------------------------------------------------------------
```

OHE 이후처럼 대부분의 값이 0인 경우, Spark는 내부적으로 **SparseVector**를 사용해 메모리를 절약함.

```
DenseVector vs SparseVector
--------------------------------------------------------------
DenseVector: 모든 값을 순서대로 저장
  [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
  → 0이 대부분이어도 10개 값 전부 메모리에 보관

SparseVector: 0이 아닌 값의 인덱스와 값만 저장
  (size=10, indices=[0], values=[1.0])
  → "총 10개 중 인덱스 0번만 1.0, 나머지는 모두 0"

OHE 결과처럼 피처 수가 수백 개인 경우:
  DenseVector → 수백 개의 0값을 전부 메모리에 보관
  SparseVector → 실제 값이 있는 위치만 기록, 메모리 대폭 절감
--------------------------------------------------------------
```

```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression

# 1단계: 문자열 → 인덱스
indexer = StringIndexer(
    inputCol="animal",
    outputCol="animal_index",
    handleInvalid="keep",   # 훈련 데이터에 없던 범주 → 별도 인덱스로 처리
                            # "skip": 해당 행 제거 / "error": 에러 발생 (기본값)
)

# 2단계: 인덱스 → OHE 벡터
encoder = OneHotEncoder(
    inputCol="animal_index",
    outputCol="animal_ohe",
)

# 3단계: 수치 피처 + OHE 벡터 통합
assembler = VectorAssembler(
    inputCols=["animal_ohe", "accommodates", "bedrooms"],
    outputCol="features",
)

lr = LinearRegression(featuresCol="features", labelCol="price")
pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])
```

`StringIndexer + OHE` 방식의 단점은 범주형 피처가 많을 때 각 컬럼마다 단계를 명시적으로 추가해야 한다는 것.
이를 간단하게 처리하는 대안이 **RFormula**.

```
RFormula 동작 방식
--------------------------------------------------------------
RFormula는 R 언어의 공식 문법으로 피처/레이블을 지정하면
StringIndexer + OHE + VectorAssembler를 내부에서 자동으로 처리

formula = "price ~ ."
  → price: 레이블 컬럼
  → . : 나머지 모든 컬럼을 피처로 사용
       - 수치형 컬럼: 그대로 사용
       - 문자열 컬럼: StringIndexer + OHE 자동 적용

StringIndexer + OHE 방식:
  indexer_animal = StringIndexer(inputCol="animal", ...)
  encoder_animal = OneHotEncoder(inputCol="animal_index", ...)
  indexer_room   = StringIndexer(inputCol="room_type", ...)
  encoder_room   = OneHotEncoder(inputCol="room_type_index", ...)
  assembler      = VectorAssembler(...)
  → 컬럼이 10개면 단계가 20개 이상

RFormula:
  rformula = RFormula(formula="price ~ .")
  → 단계 1개로 동일한 결과
--------------------------------------------------------------
```

```python
from pyspark.ml.feature import RFormula

rformula = RFormula(
    formula="price ~ .",       # price가 레이블, 나머지 전부 피처
    featuresCol="features",
    labelCol="label",
    handleInvalid="skip",
)

pipeline = Pipeline(stages=[rformula, lr])
pipeline_model = pipeline.fit(train_df)
```

| 방식 | 범주형 처리 | 코드량 | 유연성 |
|------|-----------|-------|-------|
| StringIndexer + OHE | 컬럼마다 명시적 지정 | 많음 | 높음 (세부 제어 가능) |
| RFormula | 자동 처리 | 적음 | 낮음 (커스터마이징 제한) |

**RFormula의 단점:**
- 어떤 컬럼이 범주형으로 처리됐는지 내부 동작이 불투명해 디버깅이 어려움
- 특정 컬럼에만 `handleInvalid` 전략을 다르게 적용하는 것이 불가능
- 범주형 여부를 컬럼의 타입(문자열 여부)으로만 판단하므로, 숫자로 저장된 범주형 컬럼은 자동 처리 불가

#### 트리 기반 모델에서 OHE가 필요 없는 이유

선형 모델은 피처와 레이블 사이의 선형 관계를 가정하므로 범주 간 순서가 없어야 하지만,
트리 기반 모델은 **분기 조건**으로 데이터를 나누기 때문에 인덱스 값 자체의 대소 관계가 문제가 되지 않음.

```
트리 기반 모델의 범주형 변수 처리
--------------------------------------------------------------
StringIndexer 적용 결과:
  dog=0, cat=1, fish=2

[선형 모델의 문제]
  price = β × animal_index + ...
  → animal_index가 1 증가할 때마다 price가 β만큼 증가한다고 가정
  → fish(2)가 cat(1)보다 더 비싸다는 잘못된 순서 관계 학습

[트리 기반 모델의 분기 방식]
  animal_index <= 0.5?
  ├─ Yes (dog=0) → 평균 price: $120
  └─ No  (cat=1, fish=2)
         animal_index <= 1.5?
         ├─ Yes (cat=1) → 평균 price: $95
         └─ No  (fish=2) → 평균 price: $80

  → 트리는 "인덱스 값이 얼마인가"가 아니라
    "이 분기점에서 나누는 것이 얼마나 유용한가"만 판단
  → dog=0, cat=1, fish=2의 순서는 트리 구조에 영향 없음
--------------------------------------------------------------
```

**트리 기반 모델에 OHE를 적용하면 오히려 성능이 나빠지는 이유:**

```
[문제 1] 범주 그룹화 능력 저하 — 가장 핵심적인 문제
--------------------------------------------------------------
트리는 분기 조건 하나로 "여러 범주를 동시에 묶는" 것이 가능해야 성능이 좋음.

예: animal별 평균 price
  dog=120달러, cat=115달러, fish=60달러
  → dog와 cat은 비슷하고, fish만 확연히 낮음
  → 이상적인 분기: "fish vs (dog, cat)"

StringIndexer만 사용 (dog=0, cat=1, fish=2):
  animal_index <= 1.5?
  ├─ Yes → dog(0), cat(1) 묶임 → 평균 $117   ← 한 번의 분기로 그룹화 가능
  └─ No  → fish(2)만       → 평균 $60

OHE 사용 (is_dog, is_cat, is_fish 3개 컬럼):
  is_fish = 1?
  ├─ Yes → fish       → 평균 $60
  └─ No  → is_dog = 1?
           ├─ Yes → dog → 평균 $120    ← dog와 cat을 묶으려면 2번 분기 필요
           └─ No  → cat → 평균 $115

  → OHE 후에는 각 범주가 독립된 컬럼이라 한 번에 묶을 수 없음
  → 범주가 많을수록 그룹화에 필요한 분기 횟수가 폭발적으로 증가
--------------------------------------------------------------

[문제 2] 피처 중요도 희석 (랜덤 포레스트에서 특히 심각)
--------------------------------------------------------------
랜덤 포레스트는 각 분기마다 전체 피처 중 일부만 무작위로 샘플링.
일반적으로 sqrt(전체 피처 수)개를 샘플링.

예: 전체 피처 100개, animal 1개 컬럼 vs OHE 후 50개 컬럼인 경우
  sqrt(100) ≈ 10개 샘플링

  StringIndexer만 사용:
    animal 1개 컬럼 → 샘플링될 확률 10/100 = 10%

  OHE 사용 (50개 컬럼으로 분산):
    is_dog 샘플링 확률  10/149 ≈ 6.7%   (각 컬럼별로 낮아짐)
    is_cat 샘플링 확률  10/149 ≈ 6.7%
    is_fish 샘플링 확률 10/149 ≈ 6.7%
    ...
    → "animal 정보가 담긴 컬럼 하나라도 뽑힐 확률"은 높아지지만
    → 각 컬럼이 담고 있는 정보가 쪼개져 있어 분기 기여도가 낮게 측정됨
    → featureImportances에서 animal 관련 중요도가 분산되어 낮게 나타남
--------------------------------------------------------------

[문제 3] 트리 깊이 증가 → 과적합 위험 상승
--------------------------------------------------------------
범주 수 N → OHE 후 N-1개 컬럼 생성

예: 우편번호(zip_code) 범주 1,000개
  OHE 적용 시: 999개의 이진 컬럼 생성
  → 각 우편번호를 분리하려면 999번의 분기 필요
  → 트리 깊이가 급격히 증가
  → 학습 데이터의 노이즈까지 학습하는 과적합 발생

StringIndexer만 사용:
  zip_code_index 1개 컬럼
  → 한 번의 분기로 여러 우편번호를 그룹화 가능
  → 트리가 실제 패턴만 학습, 깊이 제어 용이
--------------------------------------------------------------

결론
--------------------------------------------------------------
모델 유형        범주형 처리 권장 방식
-----------    -----------------------------------------------
선형 회귀        StringIndexer + OHE  (순서 관계 제거 필요)
로지스틱 회귀     StringIndexer + OHE  (순서 관계 제거 필요)
RandomForest   StringIndexer만      (OHE 불필요, 오히려 해로움)
GBT            StringIndexer만      (OHE 불필요, 오히려 해로움)
--------------------------------------------------------------
```

> **트러블 로그** — `StringIndexer`의 `handleInvalid` 기본값은 `"error"`임.
> 예: 훈련 데이터에 dog, cat만 있었는데 테스트 데이터에 bird가 등장하면
> 파이프라인이 즉시 중단됨.
> 운영 파이프라인에서 새로운 범주가 유입될 가능성이 있다면 반드시 `"keep"` 또는 `"skip"`으로 설정할 것.

### 2-7. 모델 평가

학습된 모델이 테스트 데이터에서 얼마나 잘 예측하는지 수치로 측정.
평가 지표 선택이 잘못되면 실제 비즈니스 성능과 다른 모델을 선택하게 됨.

- **목적**
  - 모델의 예측 성능을 객관적인 지표로 수치화
  - 서로 다른 모델 또는 하이퍼파라미터 조합을 비교하는 기준 제공
- **이유**
  - 학습 데이터 성능만 보면 과적합 모델을 선택할 위험이 있음
  - 테스트 데이터 평가로 실제 운영 성능을 사전에 추정
- **결과**
  - 회귀: RMSE, MAE, R² 등으로 예측 오차 측정
  - 분류: AUC-ROC, 정확도, F1 스코어 등으로 분류 성능 측정
- **장점**
  - `Evaluator` 객체 하나로 다양한 지표를 손쉽게 계산
  - 교차 검증 시 `CrossValidator`와 자동으로 연동
- **단점**
  - 단일 지표 최적화가 비즈니스 목표와 일치하지 않을 수 있음
  - 불균형 데이터에서 정확도는 오해를 유발 (AUC 사용 권장)

```python
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(
    labelCol="price",
    predictionCol="prediction",
    metricName="rmse",  # rmse, mae, r2 선택 가능
)

rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse:.2f}")

# R² 추가 확인
r2_evaluator = RegressionEvaluator(
    labelCol="price",
    predictionCol="prediction",
    metricName="r2",
)
print(f"R²: {r2_evaluator.evaluate(predictions):.4f}")
```

| 지표 | 계산 방식 | 해석 |
|------|----------|------|
| RMSE | √(Σ(예측-실제)²/n) | 낮을수록 좋음, 단위가 타겟과 동일 |
| MAE | Σ\|예측-실제\|/n | 이상치 영향 적음, 낮을수록 좋음 |
| R² | 1 - RSS/TSS | 1에 가까울수록 좋음, 분산 설명력 |

#### R²란 무엇인가

R²는 모델이 타겟 변수의 전체 변동성 중 얼마나 많은 부분을 설명하는지를 0~1 사이 비율로 나타내는 지표.
RMSE가 "오차의 절대 크기"를 말한다면, R²는 "베이스라인 대비 얼마나 잘 예측하는가"를 말함.

```
R² 계산 구조
--------------------------------------------------------------
TSS (Total Sum of Squares): 베이스라인의 총 오차
  TSS = Σ(실제값 - 전체 평균)²
  → "피처를 전혀 보지 않고 평균만 예측했을 때 발생하는 총 오차"
  → 모델이 아무것도 학습하지 않았을 때의 출발점

RSS (Residual Sum of Squares): 모델이 설명하지 못한 나머지 오차
  RSS = Σ(실제값 - 예측값)²
  → "모델이 예측하고도 남은 오차"
  → RSS가 작을수록 모델이 데이터를 잘 설명

R² = 1 - RSS / TSS
  → "모델이 베이스라인 오차를 얼마나 줄였는가"의 비율

  RSS = TSS 이면: R² = 0  → 모델이 평균 예측과 동일한 수준
  RSS = 0   이면: R² = 1  → 모델이 모든 값을 완벽하게 예측
  RSS > TSS 이면: R² < 0  → 모델이 평균 예측보다도 못함 (문제 있음)

직관적 해석:
  R² = 0.87 → "데이터 변동성의 87%를 모델이 설명함"
             → 평균만 예측할 때 발생하던 오차의 87%를 모델이 제거
--------------------------------------------------------------

예: 에어비앤비 가격 예측
  실제 price: [100, 200, 150, 300, 250]
  전체 평균:  200

  TSS = (100-200)² + (200-200)² + (150-200)² + (300-200)² + (250-200)²
      = 10000 + 0 + 2500 + 10000 + 2500 = 25000

  모델 예측: [120, 190, 160, 280, 240]
  RSS = (100-120)² + (200-190)² + (150-160)² + (300-280)² + (250-240)²
      = 400 + 100 + 100 + 400 + 100 = 1100

  R² = 1 - 1100/25000 = 0.956
  → 모델이 price 변동성의 95.6%를 설명
--------------------------------------------------------------
```

#### RMSE와 R²를 함께 봐야 하는 이유

RMSE는 **단위에 종속**되고, R²는 **절대적 오차 크기를 말하지 않음**.
한 지표만 보면 모델 성능을 오해할 수 있어 반드시 두 지표를 함께 확인해야 함.

```
RMSE의 한계: 단위 종속성
--------------------------------------------------------------
같은 모델, 같은 데이터인데 단위만 달라진 경우:

  가격을 달러로 측정:  RMSE = $40    → "오차가 작다"
  가격을 센트로 측정:  RMSE = 4000   → "오차가 크다"?

  → 동일한 모델인데 단위에 따라 숫자가 100배 차이
  → RMSE 숫자만으로는 "좋다/나쁘다" 판단 불가
  → 반드시 베이스라인 RMSE 또는 R²와 함께 해석해야 함
--------------------------------------------------------------

R²의 한계: 절대 오차 크기를 말하지 않음
--------------------------------------------------------------
  R² = 0.95 이지만 RMSE = $3,000인 경우:
  → "데이터 변동성의 95%를 설명"하는 것은 맞음
  → 하지만 실제 예측 오차가 $3,000이면 비즈니스적으로 쓸 수 없을 수 있음
  → R²가 높아도 오차의 절대 크기가 허용 범위를 벗어나면 운영 불가
--------------------------------------------------------------

두 지표를 함께 보는 케이스별 해석
--------------------------------------------------------------
케이스                       의미
--------------------------   ----------------------------------------
RMSE↓  R²↑  (둘 다 개선)     진짜 성능 향상. 모델이 더 잘 학습됨
RMSE↓  R²→  (RMSE만 감소)    단위 변환이나 스케일 조정 효과일 수 있음.
                              실제 설명력은 그대로이므로 주의
RMSE↑  R²↑  (R²만 증가)      타겟 분산이 큰 다른 데이터셋 적용 시 발생.
                              설명력은 높아졌지만 절대 오차도 커짐
RMSE↑  R²↓  (둘 다 악화)     모델이 나빠진 것. 하이퍼파라미터/피처 재검토
RMSE→  R²<0 (R²가 음수)      모델이 평균 예측보다도 못함.
                              전처리 오류, 타겟/피처 불일치 등 근본 문제

핵심 원칙:
  RMSE 감소 = 예측 오차의 절대값이 줄었다
  R²  증가 = 베이스라인 대비 설명력이 높아졌다
  → 둘 다 동시에 개선될 때만 "모델이 진짜 좋아졌다"고 판단
--------------------------------------------------------------
```

#### RMSE 값 해석: 베이스라인 모델과 비교

RMSE 숫자 자체만으로는 모델이 좋은지 나쁜지 판단할 수 없음.
"RMSE=100이 좋은가?"는 타겟 변수의 분포와 비교 기준 없이는 대답할 수 없음.
가장 단순한 기준(베이스라인 모델)과 비교해야 의미 있는 해석이 가능.

```
베이스라인 모델이란?
--------------------------------------------------------------
"아무 피처도 보지 않고 무조건 평균값을 예측"하는 가장 단순한 모델.

예: 에어비앤비 숙소 가격 예측
  전체 평균 price = $150

  베이스라인 예측: 모든 숙소에 대해 $150으로 예측
  → 이때의 RMSE = 베이스라인 RMSE (= 타겟 표준편차와 동일)

  우리 모델의 RMSE = $40이라면?
  → 베이스라인($150 오차) 대비 훨씬 정확 → 모델이 유의미하게 동작

  우리 모델의 RMSE = $145이라면?
  → 평균만 예측하는 것과 거의 차이 없음 → 모델이 사실상 학습 안 된 것
--------------------------------------------------------------

RMSE 해석 기준
--------------------------------------------------------------
                    베이스라인 RMSE
                         │
       ┌─────────────────┴─────────────────┐
       ▼                                   ▼
  우리 모델 RMSE << 베이스라인        우리 모델 RMSE ≈ 베이스라인
  → 모델이 피처에서 유의미한              → 피처가 예측에 기여하지 못함
    패턴을 학습함                         → 피처 선택, 전처리 재검토 필요
--------------------------------------------------------------
```

```python
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import avg, lit

# 베이스라인 RMSE 계산: 모든 예측값 = 학습 데이터 평균
avg_price = train_df.select(avg("price")).first()[0]

baseline_preds = test_df.withColumn("prediction", lit(avg_price))

baseline_evaluator = RegressionEvaluator(
    labelCol="price",
    predictionCol="prediction",
    metricName="rmse",
)
baseline_rmse = baseline_evaluator.evaluate(baseline_preds)

# 우리 모델 RMSE
model_rmse = evaluator.evaluate(predictions)

print(f"베이스라인 RMSE: {baseline_rmse:.2f}")
print(f"모델 RMSE:      {model_rmse:.2f}")
print(f"개선율:         {(1 - model_rmse / baseline_rmse) * 100:.1f}%")
```

```
결과 예시 및 해석
--------------------------------------------------------------
베이스라인 RMSE: 200.54  (평균만 예측했을 때 오차)
모델 RMSE:       68.32   (LinearRegression 예측 오차)
개선율:          65.9%

레이블의 단위는 RMSE에 직접적인 영향을 준다.
e.g. 레이블이 높이인 경우, 측정 단위로 m 대신 cm 사용하면 RMSE가 더 높아진다.
→ 다른 단위를 사용해서 RMSE를 임의로 줄일 수 있기 때문에 RMSE를 기준과 비교하는 것이 중요하다.

→ 모델이 피처(방 크기, 위치 등)를 활용해
  베이스라인 대비 65.9% 더 정확하게 예측
→ 유의미한 학습이 이루어진 것으로 판단 가능

만약 개선율이 10% 미만이라면:
  → 피처 품질 재검토 (의미 없는 피처가 너무 많은지)
  → 결측값 처리 방식 재검토
  → 더 복잡한 모델(RandomForest, GBT) 시도
--------------------------------------------------------------
```

### 2-8. 모델 저장 및 로드

학습된 파이프라인 모델을 파일 시스템에 저장하고, 추론 시 다시 로드해 사용.
전처리 통계(스케일러 평균/표준편차 등)가 모델과 함께 저장되므로 추론 시 일관성 보장됨.

- **목적**
  - 매번 학습 없이 저장된 모델을 로드해 추론 가능
  - 학습 파이프라인과 추론 파이프라인을 완전히 분리
- **이유**
  - 대규모 모델 학습은 수십 분~수 시간 소요 → 매번 재학습 불가
  - 모델 버전 관리를 위해 학습 결과를 영속적으로 저장해야 함
- **결과**
  - 저장 경로에 Parquet 형식의 모델 파일과 메타데이터가 생성됨
  - `PipelineModel.load()`로 로드 시 원래 파이프라인과 동일하게 동작
- **장점**
  - 학습/추론 파이프라인 분리로 리소스 효율화
  - S3, HDFS, 로컬 파일시스템 모두 지원
- **단점**
  - MLlib 버전이 달라지면 저장된 모델을 로드하지 못할 수 있음
  - 모델 파일 크기가 커질 수 있어 스토리지 관리 필요

```python
from pyspark.ml import PipelineModel

# 모델 저장
model_path = "/models/airbnb_lr_v1"
pipeline_model.save(model_path)

# 모델 로드
loaded_model = PipelineModel.load(model_path)

# 로드된 모델로 추론 (전처리 포함)
new_predictions = loaded_model.transform(test_df)
new_predictions.select("price", "prediction").show(5)
```

> **트러블 로그** — 모델 저장 경로에 버전 정보를 포함시켜야 롤백이 가능.
> 예: `/models/airbnb_lr_v1`만 사용하다가 v2 배포 후 문제가 생기면 v1으로 즉시 되돌릴 수 없음.
> S3라면 `s3://ml-models/airbnb/lr/2024-01-15/` 형태로 날짜 기반 경로를 사용하고,
> 현재 운영 버전을 가리키는 심볼릭 포인터(예: `current → 2024-01-15`)를 별도로 관리할 것.

---

## 3. 하이퍼파라미터 튜닝

하이퍼파라미터는 모델 학습 전에 사람이 설정하는 값으로, 모델 구조와 학습 방식을 결정함.
최적의 조합을 자동으로 탐색하기 위해 교차 검증과 격자 탐색을 함께 사용.

### 3-1. 트리 기반 모델

트리 기반 모델은 데이터를 특정 조건으로 반복 분기하여 예측하는 알고리즘.
선형 회귀와 달리 비선형 관계를 자동으로 학습하며, 앙상블 방법으로 성능을 높임.

- **목적**
  - 복잡한 비선형 패턴을 선형 모델보다 더 정확하게 학습
  - 랜덤 포레스트, GBT 등 앙상블로 분산을 줄이고 일반화 성능 향상
- **이유**
  - 실제 데이터는 피처 간 상호작용이 많아 선형 가정이 성립하지 않는 경우가 많음
  - 결측값, 스케일 차이 등에 덜 민감해 전처리 부담 감소
- **결과**
  - 의사결정 트리: 단일 트리, 해석 쉬움 but 과적합 위험
  - 랜덤 포레스트: 다수의 트리 평균, 분산 감소, 안정적
  - GBT (Gradient Boosted Trees): 이전 트리의 오차를 순차적으로 보완, 정확도 최고
- **장점**
  - 피처 스케일링 불필요 (트리 분기는 수치 크기에 독립적)
  - `featureImportances`로 피처 중요도 확인 가능
  - 결측값에 상대적으로 강인
- **단점**
  - GBT는 학습이 순차적이라 병렬화에 제한
  - 트리 깊이, 나무 수 등 하이퍼파라미터가 많아 튜닝 비용이 큼

```python
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor

# 랜덤 포레스트
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="price",
    numTrees=100,      # 트리 개수 (많을수록 안정적, but 학습 시간 증가)
    maxDepth=5,        # 트리 최대 깊이 (깊을수록 과적합 위험)
    seed=42,
)

# GBT (더 높은 정확도, but 학습 시간 증가)
gbt = GBTRegressor(
    featuresCol="features",
    labelCol="price",
    maxIter=50,       # 부스팅 반복 횟수
    maxDepth=5,
    stepSize=0.1,     # 학습률
)
```

| 모델 | 학습 방식 | 강점 | 약점 |
|------|----------|------|------|
| DecisionTree | 단일 트리 | 해석 쉬움 | 과적합 취약 |
| RandomForest | 병렬 앙상블 (Bagging) | 안정적, 빠름 | GBT 대비 정확도 낮음 |
| GBT | 순차 앙상블 (Boosting) | 정확도 최고 | 학습 느림, 과적합 주의 |

#### maxBins: 왜 사이킷런에는 없고 스파크에만 있는가

`maxBins`는 연속형 피처를 몇 개의 구간(bin)으로 이산화해서 분기 후보를 탐색할지 결정하는 매개변수.
사이킷런의 트리는 단일 머신에서 전체 데이터를 메모리에 올려 정확한 분기점을 직접 계산하므로 이산화가 불필요.
스파크는 데이터가 여러 이그제큐터에 분산되어 있어 **PLANET 알고리즘**으로 분기점을 찾는데, 이 과정에서 이산화가 핵심 역할을 함.

```
왜 분산 트리에는 이산화가 필요한가
--------------------------------------------------------------
[사이킷런 — 단일 머신]
  전체 데이터가 메모리에 한 번에 존재
  price: [80, 95, 120, 150, 200, 300]

  모든 값을 정렬 → 각 값 사이를 분기점 후보로 직접 탐색
    80|95 사이에서 자르면 RSS 얼마? → 계산
    95|120 사이에서 자르면 RSS 얼마? → 계산
    ...
  → 전체 데이터가 한 곳에 있으므로 가장 좋은 분기점을 정확하게 선택 가능
  → maxBins 불필요

[스파크 — PLANET 알고리즘]
  데이터가 이그제큐터 여러 곳에 분산
    이그제큐터A: [80, 120]
    이그제큐터B: [95, 300]
    이그제큐터C: [150, 200]

  문제: 각 이그제큐터가 자기 데이터만 보면 전체 기준 최적 분기점을 알 수 없음
    이그제큐터A: "80~120 사이가 최적"
    이그제큐터B: "95~300 사이가 최적"
    → 서로 다른 결론, 합의 불가

  PLANET의 해결책: 분기 탐색 전에 전체 값 범위를 maxBins개 구간으로 이산화

    [1단계] 이산화 (학습 시작 전 1회 수행)
      전체 price 범위를 maxBins개 구간으로 나눔
      예: maxBins=4 → 구간1:[0~100] 구간2:[100~200] 구간3:[200~300] 구간4:[300~400]

    [2단계] 각 이그제큐터가 자기 데이터를 구간 번호로 변환
      이그제큐터A: [80→구간1,  120→구간2]
      이그제큐터B: [95→구간1,  300→구간4]
      이그제큐터C: [150→구간2, 200→구간3]

    [3단계] 드라이버가 구간별 집계 통계를 모아 최적 분기 결정
      "구간1과 구간2 사이에서 자를 때 RSS가 가장 작다" → 해당 구간 경계를 분기점으로 선택

  핵심: 구간 번호(정수)만 주고받으면 되므로 이그제큐터 간 통신 비용이 대폭 감소
  maxBins가 클수록 구간이 세밀해져 정확하지만, 이그제큐터 간 통신/메모리 비용 증가

--------------------------------------------------------------
PLANET: Parallel Learner for Assembling Numerous Ensemble Trees
  → 분산 환경에서 앙상블 트리를 효율적으로 학습하기 위해 설계된 알고리즘
  → MLlib의 DecisionTree, RandomForest, GBT가 모두 이 방식을 기반으로 구현됨
--------------------------------------------------------------
```

**maxBins와 범주형 컬럼의 관계:**

`StringIndexer`로 변환된 범주형 컬럼은 정수 인덱스 값을 가짐.
MLlib는 각 고유 인덱스 값을 하나의 분기 후보로 처리하는데,
`maxBins`보다 카테고리 수가 많으면 모든 후보를 수용할 구간이 부족해짐.

```
maxBins 부족 시 발생하는 문제
--------------------------------------------------------------
예: room_type 컬럼 카테고리 50개
    StringIndexer 적용 → room_type_index: 0~49 (50개 고유값)

    maxBins = 32 (기본값)인 경우:
      분기 후보 50개 필요 → 구간 32개만 생성 가능
      → 일부 카테고리가 같은 구간에 묶여 구분 불가

      또는 MLlib가 즉시 에러 발생:
      "IllegalArgumentException: DecisionTree requires maxBins >= max categories in features.
       numClasses = X, but some features have unseen labels."

해결책: maxBins를 가장 카테고리 수가 많은 컬럼의 범주 수 이상으로 설정

maxBins 값에 따른 트레이드오프
--------------------------------------------------------------
maxBins 값    분기 정확도    메모리/통신 비용    수용 가능 카테고리 수
----------   -----------   ----------------   -------------------
32 (기본)     낮음           낮음               32개 이하
128           중간           중간               128개 이하
512           높음           높음               512개 이하
--------------------------------------------------------------
```

```python
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.functions import countDistinct

# 학습 데이터에서 범주형 컬럼의 최대 카테고리 수 확인
max_categories = (
    train_df
    .select(countDistinct("room_type_index").alias("cnt"))
    .first()["cnt"]
)

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="price",
    numTrees=100,
    maxDepth=5,
    maxBins=max(32, max_categories),  # 가장 많은 카테고리 수 이상으로 보장
    seed=42,
)
```

> **트러블 로그** — `maxBins`를 기본값(32)으로 두고 카테고리가 많은 컬럼을 학습하면
> `IllegalArgumentException: DecisionTree requires maxBins >= max categories` 에러가 발생하며 학습이 즉시 중단됨.
> 예: zip_code 컬럼에 300개의 고유 우편번호가 있는 경우 `maxBins=32`로는 학습 불가.
> `StringIndexer` 적용 후 `train_df.agg(max("zip_code_index")).first()[0]`으로 최대 인덱스를 확인하고
> `maxBins`를 그 값 이상으로 설정할 것. 단, 512 이상으로 올리면 셔플 데이터 크기가 커지므로
> 고카디널리티 컬럼은 상위 N개 범주만 유지하는 방식으로 카테고리 수를 먼저 줄이는 것을 검토할 것.

#### 앙상블: 랜덤 포레스트로 단일 트리의 한계를 극복하는 방법

단일 의사결정 트리는 **고분산(high variance)** 문제가 있음.
학습 데이터가 조금만 달라져도 트리 구조 자체가 완전히 바뀌어, 새로운 데이터에 대한 예측이 불안정해짐.

```
단일 트리의 고분산 문제
--------------------------------------------------------------
학습 데이터 A로 만든 트리:       학습 데이터 B로 만든 트리 (row2만 빠짐):
  price > 200?                     bedrooms > 2?
  ├─ Yes → $300                    ├─ Yes → $250
  └─ No  → $100                    └─ No  → $120

→ 데이터 하나 차이로 트리 구조가 완전히 달라짐
→ 어떤 데이터로 학습하느냐에 따라 예측값이 크게 흔들림
--------------------------------------------------------------
```

랜덤 포레스트는 "다양한 트리를 여러 개 만들어 평균 낸다"는 방식으로 이를 해결.
트리가 100개 있으면 각 트리가 틀리는 방향이 서로 달라, 평균 내면 오류가 상쇄됨.
단, 이 효과가 나오려면 **트리들이 서로 달라야 함**. 똑같은 트리를 100개 평균 내면 의미 없음.

다양성을 만드는 방법이 두 가지:

```
랜덤 포레스트의 두 가지 무작위성
==========================================================================

[1] 행별 부트스트랩 샘플링 (Bootstrap Sampling)
--------------------------------------------------------------
복원 추출(중복 허용)로 각 트리마다 서로 다른 데이터 부분집합을 사용

  전체 데이터: [row1, row2, row3, row4, row5]

  트리1용 샘플: [row1, row1, row3, row4, row5]  ← row1 중복, row2 빠짐
  트리2용 샘플: [row2, row3, row3, row4, row5]  ← row3 중복, row1 빠짐
  트리3용 샘플: [row1, row2, row4, row4, row5]  ← row4 중복, row3 빠짐

  복원 추출이므로 n개에서 n번 뽑으면 약 63%의 행만 실제로 사용됨
  나머지 37%는 OOB(Out-of-Bag) 데이터 → 검증 세트 없이도 성능 추정에 활용 가능

[2] 열별 무작위 피처 선택 (Random Feature Subsampling)
--------------------------------------------------------------
각 분기점을 탐색할 때 전체 피처 중 일부만 무작위로 골라 사용

  이게 왜 필요한가?
  → 모든 피처를 다 사용하면, 강력한 피처(예: 위치)가 모든 트리의
    루트 분기를 독점 → 트리 100개가 전부 비슷한 구조 → 다양성 사라짐

  전체 피처: [위치, 방크기, 층수, 방개수, 화장실수, ...] (10개)
  각 분기마다 sqrt(10) ≈ 3개만 무작위 선택

    트리1 루트 분기 후보: [방크기, 층수, 화장실수] → 3개 중 최적 선택
    트리2 루트 분기 후보: [위치,   방개수, 방크기]  → 3개 중 최적 선택
    트리3 루트 분기 후보: [층수,   화장실수, 위치]  → 3개 중 최적 선택

  → 위치가 빠진 트리1은 방크기 기준으로 데이터를 나눔
  → 각 트리가 서로 다른 피처 관점에서 데이터를 학습 → 구조가 다양해짐

==========================================================================

두 무작위성이 결합된 최종 예측
--------------------------------------------------------------
  트리1: row1,1,3,4,5 데이터 + 분기마다 다른 피처 → 예측 $210
  트리2: row2,3,3,4,5 데이터 + 분기마다 다른 피처 → 예측 $185
  트리3: row1,2,4,4,5 데이터 + 분기마다 다른 피처 → 예측 $230
  ...
  트리100:                                          → 예측 $195

  최종 예측 (회귀) = 평균 = (210 + 185 + 230 + ... + 195) / 100 = $200

  → 개별 트리가 틀려도, 틀리는 방향이 서로 달라 평균 내면 오류 상쇄
  → 단일 트리 대비 분산이 낮고 안정적인 예측
--------------------------------------------------------------
```

```python
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="price",
    numTrees=100,                    # 트리 개수. 많을수록 안정적이나 학습 시간 증가
    maxDepth=5,                      # 트리 최대 깊이
    featureSubsetStrategy="sqrt",    # 분기마다 샘플링할 피처 수
                                     # "sqrt"  : sqrt(전체 피처 수) — 기본 추천
                                     # "log2"  : log2(전체 피처 수) — 피처가 매우 많을 때
                                     # "auto"  : 회귀는 1/3, 분류는 sqrt 자동 선택
                                     # "all"   : 전체 사용 (다양성 없음, 비추천)
    subsamplingRate=1.0,             # 행 샘플링 비율 (1.0 = 복원 추출, 기본값)
    seed=42,
)
```

> **트러블 로그** — `numTrees`를 늘릴수록 성능이 올라가지만 수확 체감이 빠르게 발생함.
> 예: `numTrees=10→50` 구간에서는 RMSE가 크게 떨어지지만, `50→200` 구간에서는 거의 변화 없으면서
> 학습 시간만 4배 증가하는 상황이 자주 발생함.
> `numTrees=50`으로 빠르게 방향을 잡은 뒤 `maxDepth`와 `featureSubsetStrategy`를 먼저 조정하고,
> 마지막에 `numTrees`를 늘려 안정성을 높이는 순서로 튜닝할 것.

### 3-2. K-폴드 교차 검증

학습 데이터를 K개 폴드로 나눠 K번 반복 학습/검증하여 하이퍼파라미터 성능을 안정적으로 측정.
단일 train/test 분할보다 분산이 낮고 신뢰할 수 있는 성능 추정치를 제공.

- **목적**
  - 특정 데이터 분할에 의존하지 않고 하이퍼파라미터 성능을 객관적으로 평가
  - 하이퍼파라미터 탐색과 모델 검증을 동시에 수행
- **이유**
  - 단일 분할은 운이 좋거나 나쁜 분할에 따라 성능 추정이 불안정
  - K번 검증 평균으로 더 안정적인 성능 지표 획득
- **결과**
  - K번 반복에서 각 폴드를 검증 세트로 사용한 성능의 평균을 반환
  - 가장 성능 좋은 하이퍼파라미터 조합으로 전체 학습 데이터 재학습
- **장점**
  - 테스트 데이터를 건드리지 않고 하이퍼파라미터 선택 가능
  - 데이터가 적을 때도 안정적인 성능 추정
- **단점**
  - K배의 학습 시간 소요 (K=5이면 5번 학습)
  - 대규모 데이터에서는 비용이 매우 큼

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

# 탐색할 하이퍼파라미터 격자 정의
param_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [50, 100, 200])    # 3가지 값
    .addGrid(rf.maxDepth, [3, 5, 7])         # 3가지 값
    .build()                                  # → 총 9개 조합
)

evaluator = RegressionEvaluator(
    labelCol="price",
    predictionCol="prediction",
    metricName="rmse",
)

# 5-폴드 교차 검증
cv = CrossValidator(
    estimator=pipeline,      # 파이프라인 전체를 추정기로 지정
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,              # 폴드 수 (실무에서 3~5가 일반적)
    seed=42,
)

cv_model = cv.fit(train_df)
print(f"최적 RMSE: {min(cv_model.avgMetrics):.2f}")
```

> **트러블 로그** — 하이퍼파라미터 조합이 많으면 교차 검증 시간이 폭발적으로 증가.
> 예: `numTrees` 5개 × `maxDepth` 4개 × `stepSize` 3개 = 60개 조합을 5-fold로 돌리면
> 실제로 300번 학습 발생 → 단일 학습이 10분이면 총 50시간 소요.
> 격자 범위를 좁게 잡고 먼저 방향을 파악한 뒤 세밀화하는 방식으로 진행할 것.

### 3-3. 파이프라인 최적화

교차 검증과 파이프라인을 결합하면 전처리 단계까지 포함한 최적화가 가능.
베스트 모델을 추출한 후 테스트 데이터에서 최종 성능을 측정.

- **목적**
  - 전처리 파라미터(예: 스케일링 여부)와 모델 파라미터를 함께 최적화
  - 최적 파이프라인을 저장하여 바로 운영에 투입
- **이유**
  - 전처리 방식 자체도 성능에 영향을 미치므로 모델 파라미터만 튜닝하면 불완전
  - 파이프라인 전체를 최적화하면 학습/추론 일관성도 자동 보장
- **결과**
  - `cv_model.bestModel`로 가장 성능 좋은 파이프라인 추출
  - 해당 모델로 테스트 데이터 최종 평가 후 저장
- **장점**
  - 파이프라인과 교차 검증의 결합으로 코드 추가 없이 완전한 AutoML 파이프라인 구성
  - 베스트 모델이 이미 전체 학습 데이터로 재학습된 상태
- **단점**
  - 파이프라인 단계가 많아질수록 튜닝 탐색 공간이 기하급수적으로 증가

```python
# 베스트 모델 추출
best_model = cv_model.bestModel

# 테스트 데이터 최종 평가
test_predictions = best_model.transform(test_df)
final_rmse = evaluator.evaluate(test_predictions)
print(f"최종 테스트 RMSE: {final_rmse:.2f}")

# 베스트 파이프라인 저장
best_model.save("/models/airbnb_rf_best_v1")

# 피처 중요도 확인 (RandomForest 모델 추출)
rf_model = best_model.stages[-1]  # 마지막 단계가 RandomForest 모델
print(rf_model.featureImportances)
```

```
파이프라인 최적화 전체 흐름
--------------------------------------------------------------
train_df
    │
    ├──► CrossValidator.fit()
    │       ├── fold 1: 학습 + 검증 (조합 1~9)
    │       ├── fold 2: 학습 + 검증 (조합 1~9)
    │       └── fold 3: 학습 + 검증 (조합 1~9)
    │            ↓
    │       평균 RMSE 계산 → 최적 조합 선택
    │            ↓
    │       전체 train_df로 재학습 → bestModel
    │
    └──► bestModel.transform(test_df) → 최종 성능 측정
--------------------------------------------------------------
```

#### parallelism: 순차 학습 문제 해결

`spark.ml`의 `CrossValidator`는 기본적으로 모델을 **순차적으로** 학습함.
클러스터가 분산 처리를 지원해도, 하이퍼파라미터 조합들은 하나씩 순서대로 실행됨.
`parallelism` 매개변수는 이 문제를 해결하기 위해 도입됨.

```
기본 동작 (parallelism=1) — 순차 실행
--------------------------------------------------------------
9개 조합 × 3-fold = 27번 학습

  [조합1-fold1] → [조합1-fold2] → [조합1-fold3]
  → [조합2-fold1] → [조합2-fold2] → ...
  → [조합9-fold3]

  한 번에 하나씩, 순서대로 실행
  단일 학습에 10분 소요 시 → 27 × 10분 = 총 270분
  이 동안 클러스터 일부 자원이 유휴 상태로 낭비됨

parallelism=4 설정 시 — 병렬 실행
--------------------------------------------------------------
  라운드1: [조합1-fold1] [조합2-fold1] [조합3-fold1] [조합4-fold1]  ← 동시 실행
  라운드2: [조합5-fold1] [조합6-fold1] [조합7-fold1] [조합8-fold1]  ← 동시 실행
  ...
  라운드7: [조합27-fold3]

  ceil(27/4) = 7라운드 완료 → 이론상 270분 → 약 70분으로 단축

주의:
  각 모델이 클러스터 전체 자원을 100% 사용 중이라면
  4개 동시 실행 시 각각 약 25% 자원만 받음
  → 클러스터에 유휴 자원이 있을 때 가장 효과적
  → 유휴 자원 없이 parallelism을 과도하게 높이면 오히려 전체 시간이 늘어날 수 있음
--------------------------------------------------------------
```

```python
cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    parallelism=4,   # 최대 4개 모델을 동시에 학습
                     # 클러스터 이그제큐터 수와 단일 모델 자원 소비량을 고려해 설정
    seed=42,
)
```

#### 전처리 중복 계산 제거: Pipeline 내부에 CrossValidator 배치

기본 방식인 `CrossValidator(estimator=pipeline, ...)`는 하이퍼파라미터 조합마다
파이프라인 전체를 처음부터 재실행함. 전처리 단계(StringIndexer, StandardScaler 등)는
RF 하이퍼파라미터가 바뀌어도 결과가 동일한데, 불필요하게 반복 계산됨.

```
기본 방식: CrossValidator 내부에 Pipeline
--------------------------------------------------------------
CrossValidator(estimator=pipeline, ...)
  pipeline = [StringIndexer, VectorAssembler, StandardScaler, RandomForest]

9개 조합 × 3-fold = 27번 실행, 각 실행마다:
  StringIndexer.fit()         ← numTrees/maxDepth와 무관한데 27번 반복
  VectorAssembler.transform() ← 27번 반복
  StandardScaler.fit()        ← 27번 반복
  RandomForest.fit()          ← 실제로 조합마다 달라지는 부분

전체 비용 = (전처리 비용 × 27) + (RF 학습 비용 × 27)

개선된 방식: Pipeline 내부에 CrossValidator
--------------------------------------------------------------
cv = CrossValidator(estimator=rf, ...)   ← 모델만 넣음
pipeline = Pipeline(stages=[StringIndexer, VectorAssembler, StandardScaler, cv])
pipeline_model = pipeline.fit(train_df)

실행 흐름:
  StringIndexer.fit()         ← 딱 1번만 실행
  VectorAssembler.transform() ← 딱 1번만 실행
  StandardScaler.fit()        ← 딱 1번만 실행
       ↓ 전처리된 데이터를 cv에 넘김
  cv.fit(전처리된 데이터):
    RF 조합1 학습, RF 조합2 학습, ... RF 조합27 학습
    → RF 부분만 27번 반복

전체 비용 = (전처리 비용 × 1) + (RF 학습 비용 × 27)

전처리 비용이 클수록 (컬럼 수 많거나, 데이터 크기가 크거나) 절감 효과가 커짐
--------------------------------------------------------------
```

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline

param_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [50, 100, 200])
    .addGrid(rf.maxDepth, [3, 5, 7])
    .build()
)

evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")

# CrossValidator에는 모델만 넣음 (전처리 제외)
cv = CrossValidator(
    estimator=rf,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    parallelism=4,
    seed=42,
)

# Pipeline 마지막 단계로 cv를 배치 → 전처리는 1번만 실행됨
pipeline = Pipeline(stages=[indexer, assembler, scaler, cv])
pipeline_model = pipeline.fit(train_df)

# 최종 테스트 평가
test_predictions = pipeline_model.transform(test_df)
print(f"최종 RMSE: {evaluator.evaluate(test_predictions):.2f}")
```

> **트러블 로그** — `Pipeline(stages=[..., cv])` 방식으로 바꿀 때 `bestModel` 추출 경로가 달라짐.
> 기존: `cv_model.bestModel`
> 변경 후: `pipeline_model.stages[-1].bestModel`
> `pipeline_model.stages[-1]`이 CrossValidatorModel이고, 거기서 `.bestModel`로 RF 모델에 접근해야 함.
> 피처 중요도 확인 시 `pipeline_model.stages[-1].bestModel.featureImportances`로 접근할 것.

---

## 4. 요약

```
MLlib 핵심 개념 요약
==========================================================================

컴포넌트                설명
--------------------   -----------------------------------------------
Transformer            transform()으로 DataFrame → DataFrame 변환
                       예: StringIndexer, VectorAssembler, StandardScaler

Estimator              fit()으로 학습 후 Model(Transformer) 반환
                       예: LinearRegression, RandomForestRegressor

Pipeline               Transformer + Estimator를 순서대로 연결
                       fit() 한 번으로 전체 단계 학습

PipelineModel          학습된 파이프라인, transform()으로 추론
                       save()/load()로 영속화 가능

CrossValidator         K-fold 교차 검증 + 하이퍼파라미터 탐색 자동화
                       bestModel로 최적 파이프라인 반환

==========================================================================

모델 선택 가이드
--------------------------------------------------------------
상황                          추천 모델
----------------------------  --------------------------------
빠른 베이스라인 필요            LinearRegression
비선형 관계, 안정성 우선        RandomForestRegressor
최고 정확도 필요               GBTRegressor
해석 가능성이 최우선            DecisionTreeRegressor
--------------------------------------------------------------

MLlib 파이프라인 체크리스트
--------------------------------------------------------------
□ EDA: 결측값, 이상치, 분포 확인
□ 데이터 분리: randomSplit + seed 고정
□ 전처리: StringIndexer → VectorAssembler → (StandardScaler)
□ 파이프라인: Pipeline(stages=[...])으로 묶기
□ 평가: Evaluator로 RMSE/R² 확인
□ 튜닝: ParamGridBuilder + CrossValidator
□ 저장: bestModel.save(버전 포함 경로)
--------------------------------------------------------------
```
