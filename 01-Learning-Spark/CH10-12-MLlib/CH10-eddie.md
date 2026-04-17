
# 01.MLib 개념
## (1)배경 / 등장 이유

**이전 기술의 한계**

- scikit-learn, R 같은 기존 ML 라이브러리는 단일 머신 메모리 안에서만 동작
- 데이터가 수백 GB ~ TB 규모가 되면 메모리에 올리는 것 자체가 불가능
- 그래서 Spark 위에서 데이터 분산 처리를 그대로 활용하는 ML 라이브러리가 필요해졌고, MLlib가 등장



## (2)핵심 개념

MLlib는 크게 두 가지 API가 존재한다.

1. **RDD-based API** (`spark.mllib`) — 구버전, deprecated 상태
2. **DataFrame-based API** (`spark.ml`) — 현재 표준, Pipeline 개념 지원

실무에서는 `spark.ml`만 사용함


## (3)제공 모델 및 기능

- **분류** : Logistic Regression, Random Forest, GBT(Gradient Boost Tree)
- **회귀** : Linear Regression, GBT Regressor
- **클러스터링** : K-Means, LDA
- **추천** : ALS (Alternating Least Squares)
- **전처리** : Tokenizer, StandardScaler, VectorAssembler
- **Pipeline** : 전처리 → 학습 → 예측을 하나의 워크플로우로 묶는 구조

## (4)Pipeline이 핵심인 이유

MLlib의 가장 큰 특징은 scikit-learn의 Pipeline과 동일한 개념을 분산 환경에서 지원한다는 것이다.

- `Transformer` : 데이터를 변환 (예: VectorAssembler, StandardScaler)
- `Estimator` : 데이터를 학습해서 모델 생성 (예: LogisticRegression)
- `Pipeline` : Transformer + Estimator를 순서대로 묶음


## (5)실무에서의 위치

MLlib는 아래 상황에서 사용한다.

- 학습 데이터 자체가 수억 건 이상으로 단일 머신에 올라가지 않을 때
- 이미 Spark로 전처리 중인 파이프라인에 ML을 붙여야 할 때
- Feature Engineering이 복잡하고 Spark DataFrame을 그대로 쓰고 싶을 때

반대로 데이터가 수백만 건 이하라면 pandas + scikit-learn이 훨씬 빠르고 디버깅도 편하다.  
MLlib는 분산 오버헤드가 있기 때문에 데이터가 작으면 오히려 느리다.  



# 02.Pipeline

Pipeline 구조는 **전처리 → 학습 → 예측을 하나의 객체로 묶는 것**.

## (1)Pipeline이 필요한 이유

**이전 방식의 문제**

- 전처리(scaler, encoding) → 학습 → 예측을 각각 따로 실행하면
- train/test 각각에 동일한 전처리를 **수동으로 반복** 적용해야 함
- 실수가 생기기 쉽고 (train에만 fit, test에 transform 빠뜨리는 등)
- 배포할 때 전처리 코드와 모델 코드를 **따로 관리**해야 하는 문제 발생

Pipeline은 이걸 하나의 객체로 묶어서 저장/배포/재사용을 단순하게 만든다.

## (2)구성 요소

1. **Transformer** : `transform()` 메서드만 있음. 데이터를 변환만 함
    - 예: `VectorAssembler`, `StandardScaler`, `StringIndexer`
2. **Estimator** : `fit()` 메서드가 있음. 데이터를 학습해서 Transformer를 반환
    - 예: `LogisticRegression`, `RandomForestClassifier`
    - `fit()` 호출 → `Model` 객체(Transformer) 반환
3. **Pipeline** : Transformer와 Estimator를 stages 리스트로 순서대로 묶음

### VectorAssembler
- VectorAssembler는 **여러 컬럼을 하나의 Vector 컬럼으로 합치는 것**
#### 왜 필요한가?
Spark ML 모델은 입력으로 **반드시 Vector 타입 컬럼 하나**만 받는다. 

scikit-learn →  DataFrame 단위로 데이터 전달함
```
model.fit(df[["age", "total_purchase", "login_count"]])
```

Spark ML → Dataframe단위로 전달 불가능, 한 행(벡터) 단위로 전달함
```
model.fit(df["features"])  # Vector 타입 컬럼 하나
```

#### (예시)VectorAssembler가 하는 일
Before (입력)
```
| age | total_purchase | login_count |
|-----|----------------|-------------|
| 25  | 150.0          | 10          |
| 45  | 20.0           | 1           |
```

After (출력)
```
| features              |
|-----------------------|
| [25.0, 150.0, 10.0]   |
| [45.0, 20.0,  1.0]    |
```
3개 컬럼이 Vector 1개로 합쳐주는 작업 진행

## (3)실행 흐름

```
VectorAssembler (Transformer)
        ↓
StandardScaler (Estimator → fit() → ScalerModel)
        ↓
LogisticRegression (Estimator → fit() → LogisticRegressionModel)
        ↓
pipeline.fit(train_df) 호출 시 위 순서대로 자동 실행
        ↓
PipelineModel 반환 (저장 가능)
        ↓
pipelineModel.transform(test_df) → 예측 결과
```


## (4)핵심 포인트

- `pipeline.fit(train_df)` 한 번 호출하면 stages 전체가 순서대로 실행됨
- 반환된 `PipelineModel`은 **전처리 파라미터 + 모델 가중치**를 모두 포함
- `pipelineModel.save(path)` 로 통째로 저장, `PipelineModel.load(path)` 로 불러와서 바로 예측 가능
- train에서 fit한 scaler 파라미터가 test에 그대로 적용되는 것을 **자동으로 보장**


## (5)예시

도메인 : 전자상거래 사용자 이탈 예측 (churn prediction)
### Step01. 데이터 설정

| 컬럼              | 설명            | 타입        |
| --------------- | ------------- | --------- |
| user_id         | 사용자 ID        | string    |
| age             | 나이            | int       |
| total_purchase  | 총 구매금액        | float     |
| login_count     | 최근 30일 로그인 수  | int       |
| last_login_days | 마지막 로그인 후 경과일 | int       |
| churn           | 이탈 여부 (label) | int (0/1) |

### Step02.코드

```
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# 1. 샘플 데이터
data = [
    (1, 25, 150.0, 10, 3, 0),
    (2, 45, 20.0,  1,  30, 1),
    (3, 32, 300.0, 15, 1,  0),
    (4, 60, 5.0,   0,  60, 1),
    (5, 28, 200.0, 12, 2,  0),
]
columns = ["user_id", "age", "total_purchase", "login_count", "last_login_days", "churn"]
df = spark.createDataFrame(data, columns)



# 2. feature 컬럼 목록 지정
feature_cols = ["age", "total_purchase", "login_count", "last_login_days"]

# 3. VectorAssembler : 여러 컬럼 → features 벡터 1개로 합침
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")

# 4. StandardScaler : features_raw → 평균0 표준편차1로 정규화
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)



# 5. LogisticRegression : features → 예측
lr = LogisticRegression(featuresCol="features", labelCol="churn")



# 6. Pipeline 조립
pipeline = Pipeline(stages=[assembler, scaler, lr])


# 7. 학습
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train_df)


# 8. 예측
predictions = model.transform(test_df)
predictions.select("user_id", "churn", "prediction", "probability").show()

#출력결과
+-------+-----+----------+----------------------------------------+
|user_id|churn|prediction|probability                             |
+-------+-----+----------+----------------------------------------+
|2      |1    |1.0       |[0.21, 0.79]                            |
+-------+-----+----------+----------------------------------------+


# 9. 모델 저장 
model.save("s3://my-bucket/models/churn_model_v1") 

# 10. 모델 로드 후 예측 
loaded_model = PipelineModel.load("s3://my-bucket/models/churn_model_v1") predictions = loaded_model.transform(test_df)
```
#### transform() 이 하는 일

`model.transform(test_df)` 를 호출하면 
test_df에 아래 3개 컬럼이 추가된 DataFrame이 반환된다.

|컬럼|타입|설명|
|---|---|---|
|`rawPrediction`|Vector|각 클래스의 raw 점수|
|`probability`|Vector|각 클래스의 확률 합계 1.0|
|`prediction`|double|최종 예측 클래스 (0.0 or 1.0)|

#### 실제 출력
```
+-------+-----+----------+-------------------+
|user_id|churn|prediction|probability        |
+-------+-----+----------+-------------------+
|2      |1    |1.0       |[0.21, 0.79]       |
+-------+-----+----------+-------------------+
```
- churn=1 (실제 이탈)
- prediction=1.0 (모델도 이탈로 예측)
- probability[1]=0.79 → 79% 확률로 이탈 예측 → threshold 0.5 초과 → prediction=1.0
#### probability 읽는 법
```
[0.21, 0.79]
```
- 인덱스 0 → churn=0 (이탈 안함) 확률 21%
- 인덱스 1 → churn=1 (이탈) 확률 79%
- 둘을 합치면 반드시 1.0

#### 모델 저장 방식 2가지

**1. 그냥 저장 (경로 없으면 저장, 있으면 에러)**

```
model.save("s3://my-bucket/models/churn_model_v1")
```

- 경로가 이미 존재하면 **에러 발생**
- 실수로 덮어쓰는 걸 방지

**2. 덮어쓰기 저장**

```
model.write().overwrite().save("s3://my-bucket/models/churn_model_v1")
```

- 경로가 이미 존재해도 **그냥 덮어씀**
- 재학습 후 같은 경로에 업데이트할 때 사용

**3.실무에서 언제 쓰나**

- `save()` : 처음 저장할 때, 또는 버전 경로를 매번 새로 만들 때

```
model.save("s3://my-bucket/models/churn_model_v2")  # 새 버전
```

- `write().overwrite().save()` : Airflow DAG에서 매일 재학습 후 같은 경로에 덮어쓸 때

```
# 매일 재학습 후 latest 경로에 덮어씀
model.write().overwrite().save("s3://my-bucket/models/churn_model_latest")
```

실무 패턴은 두 가지를 같이 쓴다.
- `churn_model_latest` 경로에는 `overwrite()` 로 항상 최신 모델 유지
- `churn_model_v1`, `v2` 경로에는 `save()` 로 버전별 보존


### Step03.핵심 코드 

```
pipeline = Pipeline(stages=[assembler, scaler, lr])
model = pipeline.fit(train_df)
```
- `stages` 리스트 순서대로 자동 실행됨
- `fit()` 한 번으로 assembler → scaler fit → lr fit 전부 처리
- 반환된 `model`은 `PipelineModel` 타입으로, 전처리 파라미터와 모델 가중치를 모두 포함

### Step04.엔지니어 독백
> Pipeline 저장을 꼭 습관화해.
>  `model.save("s3://bucket/churn_model")` 이렇게 저장하면 
>  나중에 Airflow DAG에서 `PipelineModel.load()` 로 불러와서 
>  바로 배치 예측 돌릴 수 있거든. 
>  전처리 코드 따로 관리하다가 버전 안 맞아서 
>  예측 결과 이상해지는 사고를 한 번이라도 겪어보면 
>  Pipeline으로 묶어서 저장하는 게 얼마나 중요한지 알게 돼.


## (6)모델 평가
모델 평가는 **예측 결과가 실제 정답과 얼마나 일치하는지 수치로 측정**하는 것

### BinaryClassificationEvaluator 가 보는 지표
2가지 지표를 제공한다.

| 지표             | 설명                          | 기본값 |
| -------------- | --------------------------- | --- |
| `areaUnderROC` | ROC 커브 아래 면적. 1.0에 가까울수록 좋음 | 기본값 |
| `areaUnderPR`  | Precision-Recall 커브 아래 면적   | 옵션  |

실무에서는 **이탈 예측처럼 불균형 데이터(이탈자가 적음)일 때** `areaUnderPR`을 더 신뢰한다. `areaUnderROC`는 클래스 불균형이 심하면 실제보다 높게 나오는 경향이 있기 때문이다.


### 코드
```
from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator(
    labelCol="churn",           # 실제 정답 컬럼
    rawPredictionCol="rawPrediction",  # transform() 이 만든 컬럼
    metricName="areaUnderROC"   # 평가 지표
)

auc = evaluator.evaluate(predictions)
print(f"AUC: {auc:.4f}")
```

코드해석
```
auc = evaluator.evaluate(predictions)
```

- `predictions` 는 앞서 `model.transform(test_df)` 로 만든 DataFrame
- `rawPrediction` 컬럼을 기준으로 AUC를 계산함
- `probability` 가 아니라 `rawPrediction` 을 쓰는 이유는 AUC 계산이 threshold 없이 연속적인 점수 기반으로 하기 때문

#### 출력

```
AUC: 0.8750
```

- 0.5 = 랜덤 수준 (의미없음)
- 0.7 이상 = 실무에서 사용 가능한 수준
- 0.9 이상 = 좋은 모델


### 엔지니어 독백
> AUC 하나만 보고 모델 좋다고 판단하면 안 돼. 이탈 예측 같은 경우 실제 이탈자가 전체의 5%밖에 안 되는 경우가 많아. 이때 모델이 전부 "이탈 안 함"으로 예측해도 accuracy는 95%가 나와. AUC도 높게 나올 수 있어.

> 그래서 실무에서는 `areaUnderPR` 같이 보고, 추가로 threshold 조정해서 Precision/Recall 균형을 비즈니스 요구에 맞게 튜닝하는 작업까지 해야 진짜 평가가 끝난 거야.






# 03.파라미터 튜닝 및 저장

## (1)파라미터 튜닝 - CrossValidator

- CrossValidator는 **여러 하이퍼파라미터 조합을 자동으로 시도해서 가장 좋은 모델을 찾는 것**
- **scikit-learn의 GridSearchCV랑 완전히 같은 개념**.

| | scikit-learn | Spark MLlib |
|---|---|---|
| 파라미터 조합 정의 | `param_grid = {"regParam": [...]}` | `ParamGridBuilder().addGrid(...)` |
| CV 실행 | `GridSearchCV(model, param_grid, cv=3)` | `CrossValidator(pipeline, paramGrid, numFolds=3)` |
| 학습 | `grid.fit(X_train, y_train)` | `cv.fit(train_df)` |
| 최적 모델 | `grid.best_estimator_` | `cvModel.bestModel` |
| 최적 파라미터 | `grid.best_params_` | `bestModel.stages[-1].getRegParam()` |



### 배경 / 등장 이유

- 모델 성능은 하이퍼파라미터(학습률, 트리 깊이 등)에 따라 크게 달라짐
- 수동으로 하나씩 바꿔가며 테스트하면 시간도 오래 걸리고 실수도 많음
- 그래서 조합을 자동으로 돌리고 가장 좋은 걸 뽑아주는 CrossValidator가 등장

### Cross Validation 개념

데이터를 K개로 나눠서 K번 학습/검증을 반복한다.

```
전체 데이터
[ fold1 | fold2 | fold3 ]

1회차 : fold1+fold2 학습 → fold3 검증
2회차 : fold1+fold3 학습 → fold2 검증
3회차 : fold2+fold3 학습 → fold1 검증

→ 3번의 AUC 평균으로 해당 파라미터 조합의 성능 측정
```


### 구성 요소

|구성요소|역할|
|---|---|
|`Pipeline`|학습할 모델|
|`ParamGridBuilder`|시도할 파라미터 조합 목록|
|`CrossValidator`|조합마다 CV 실행 후 최적 모델 반환|


### 코드

```
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# 파라미터 조합 정의
paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01, 0.1, 1.0]) \
    .addGrid(lr.maxIter, [10, 50]) \
    .build()
# regParam : 규제 강도 (클수록 과적합 방지)
# maxIter : 최대 반복 학습 횟수
# 총 3 x 2 = 6가지 조합 시도

evaluator = BinaryClassificationEvaluator(
    labelCol="churn",
    metricName="areaUnderROC"
)

cv = CrossValidator(
    estimator=pipeline,      # 학습할 Pipeline
    estimatorParamMaps=paramGrid,  # 시도할 파라미터 조합
    evaluator=evaluator,     # 평가 지표
    numFolds=3,              # K=3
    seed=42
)

cvModel = cv.fit(train_df)  # 6조합 x 3fold = 18번 학습 실행

# 최적 모델로 예측
predictions = cvModel.transform(test_df)
print(f"Best AUC: {evaluator.evaluate(predictions):.4f}")
```


### 핵심 코드

```
cvModel = cv.fit(train_df)
```

- 이 한 줄로 18번 학습이 자동 실행됨
- 내부적으로 AUC가 가장 높은 파라미터 조합을 찾아서
- `cvModel` 안에 최적 파라미터로 학습된 `PipelineModel`을 저장함



### 최적 파라미터 확인

```
bestModel = cvModel.bestModel
lr_model = bestModel.stages[-1]  # Pipeline의 마지막 stage = LR 모델
print(f"Best regParam: {lr_model._java_obj.getRegParam()}")
print(f"Best maxIter: {lr_model._java_obj.getMaxIter()}")
```



### 실무 주의사항

- `numFolds=3`, 파라미터 조합 6개 → 18번 학습
- 조합이 많아질수록 Spark 클러스터에 **엄청난 부하**
- 실무에서는 조합을 최대 20개 이하로 제한하고, 먼저 넓은 범위로 탐색 후 좁혀가는 방식 사용



### 엔지니어 독백

> CrossValidator 처음 쓰면 파라미터 조합을 엄청 많이 넣고 싶은 충동이 생겨. 근데 EMR 클러스터에서 조합 50개에 fold 5개 돌렸다가 4시간 넘게 돌아가는 거 한 번 겪어봐. 그 다음부턴 조합 줄이게 돼.

> 실무 패턴은 이래. 먼저 regParam을 [0.001, 0.01, 0.1, 1.0] 넓게 탐색해서 대략적인 범위 잡고, 그 다음 좁은 범위로 fine-tuning 하는 2단계 방식으로 가. 한 번에 완벽한 파라미터 찾으려다 클러스터 비용만 날리는 경우 많아.



## (2) 모델 저장/배포


### 모델 저장/배포 (PipelineModel.save)

모델 저장/배포는 **학습된 PipelineModel을 파일로 저장하고 나중에 불러와서 예측에 재사용**하는 것이다.


#### 왜 필요한가
- 매번 학습을 새로 돌리면 시간/비용 낭비
- 학습은 한 번만 하고, 예측은 저장된 모델을 불러와서 실행
- 전처리 파라미터(scaler 평균/표준편차)와 모델 가중치를 **통째로 저장**하기 때문에 버전 불일치 문제가 없음


#### 저장/로드 코드
```
# 저장 
cvModel.bestModel.save("s3://my-bucket/models/churn_model_v1") 

# 로드 
from pyspark.ml import PipelineModel 
loaded_model = PipelineModel.load("s3://my-bucket/models/churn_model_v1") 

# 바로 예측 
predictions = loaded_model.transform(new_df)
```



#### 저장되는 구조

```
churn_model_v1/
├── metadata/        # Pipeline stages 구조 정보
├── stages/
│   ├── 0_VectorAssembler/   # assembler 설정
│   ├── 1_StandardScaler/    # scaler 평균/표준편차
│   └── 2_LogisticRegression/ # 모델 가중치
```
​stages 폴더 안에 각 단계별 파라미터가 전부 저장된다.


### 실무 배포 패턴

**배치 예측 (Airflow DAG)**

```
# 매일 새 데이터에 대해 이탈 예측 실행
loaded_model = PipelineModel.load("s3://my-bucket/models/churn_model_v1")

new_df = spark.read.parquet("s3://my-bucket/data/daily/2026-04-17/")


predictions = loaded_model.transform(new_df)
predictions.write.parquet("s3://my-bucket/predictions/2026-04-17/")
```

---

### 모델 버전 관리 패턴
실무에서는 날짜나 버전을 경로에 포함시킨다.
```
s3://my-bucket/models/churn_model_v1/   # 첫 번째 모델
s3://my-bucket/models/churn_model_v2/   # 파라미터 튜닝 후
s3://my-bucket/models/churn_model_v3/   # 피처 추가 후
```


### 엔지니어 독백

> 모델 저장할 때 로컬 경로 쓰지 마. 
> EMR 클러스터는 작업 끝나면 내려가버려서 
> 로컬에 저장한 모델 다 날아가. 항상 S3에 저장하는 습관 들여.

> 그리고 버전 관리 꼭 해. 
> 나중에 모델 성능이 갑자기 떨어졌을 때 
> 이전 버전으로 롤백해야 하는 상황이 반드시 생겨. 
> 그때 버전별로 저장 안 해놨으면 답이 없어. 
> MLflow 같은 툴로 모델 메타데이터까지 관리하면 더 좋고.
