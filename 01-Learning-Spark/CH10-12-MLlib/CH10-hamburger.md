## 머신러닝(ML)이란 무엇인가?

머신러닝은 통계, 선형 대수 및 수치 최적화를 사용하여 데이터에서 패턴을 추출하는 프로세스이다. 크게 두 가지 유형으로 나뉜다.

-   지도 학습 (Supervised Learning): 레이블(정답)이 있는 데이터를 학습하여 새로운 입력에 대한 결과를 예측
-   분류(Classification): 이진 분류(개/고양이), 다항 분류(견종 구분) 등 이산적인 값을 예측
-   회귀(Regression): 온도에 따른 아이스크림 판매액 예측 등 연속적인 숫자를 예측
-   비지도 학습 (Unsupervised Learning): 레이블이 없는 데이터의 구조를 이해하는 데 도움을 줌
-   클러스터링(Clustering): 유사한 특성을 가진 항목끼리 그룹화

왜 스파크(Spark) MLlib인가?

스파크는 데이터 수집부터 피처 엔지니어링, 모델 교육 및 배포까지 하나의 에코시스템에서 처리할 수 있는 통합 분석 엔진이다.

두 가지 패키지:

1\. spark.mllib: RDD 기반의 레거시 API (유지 관리 모드)

2\. spark.ml: 데이터 프레임 기반의 최신 API (권장)

확장성: 데이터 포인트 수에 따라 선형적으로 확장($O(n)$)되어 방대한 양의 데이터를 처리할 수 있다.

## ML 파이프라인의 핵심 구성 요소

스파크 ML 파이프라인은 일련의 워크플로를 표준화하기 위해 세 가지 주요 개념을 사용

| 개념 | 설명 | 주요 메서드 |
| --- | --- | --- |
| 변환기 (Transformer) | 데이터를 변환하여 새로운 열을 추가함. | .transform() |
| 추정기 (Estimator) | 데이터를 학습(Fitting)하여 모델(변환기)을 생성함. | .fit() |
| 파이프라인 (Pipeline) | 여러 변환기와 추정기를 하나의 워크플로로 묶음. | .fit() / .transform() |

## 실습

파이썬(PySpark)을 사용하여 Parquet 데이터를 불러오고 필요한 컬럼을 선택한다.

```
# 파이썬 예제
filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
airbnbDF = spark.read.parquet(filePath)
airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "price").show(5)
```

학습 및 테스트 데이터셋 생성

모델의 일반화 성능을 측정하기 위해 데이터를 80(학습):20(테스트) 비율로 분할한다.

재현성을 위해 seed 값을 설정하는 것이 중요!

```
trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)
print(f"학습 데이터: {trainDF.count()}, 테스트 데이터: {testDF.count()}")
```

스파크의 머신러닝 알고리즘은 입력 기능을 단일 벡터 형태의 컬럼으로 전달받아야 한다.

이때 사용하는 것이 VectorAssembler 이다.  
\-> 역할: 여러 개의 숫자형 컬럼을 하나의 벡터 컬럼(features)으로 결합한다.

```
from pyspark.ml.feature import VectorAssembler

# 'bedrooms' 컬럼을 'features'라는 이름의 벡터로 변환
vecAssembler = VectorAssembler(inputCols=["bedrooms"], outputCol="features")
vecTrainDF = vecAssembler.transform(trainDF)

vecTrainDF.select("bedrooms", "features", "price").show(10)
```

## 선형 회귀(Linear Regression) 이해하기

선형 회귀는 종속 변수(예: 가격)와 하나 이상의 독립 변수(예: 침실 수) 간의 선형 관계를 모델링하는 알고리즘이다.

<img width="213" height="30" alt="스크린샷 2026-04-17 22 13 53" src="https://github.com/user-attachments/assets/cd69341a-ff2a-47d4-ba68-8c253e58fba6" />


-   y: 예측하려는 값 (Label)
-   x: 입력 데이터 (Feature)
-   w: 학습을 통해 찾아낼 가중치 (Coefficients)
-   E: 오차 항 (Residual)

목적은 실제 값과 예측 값의 차이인 잔차(Residual)의 제곱합을 최소화하는 최적의 선을 찾는 것이다.

## 추정기(Estimator)를 사용한 모델 구축

스파크에서 LinearRegression은 추정기(Estimator)이다.

.fit() 메서드를 호출하면 데이터를 학습하여 LinearRegressionModel(변환기)을 반환한다.

```
from pyspark.ml.regression import LinearRegression

# 모델 초기화 (입력 벡터 컬럼과 정답 레이블 컬럼 지정)
lr = LinearRegression(featuresCol="features", labelCol="price")

# 학습 데이터로 모델 학습
lrModel = lr.fit(vecTrainDF)

# 학습된 계수 및 절편 확인
print(f"Coefficients: {lrModel.coefficients}")
print(f"Intercept: {lrModel.intercept}")
```

## ML 파이프라인(Pipeline)으로 워크플로 자동화

데이터 변환(VectorAssembler)과 모델 학습(LinearRegression)을 매번 따로 실행하는 것은 번거롭다.

Pipeline을 사용하면 이 과정을 하나로 묶어 관리할 수 있다.

-   **장점:** 코드 재사용성이 높아지고, 테스트 데이터에도 동일한 변환 과정을 손쉽게 적용할 수 있다.
-   **구성:** Pipeline(stages=\[step1, step2, ...\])

```
from pyspark.ml import Pipeline

# 파이프라인 구성 및 학습
pipeline = Pipeline(stages=[vecAssembler, lr])
pipelineModel = pipeline.fit(trainDF)

# 테스트 데이터에 예측 적용
predDF = pipelineModel.transform(testDF)
predDF.select("features", "price", "prediction").show(5)
```

## 범주형 데이터 처리: 원-핫 인코딩 (OHE)

머신러닝 모델은 문자열을 직접 이해할 수 없다. 따라서 '개', '고양이' 같은 범주형 데이터를 숫자로 변환해야 한다.

**왜 원-핫 인코딩인가?**

단순히 '개=1, 고양이=2, 물고기=3'으로 바꾸면, 모델은 고양이가 개의 2배라는 잘못된 관계를 학습할 수 있다.

**원-핫 인코딩**은 각 카테고리를 독립된 컬럼(0 또는 1)으로 만들어 이를 방지한다.

**스파크에서의 단계 (StringIndexer → OneHotEncoder)**

1.  **StringIndexer:** 문자열 레이블을 숫자 인덱스로 변환
2.  **OneHotEncoder:** 숫자 인덱스를 이진 벡터로 변환

```
from pyspark.ml.feature import StringIndexer, OneHotEncoder

# 1. 문자열을 숫자로
indexer = StringIndexer(inputCols=["room_type"], outputCols=["room_type_Index"], handleInvalid="skip")

# 2. 숫자를 원-핫 벡터로
encoder = OneHotEncoder(inputCols=["room_type_Index"], outputCols=["room_type_OHE"])
```

만약 위 과정들이 너무 복잡하다면 RFormula를 사용할 수 있다.

R 언어의 선언적 구문을 사용하여 레이블과 피처를 정의하며, StringIndexing과 원-핫 인코딩을 자동으로 처리해준다.

-   **구문:** price ~ . (price를 제외한 모든 컬럼을 피처로 사용)
-   **주의점:** 모든 알고리즘에 원-핫 인코딩이 최선은 아님(예: 트리 기반 모델)

```
from pyspark.ml.feature import RFormula

rFormula = RFormula(formula="price ~ .", featuresCol="features", labelCol="price", handleInvalid="skip")
```
