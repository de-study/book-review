# Learning Spark - 아파치 스파크로 머신러닝 파이프라인 관리, 배포 및 확장

> 출처: Learning Spark (2nd Edition) | 실무 데이터 엔지니어링 관점 정리

---

## 목차

1. [모델 관리](#1-모델-관리)
   - MLflow
2. [MLlib을 사용한 모델 배포 옵션](#2-mllib을-사용한-모델-배포-옵션)
   - 배치
   - 스트리밍
   - 실시간 추론을 위한 모델 내보내기 패턴
3. [비MLlib 모델에 스파크 활용](#3-비mllib-모델에-스파크-활용)
   - 판다스 UDF
   - 분산 하이퍼파라미터 조정을 위한 스파크
4. [요약](#4-요약)

---

## 전체 구조 개요

```
ML 파이프라인 관리 및 배포 전체 흐름
==========================================================================

[실험/학습]
    │
    ├──► MLflow Tracking   ← 파라미터, 지표, 모델 자동 기록
    │         │
    │         ▼
    │    MLflow Model Registry
    │         │
    │    Staging → Production → Archived
    │
    ▼
[모델 배포 방식 선택]
    │
    ├──► 배치 추론
    │    Spark DataFrame + PipelineModel.transform()
    │    → 대용량 스코어링, 주기적 실행
    │
    ├──► 스트리밍 추론
    │    Structured Streaming + MLlib 모델
    │    → 실시간 이벤트 스코어링
    │
    └──► 실시간 추론 (REST API)
         MLlib → ONNX / MLeap → REST 서버
         → 저지연 단건 요청 처리

==========================================================================

[비MLlib 모델 스파크 활용]

    scikit-learn / XGBoost / PyTorch
              │
    ┌─────────┴─────────┐
    ▼                   ▼
판다스 UDF           Hyperopt + Spark
(분산 배치 추론)     (분산 하이퍼파라미터 탐색)
```

---

## 1. 모델 관리

머신러닝 모델 배포하기 전에 모델의 성능을 재현하고 추적할 수 있는지 확인해야 함.
**머신러닝 솔루션의 종단 간 재현성**: 모델을 생성한 코드, 훈련에 사용된 환경, 훈련된 데이터 및 모델 자체를 재현할 수 있어야 함.
- 라이브러리 버전 관리
- 데이터 진화
- 실행 순서
- 병렬 작업

모델 관리는 학습 실험 추적, 모델 버전 관리, 운영 배포 승인 프로세스를 포함.
코드는 Git으로 관리하듯, 모델은 MLflow로 관리함.

### 1-1. MLflow

MLflow는 ML 실험 추적, 모델 패키징, 모델 레지스트리를 제공하는 오픈소스 플랫폼.
Spark와 통합이 잘 되어 있어 MLlib 모델을 자연스럽게 관리할 수 있음.

| MLflow 구성 요소 |                     설명                           |
|----------------|---------------------------------------------------|
| 트래킹 | 코드, 데이터 구성 및 결과 등 실험을 기록하고 쿼리한다                   | 
| 프로젝트 | 모든 플랫폼에서 실행을 재현할 수 있는 형식으로 데이터 과학 코드를 패키징한다 | 
| 모델 | 다양한 서비스 환경에 머신러닝 모델을 배포한다                           | 
| 레지스트리 | 중앙 저장소에서 모델을 저장, 주석 달기, 검색 그리고 관리한다           | 

- **목적**
  - 실험별 파라미터, 지표, 아티팩트를 자동으로 기록하고 비교
  - 모델을 중앙 레지스트리에서 버전 관리하고 운영 배포 프로세스를 표준화
- **이유**
  - 실험 결과를 노트나 파일로 관리하면 "어떤 조건에서 최고 성능이 나왔는지" 추적 불가
  - 팀 단위로 모델을 운영하려면 학습 재현성과 배포 이력이 공식적으로 관리되어야 함
- **결과**
  - 모든 `mlflow.log_*` 호출이 MLflow 서버에 기록되어 UI에서 실험 비교 가능
  - Model Registry에서 모델 상태(Staging → Production)를 명시적으로 관리
- **장점**
  - 코드 한 줄(`mlflow.autolog()`)로 파라미터/지표 자동 기록
  - 모델을 `mlflow.pyfunc` 형식으로 저장하면 프레임워크 무관하게 로드 가능
  - Databricks, AWS SageMaker 등 플랫폼과 통합 지원
- **단점**
  - 자체 MLflow 서버 운영 시 인프라 관리 비용 발생
  - 대규모 팀에서 실험이 쌓이면 레지스트리 관리가 복잡해짐

```python
import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# MLflow 실험 설정
mlflow.set_experiment("/airbnb-price-prediction")

with mlflow.start_run(run_name="rf-baseline") as run:
    # 하이퍼파라미터 기록
    mlflow.log_param("num_trees", 100)
    mlflow.log_param("max_depth", 5)

    # 학습
    pipeline_model = pipeline.fit(train_df)
    predictions = pipeline_model.transform(test_df)

    # 지표 기록
    evaluator = RegressionEvaluator(
        labelCol="price",
        predictionCol="prediction",
        metricName="rmse",
    )
    rmse = evaluator.evaluate(predictions)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", RegressionEvaluator(
        labelCol="price", predictionCol="prediction", metricName="r2"
    ).evaluate(predictions))

    # 모델 저장 (MLflow 형식)
    mlflow.spark.log_model(
        pipeline_model,
        artifact_path="model",
        registered_model_name="airbnb-price-rf",  # Model Registry 등록
    )

    print(f"Run ID: {run.info.run_id}")
    print(f"RMSE: {rmse:.2f}")
```

#### 트래킹 서버

트래킹 서버는 ML 실험의 모든 실행(Run) 정보를 중앙에서 저장하고 조회할 수 있게 해주는 서버.
`mlflow.start_run()` 컨텍스트 내에서 기록한 모든 정보가 이 서버에 누적되어 실험 간 비교와 재현이 가능해짐.

트래킹 서버에 기록할 수 있는 항목은 다섯 가지로 구분됨.

| 항목 | API | 설명 |
|------|-----|------|
| 매개변수(Parameters) | `mlflow.log_param()` | 학습에 사용된 하이퍼파라미터 등 고정된 입력값. 실행당 한 번만 기록 |
| 메트릭(Metrics) | `mlflow.log_metric()` | RMSE, Accuracy 등 수치 지표. 스텝(step)별로 여러 번 기록 가능 |
| 아티팩트(Artifacts) | `mlflow.log_artifact()` | 파일 형태의 산출물. 피처 중요도 그래프, 혼동 행렬 이미지, CSV 등 |
| 메타데이터(Tags) | `mlflow.set_tag()` | 실험 목적, 담당자, 데이터 버전 등 실행에 붙이는 레이블 |
| 모델(Models) | `mlflow.spark.log_model()` | 직렬화된 모델 전체. Model Registry와 연동해 버전 관리 가능 |

```
트래킹 서버 기록 흐름
--------------------------------------------------------------
mlflow.start_run()
    │
    ├─► log_param("max_depth", 5)           → 매개변수 저장소
    ├─► log_metric("rmse", 0.83)            → 메트릭 저장소 (스텝별 시계열)
    ├─► log_artifact("feat_importance.png") → 아티팩트 스토리지 (S3/로컬)
    ├─► set_tag("data_version", "v3")       → 태그/메타데이터
    └─► log_model(pipeline_model, ...)      → 모델 아티팩트 + Registry 등록
          │
          ▼
    MLflow UI (실험 비교, 메트릭 시각화, 아티팩트 다운로드)
--------------------------------------------------------------
```

```
MLflow Model Registry 상태 전이
--------------------------------------------------------------
None
  │
  ▼ (log_model 후 등록)
Staging       ← 검증 완료 전, 테스트 환경 배포
  │
  ▼ (승인 후)
Production    ← 운영 환경에서 실제 서빙 중
  │
  ▼ (신규 버전 배포 후)
Archived      ← 운영 종료, 이력 보존

각 버전은 독립적으로 상태 전이 가능
예: v3 Production 상태에서 v4 Staging 동시 존재 가능
--------------------------------------------------------------
```

```python
import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Staging → Production으로 상태 변경
client.transition_model_version_stage(
    name="airbnb-price-rf",
    version=3,
    stage="Production",
    archive_existing_versions=True,  # 기존 Production 버전 자동 Archived 처리
)

# Production 모델 로드
model_uri = "models:/airbnb-price-rf/Production"
loaded_model = mlflow.spark.load_model(model_uri)

# 추론
predictions = loaded_model.transform(test_df)
```

> **트러블 로그** — MLflow 없이 모델을 운영하면 "지금 서빙 중인 모델이 어떤 데이터로 학습됐는지"를 알 수 없게 됨.
> 예: 운영 중 예측값이 이상하다는 알림이 왔을 때, 학습 파라미터와 데이터 버전 기록이 없으면
> 원인 파악 자체가 불가능해 새로 학습하는 것 외에 선택지가 없어짐.
> `mlflow.autolog()`를 기본으로 켜두고, `run_name`은 반드시 의미있는 이름으로 설정할 것.

---

## 2. MLlib을 사용한 모델 배포 옵션

학습된 MLlib 모델을 실제 서비스에 적용하는 방식은 요구 지연시간과 데이터 볼륨에 따라 세 가지로 나뉨.
잘못된 배포 방식 선택은 불필요한 인프라 비용이나 응답 지연으로 이어짐.

```
배포 방식 선택 기준
--------------------------------------------------------------
요구사항                            추천 방식
----------------------------    ------------------------------
대용량 데이터, 주기 실행               배치 추론
실시간 이벤트 스트림 처리              스트리밍 추론
단건 요청, 저지연 (< 100ms)          모델 내보내기 + REST API
--------------------------------------------------------------
```

| 배포 방식 | 처리량 | 지연시간 | 애플리케이션 예제 |
|---------|------|--------| ------------ |
| 배치 | 높음 | 높음 (몇 시간에서 며칠) | 고객 이탈 예측 |
| 스트리밍 | 중간 | 중간 (몇 초에서 몇 분) | 동적 가격 책정 |
| 실시간 | 낮음 | 낮음 (몇 밀리초) | 온라인 광고 입찰 |

#### 배포 방식 비교

세 가지 방식은 처리 단위와 Spark가 관여하는 방식이 근본적으로 다름.
어떤 방식을 선택하느냐가 인프라 구성, 지연시간, 비용에 직접적인 영향을 줌.

**배치 추론**
- **장점**
  - 예정된 실행 중에만 컴퓨팅 비용 지불하면 되기 때문에 일반적으로 가장 저렴하고 쉬운 방식
  - 구현이 가장 단순하며, 기존 Spark 배치 파이프라인과 동일한 구조로 작성 가능
  - 모든 예측 작업이 분할되어서 누적되는 오버헤드가 적기 때문에 데이터 포인트당훨씬 더 효율적
  - 클러스터 전체 자원을 스코어링에 집중시켜 수억 건을 단시간에 처리 가능
  - 결과를 Delta/S3에 미리 저장해두므로 서빙 레이어의 부하가 없음
- **단점**
  - 마지막 배치 실행 이후 발생한 이벤트에 대해 예측값을 즉시 제공 불가
  - 스케줄 주기(일/시간 단위) 동안 예측값이 stale 상태로 유지됨
  - 다음 예측 배치를 행성하기 위해 몇 시간 또는 며칠로 예약되기 때문에 지연 시간이 발생
- **Spark 동작 방식**
  - `PipelineModel.load()` → `model.transform(df)` → 결과를 스토리지에 `write`
  - Spark 드라이버가 실행 계획을 수립하고, 워커들이 파티션 단위로 병렬 스코어링 수행

**스트리밍 추론**
- **장점**
  - 처리량과 대기 시간 간의 적절한 균형 제공
  - 배치 추론 코드와 API가 거의 동일해 학습-서빙 간 코드 불일치가 최소화됨
  - 마이크로배치 단위(수 초)로 이벤트를 처리해 준실시간 응답 가능
    - 마이크로 데이터 배치에 대해 지속적으로 예측하고 몇 초에서 몇 분만에 예측 얻을 수 있음
  - Structured Streaming 사용하는 경우 거의 모든 코드가 배치 사용 사례와 동일하게 표시되므로 두 옵션사이 쉽게 오갈 수 있음
  - Delta Lake sink와 결합하면 배치/스트리밍 결과를 동일 테이블에서 통합 조회 가능
- **단점**
  - 체크포인트 관리, 워터마크, 상태 저장소(StateStore) 등 운영 복잡도가 높음
  - 마이크로배치 지연이 있어 진짜 실시간(< 100ms) 요건에는 부적합
  - 스트리밍 잡이 상시 실행되므로 클러스터 비용이 지속적으로 발생
- **Spark 동작 방식**
  - `readStream` → `model.transform(streamDF)` → `writeStream`
  - Structured Streaming이 마이크로배치마다 새로운 이벤트를 수집 후 모델 추론 적용
  - MLlib 모델은 상태를 갖지 않으므로 배치와 동일한 `transform()` API를 스트리밍에 그대로 사용

**실시간 추론 (REST API)**
- **장점**
  - 처리량보다 대기 시간을 우선시하고, 단건 요청에 밀리초 수준의 응답 제공 가능
  - 인프라는 로드 밸런싱 지원해야 하고 수요 급증하는 경우 많은 동시 요청으로 확장할 수 있어야 함
  - 실시간으로 모델 예측을 생성하는 것을 의미한다
  - Spark 클러스터 없이 경량 서버(Flask, FastAPI, mlflow models serve)로 운영 가능
  - MLflow pyfunc 포맷을 사용하면 모델 프레임워크와 무관하게 동일한 서빙 인터페이스 제공
- **단점**
  - 실시간 배포는 스파크가 대기 시간 요구사항을 충족할 수 없는 유일한 옵션이라 스파크 외부로 모델을 내보내야 함
  - MLlib 모델을 Spark 없이 서빙 가능한 포맷(MLeap, ONNX, pyfunc)으로 변환해야 함
  - 변환 시 일부 트랜스포머가 지원되지 않을 수 있으며, 원본과 예측값 동일성 검증 필수
  - 대량 동시 요청 시 수평 확장을 별도로 설계해야 함 (Spark가 자동으로 처리해주지 않음)
- **Spark 동작 방식**
  - Spark는 **학습 및 모델 저장 단계에만 관여**, 서빙 시점에는 Spark 불필요
  - `mlflow.spark.log_model()` → Registry에 등록 → `mlflow models serve`로 REST API 기동
  - 요청은 단일 Python 프로세스에서 처리되며, SparkContext 초기화 오버헤드 없음
![mllib용 배포 옵션](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FbreuN2%2FbtsydUersRg%2FAAAAAAAAAAAAAAAAAAAAALNBe8UxcHzuK18WR11V4B_OtKbzh8wvXAnnQzRcdpbR%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1777561199%26allow_ip%3D%26allow_referer%3D%26signature%3DrLFkl9HW6%252BYE0TQDnOPJbSAKtu0%253D)

```
배포 방식별 Spark 관여 범위
------------------------------------------------------------------
단계            배치 추론              스트리밍 추론       실시간 REST API
----------    -----------          -------------   ---------------
데이터 수집       Spark                Spark            외부 API 직접
모델 추론        Spark                Spark            Python 프로세스
결과 저장        Spark                Spark            응답 반환
Spark 상시      배치 실행 시에만 실행     항상 실행          불필요
------------------------------------------------------------------
```

### 2-1. 배치 추론

배치 추론은 대규모 데이터셋에 대해 정기적으로 예측값을 일괄 생성하는 방식.
가장 단순하고 안정적인 배포 방식으로, 지연시간 요구가 없는 케이스에 적합.
일단 작업을 실행하여 예측 생성하고 결과를 다운스트림에서 소비할 수 있도록 테이블, 데이터베이스, 데이터 레이크 등에 저장.

- **목적**
  - 매일/매시간 등 주기적으로 대량 데이터에 대한 예측 결과를 미리 계산하여 저장
  - 조회 시점에 실시간 추론 없이 미리 계산된 결과를 즉시 반환
- **이유**
  - 수백만 건의 추천, 이탈 예측 등은 요청 시점에 계산하면 지연시간이 허용 수준 초과
  - Spark 클러스터의 병렬 처리로 대용량 스코어링이 빠르고 비용 효율적
- **결과**
  - 예측 결과가 Delta Lake, S3, DB 등 스토리지에 저장됨
  - 서빙 레이어는 미리 계산된 결과를 단순 조회만 수행
- **장점**
  - 구현이 단순하고 안정적 (Spark 배치 파이프라인과 동일한 구조)
  - 대용량 처리에 최적화, 클러스터 자원을 풀로 활용 가능
  - 모델 변경 시 재실행으로 전체 결과를 일괄 갱신 가능
- **단점**
  - 예측 결과가 최신이 아님 (마지막 배치 실행 시점 기준)
  - 실시간 이벤트 기반 의사결정에는 부적합

**배치 배포 시 염두에 두어야 할 사항**

- 얼마나 자주 예측을 생성할 것인가?
    - 많은 예측을 일괄 처리하면 더 높은 처리량을 얻을 수 있지만 개별 예측을 수신하는 데 걸리는 시간 길어지고, 예측에 대해 조치를 취하는 능력이 지연됨
- 얼마나 자주 모델을 재교육할 예정인가?
    - MLlib은 온라인 업데이트나 웜 스타트를 지원하지 않음
    - 최신 데이터를 통합하도록 모델을 다시 훈련시키려면 기존 매개변수 활용하는 대신 전체 모델 처음부터 다시 훈련 필요
- 모델을 어떻게 버전화할 것인가?
    - MLflow 모델 레지스트리를 사용하여 사용중인 모델 트래킹하고 스테이징, 프로덕션 및 아카이브 간에 전환되는 방식 제어 가능

```python
from pyspark.ml import PipelineModel

# 운영 모델 로드
model = PipelineModel.load("/models/airbnb-price-rf/production")

# 스코어링 대상 데이터 로드
scoring_df = spark.read.format("delta").load("/data/listings/new/")

# 배치 추론 실행
predictions = model.transform(scoring_df)

# 결과 저장 (Delta)
(
    predictions
    .select("listing_id", "prediction", "features")
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", "score_date = '2024-01-15'")  # 해당 날짜 파티션만 교체
    .save("/data/predictions/airbnb_price/")
)
```

> **트러블 로그** — 배치 추론 결과를 `overwrite`로 통째로 덮어쓰면 이전 결과가 모두 사라짐.
> 예: 파이프라인 오류로 일부 데이터만 스코어링됐는데 overwrite가 실행되면
> 정상 데이터도 함께 삭제되어 서비스 장애로 이어질 수 있음.
> Delta의 `replaceWhere`로 특정 파티션만 교체하거나, `append` 후 서빙 레이어에서 최신 버전만 조회하도록 설계할 것.

### 2-2. 스트리밍 추론

Structured Streaming 파이프라인에 MLlib 모델을 연결하여 실시간 이벤트를 처리하는 방식.
Structured Streaming은 데이터 처리 후 예측 생성하기 위해 시간별 작업 기다리는 대신 들어오는 데이터에 대한 추론 지속적으로 수행 가능.
배치와 동일한 MLlib API를 사용하므로 학습-서빙 코드의 일관성이 높음.
배치 솔루션보다 유지 관리 및 모니터링 더 복잡하지만 대기시간 짧음.

- **목적**
  - Kafka, Kinesis 등 스트리밍 소스에서 유입되는 이벤트를 실시간으로 스코어링
  - 이상 탐지, 실시간 추천, 사기 탐지 등 지연 없는 예측이 필요한 케이스 대응
- **이유**
  - 배치로 처리하면 이벤트 발생 후 수십 분 ~ 수 시간 뒤에야 결과를 얻음
  - Structured Streaming은 MLlib의 `transform()`을 스트리밍 DataFrame에도 그대로 적용 가능
- **결과**
  - 마이크로배치 단위로 수신된 이벤트에 모델 예측값을 실시간으로 추가
  - 결과를 Kafka, Delta, 외부 DB 등으로 즉시 전달 가능
- **장점**
  - 예측을 더 자주 생성하여 더 빠르게 조치 가능
  - 배치 추론 코드와 거의 동일한 구조로 스트리밍 추론 구현 가능
  - Delta Lake sink와 결합하면 배치/스트리밍 결과를 단일 테이블에서 통합 관리
- **단점**
  - 컴퓨팅 시간에 대해 지속적으로 비용을 지불해야 해서 배치 솔루션보다 비용이 많이 발생
  - 체크포인트 관리, 워터마크 설정 등 스트리밍 운영 복잡성이 추가됨
  - 마이크로배치 지연(수 초)이 있어 진짜 실시간(< 100ms)에는 부적합

```python
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 운영 모델 로드
model = PipelineModel.load("/models/airbnb-price-rf/production")

# 스키마 정의
schema = StructType([
    StructField("listing_id", StringType()),
    StructField("room_type", StringType()),
    StructField("accommodates", DoubleType()),
    StructField("bedrooms", DoubleType()),
    StructField("bathrooms", DoubleType()),
])

# Kafka 스트림 수신
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "listing-events")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# 스트리밍 DataFrame에 모델 적용 (배치와 동일한 API)
scored_stream = model.transform(stream_df)

# 결과를 Delta로 스트리밍 저장
(
    scored_stream
    .select("listing_id", "prediction")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/scoring/")
    .start("/data/predictions/streaming/")
)
```

> **트러블 로그** — 스트리밍 추론에서 모델을 교체할 때 체크포인트를 그대로 재사용하면 안 되는 경우가 있음.
> 예: 피처 컬럼이 추가된 새 모델로 교체했는데 기존 체크포인트를 이어 쓰면
> 스키마 불일치로 스트리밍 잡이 시작하자마자 `AnalysisException`으로 중단됨.
> 모델 스키마 변경 시 체크포인트 경로를 새로 지정하고, 이전 버전으로 롤백 경로도 미리 준비해 둘 것.

### 2-3. 실시간 추론을 위한 모델 내보내기 패턴

적은 수의 레코드로 예측을 수행하면 실시간 추론에 필요한 짧은 대기 시간을 달성할 수 있지만, 지연 시간 중요한 작업에서 로드 밸런싱 (많은 요청 동시 처리)과 지리적 위치 고려 필요.
단건 요청에 100ms 이하의 응답이 필요한 경우, MLlib 모델을 Spark 없이 동작하는 경량 포맷으로 내보내야 함.
대표적인 방법으로 MLeap과 ONNX를 통한 모델 직렬화가 있음.

- **목적**
  - Spark 클러스터 없이 REST API 서버에서 단건 추론을 저지연으로 수행
  - MLlib 파이프라인을 표준 포맷으로 변환해 다양한 서빙 환경에 배포
- **이유**
  - Spark는 대규모 분산 처리에 최적화되어 있어, 단건 요청에는 SparkContext 초기화 오버헤드가 너무 큼
  - 실시간 API 서버는 JVM 위에서 경량으로 동작하는 런타임이 필요
- **결과**
  - MLeap: MLlib 파이프라인을 직렬화하여 Spark 없이 JVM 환경에서 추론 가능
  - ONNX: scikit-learn, XGBoost 등을 표준 포맷으로 변환, 다양한 런타임 지원
  - MLflow `pyfunc`: 프레임워크 무관한 Python 추론 래퍼
- **장점**
  - Spark 클러스터 없이 경량 서버(Flask, FastAPI)에서 실시간 추론 가능
  - 모델 포맷 표준화로 다양한 플랫폼에 배포 가능
- **단점**
  - 변환 시 일부 MLlib 변환기가 지원되지 않을 수 있음
  - 원본 MLlib 모델과 내보낸 모델의 예측값 동일성 검증이 반드시 필요

```python
# MLflow pyfunc 형식으로 모델 저장 (REST API 서빙에 바로 사용 가능)
import mlflow.spark

with mlflow.start_run():
    mlflow.spark.log_model(
        pipeline_model,
        artifact_path="model",
        registered_model_name="airbnb-price-api",
    )

# MLflow 모델 서버로 REST API 실행 (CLI)
# mlflow models serve -m "models:/airbnb-price-api/Production" -p 5001

# 서빙된 모델에 단건 추론 요청
import requests, json

payload = {
    "dataframe_records": [{
        "room_type": "Entire home/apt",
        "accommodates": 4,
        "bedrooms": 2,
        "bathrooms": 1.0,
    }]
}
response = requests.post(
    "http://localhost:5001/invocations",
    headers={"Content-Type": "application/json"},
    data=json.dumps(payload),
)
print(response.json())  # {"predictions": [185.3]}
```

```
배포 방식별 지연시간 및 처리량 비교
--------------------------------------------------------------
방식                 지연시간      처리량          Spark 필요
---------------   ----------   -----------   ----------
배치 추론             분 단위       수억 건/회         필요
스트리밍 추론          초 단위       수만 건/초         필요
REST API (pyfunc)  밀리초 단위     수백 건/초        불필요
--------------------------------------------------------------
```

### 2-4. 스트리밍 추론 vs 실시간 추론

겉보기엔 둘 다 "들어오는 데이터를 지속적으로 추론"하는 것처럼 보이지만, 처리 단위와 요청 주체가 근본적으로 다름.

**처리 단위의 차이**

스트리밍 추론은 마이크로배치 방식으로 동작함.
이벤트를 1건씩 즉시 처리하는 것이 아니라, 수 초 간격으로 그 사이에 쌓인 이벤트를 묶어서 일괄 처리함.
Structured Streaming이 내부적으로 "일정 시간치 데이터를 수집 → 미니 배치로 실행"을 반복하는 구조이므로, 지연시간이 수 초~수십 초 단위로 발생함.

실시간 추론(REST API)은 단건 요청이 들어오는 즉시 처리함.
클라이언트가 HTTP 요청을 보내면 그 요청 하나에 대해 바로 예측값을 반환하며, 지연시간이 수 밀리초 단위임.

**요청 주체의 차이**

스트리밍 추론은 Spark 잡이 Kafka 같은 소스를 능동적으로 폴링해서 데이터를 가져오는 구조임.
클라이언트가 요청을 보내는 것이 아니라, Spark가 소스를 감시하다가 데이터가 쌓이면 스스로 처리함.

실시간 REST API는 클라이언트가 요청을 보내고 서버가 즉시 응답하는 전형적인 request-response 패턴임.
예측이 필요한 시점에 호출자가 직접 트리거하는 구조임.

```
스트리밍 추론 vs 실시간 추론 처리 흐름
--------------------------------------------------------------
[스트리밍 추론]

Kafka → Spark Structured Streaming
              │
              │ (수 초마다 마이크로배치 실행)
              ▼
         이벤트 N건 묶음 → model.transform(miniBatch)
              │
              ▼
         Delta / Kafka sink에 결과 저장
         (지연: 수 초 ~ 수십 초)

[실시간 REST API]

클라이언트 ──POST /invocations──► mlflow 서버 (Python 프로세스)
                                        │
                                        │ 단건 즉시 처리
                                        ▼
                                   model.predict(1건)
                                        │
                                        ▼
              클라이언트 ◄── 응답 반환 ── {"predictions": [185.3]}
              (지연: 수 밀리초)
--------------------------------------------------------------
```

| 비교 항목 | 스트리밍 추론 | 실시간 REST API |
|---------|------------|---------------|
| 처리 단위 | 마이크로배치 (N건 묶음) | 단건 |
| 요청 주체 | Spark가 소스를 폴링 | 클라이언트가 직접 호출 |
| 지연시간 | 수 초 ~ 수십 초 | 수 밀리초 |
| Spark 필요 | 항상 실행 중 | 서빙 시 불필요 |
| 적합한 케이스 | 이벤트 스트림 지속 처리 | 단건 요청, 응답 지연 엄격 |
| 예시 | 실시간 이상 탐지, 동적 가격 책정 | 온라인 광고 입찰, 챗봇 응답 |

온라인 광고 입찰이 REST API여야 하는 이유가 바로 여기 있음.
사용자가 페이지를 열었을 때 100ms 안에 입찰가를 결정해야 하는데,
스트리밍은 다음 마이크로배치 실행까지 기다려야 하므로 지연시간 요건을 충족할 수 없음.

---

## 3. 비MLlib 모델에 스파크 활용

MLlib 외의 모델(scikit-learn, XGBoost, PyTorch 등)도 Spark를 활용해 분산 처리 가능.
모델 학습이나 추론 자체는 각 라이브러리가 담당하고, Spark는 데이터 분산과 병렬 실행을 담당.

### 3-1. 판다스 UDF

판다스 UDF(Pandas UDF)는 Python 함수를 Spark DataFrame의 각 파티션에 분산 적용하는 방법.
scikit-learn, XGBoost 등 단일 머신 라이브러리를 Spark 클러스터에서 병렬 실행 가능하게 함.

- **목적**
  - 단일 머신 ML 라이브러리(scikit-learn, XGBoost)로 학습된 모델을 Spark로 분산 배치 추론
  - 각 워커 노드에서 Pandas DataFrame 단위로 모델 추론을 병렬 실행
- **이유**
  - Python ML 생태계는 Spark MLlib보다 훨씬 풍부 (XGBoost, LightGBM, 딥러닝 등)
  - 이 모델들을 Spark 없이 쓰면 단일 머신 메모리 한계에 제약됨
- **결과**
  - Spark가 DataFrame을 파티션으로 나눠 각 워커에 분배
  - 각 워커에서 Pandas UDF 함수가 파티션 단위 Pandas DataFrame으로 모델 추론 실행
  - 결과를 다시 Spark DataFrame으로 합산
- **장점**
  - 기존 scikit-learn/XGBoost 코드를 거의 수정 없이 분산 추론에 활용
  - 각 워커에서 독립적으로 실행되어 수평 확장 가능
- **단점**
  - 모델 파일을 모든 워커에 배포해야 함 (브로드캐스트 또는 파일 공유 필요)
  - 워커마다 동일한 Python 환경과 라이브러리 버전이 맞아야 함

```python
import mlflow.pyfunc
import pandas as pd
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType

# 학습된 모델 브로드캐스트 (워커 노드에 효율적으로 배포)
model_uri = "models:/airbnb-price-rf/Production"
loaded_model = mlflow.pyfunc.load_model(model_uri)
broadcast_model = spark.sparkContext.broadcast(loaded_model)

# Pandas UDF 정의: Pandas DataFrame → Pandas Series 반환
@pandas_udf(DoubleType())
def predict_udf(*features: pd.Series) -> pd.Series:
    # 각 파티션에서 모델 로드 (브로드캐스트 활용)
    model = broadcast_model.value
    # 피처 DataFrame 구성
    pdf = pd.concat(features, axis=1)
    pdf.columns = ["room_type", "accommodates", "bedrooms", "bathrooms"]
    return pd.Series(model.predict(pdf))

# Spark DataFrame에 UDF 적용
predictions = scoring_df.withColumn(
    "prediction",
    predict_udf(
        col("room_type"),
        col("accommodates"),
        col("bedrooms"),
        col("bathrooms"),
    )
)
```

#### mapInPandas()를 활용한 방식

`pandas_udf`와 달리 파티션 전체를 `pd.DataFrame`으로 받아 처리하는 방식.
모델이 배치 단위 입력을 필요로 하거나, 반환 컬럼이 여러 개일 때 적합함.

```python
import mlflow.pyfunc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

model_uri = "models:/airbnb-price-rf/Production"
broadcast_model = spark.sparkContext.broadcast(
    mlflow.pyfunc.load_model(model_uri)
)

features = ["room_type", "accommodates", "bedrooms", "bathrooms"]

def predict_batch(iterator):
    model = broadcast_model.value  # 파티션당 1회만 로드
    for pdf in iterator:           # pdf: 파티션 하나를 pd.DataFrame으로 수신
        pdf["prediction"] = model.predict(pdf[features])
        yield pdf                  # 컬럼이 추가된 DataFrame 반환

result_schema = StructType(scoring_df.schema.fields + [
    StructField("prediction", DoubleType())
])

predictions = scoring_df.mapInPandas(predict_batch, schema=result_schema)
```

`pandas_udf`와의 차이는 다음과 같음.

| 항목 | `pandas_udf` | `mapInPandas` |
|------|-------------|---------------|
| 입력 단위 | 컬럼별 `pd.Series` | 파티션 전체 `pd.DataFrame` |
| 반환 단위 | `pd.Series` 하나 | `pd.DataFrame` (컬럼 수 자유) |
| 반환 타입 선언 | 데코레이터에 지정 | `schema` 파라미터로 지정 |
| 배치 크기 제어 | Spark 내부 결정 | 파티션 단위로 직접 제어 |
| 적합한 케이스 | 단일 컬럼 추가 | 배치 추론, 다중 컬럼 반환 |

> **트러블 로그** — 모델을 브로드캐스트하지 않으면 각 파티션 처리마다 원격 스토리지에서 모델을 다운로드.
> 예: 파티션이 200개인 DataFrame에서 브로드캐스트 없이 S3에서 모델(500MB)을 로드하면
> 총 100GB의 네트워크 트래픽이 발생하고 추론 시간이 수십 배 증가함.
> `spark.sparkContext.broadcast()`로 모델을 한 번만 워커에 배포하고 재사용할 것.

### 3-2. 분산 하이퍼파라미터 조정을 위한 스파크

단일 머신에서 하이퍼파라미터를 순차 탐색하면 조합 수에 비례해 시간이 선형으로 증가함.
Spark를 활용하면 탐색 공간을 워커들이 병렬로 분담해 탐색 시간을 대폭 단축할 수 있음.
방식은 joblib 백엔드를 활용한 방식과 Hyperopt + SparkTrials 방식 두 가지가 있음.

#### joblib을 활용한 방식

scikit-learn은 내부적으로 병렬 처리를 `joblib` 라이브러리에 위임함.
`joblibspark`를 사용하면 joblib의 백엔드를 Spark로 교체하여, scikit-learn의 `n_jobs=-1` 옵션이 단일 머신 멀티코어가 아닌 Spark 클러스터 전체에서 동작하게 됨.
코드 변경이 거의 없이 기존 scikit-learn GridSearchCV를 분산 실행할 수 있다는 것이 가장 큰 장점.

![분산 하이퍼파라미터 검색](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FmxlHr%2Fbtsx1RKCBNZ%2FAAAAAAAAAAAAAAAAAAAAAFSrqrilX_uxvqCWAhnF_UJuuxQCROfZGlESzHBnlhfL%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1777561199%26allow_ip%3D%26allow_referer%3D%26signature%3DKhcatB%252BnukUSSCKV42UG6VdrLmU%253D)

- **목적**
  - 기존 scikit-learn 코드를 최소 수정으로 Spark 클러스터에서 병렬 튜닝
  - `GridSearchCV`, `cross_val_score` 등 joblib을 내부적으로 쓰는 모든 scikit-learn API에 적용 가능
- **이유**
  - scikit-learn의 `n_jobs=-1`은 단일 머신 CPU 코어 수로 병렬화가 제한됨
  - joblib 백엔드를 Spark로 바꾸면 클러스터 전체 코어를 활용 가능
- **결과**
  - `register_spark()`로 백엔드 등록 후, `parallel_backend("spark")` 컨텍스트 안에서 실행하면 Spark 워커에서 병렬 처리됨
- **장점**
  - 기존 scikit-learn 코드 구조를 그대로 유지 가능 (`GridSearchCV` 코드 변경 없음)
  - Spark 클러스터 설정만으로 병렬화 가능, 별도 탐색 알고리즘 학습 불필요
- **단점**
  - 격자 탐색(Grid Search) 방식이라 탐색 공간이 크면 비효율적
  - Bayesian 최적화처럼 이전 결과를 반영한 스마트한 탐색은 불가능

```python
from sklearn.utils import parallel_backend
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
from joblibspark import register_spark

register_spark()  # joblib 백엔드를 Spark로 교체

param_grid = {
    "n_estimators": [50, 100, 200],
    "max_depth": [3, 5, 10],
    "min_samples_leaf": [1, 5, 10],
}

rf = RandomForestRegressor(random_state=42)
grid_search = GridSearchCV(rf, param_grid, cv=3, scoring="neg_root_mean_squared_error", n_jobs=-1)

# parallel_backend("spark") 컨텍스트 안에서 실행 → Spark 클러스터에서 병렬 수행
with parallel_backend("spark", n_jobs=-1):
    grid_search.fit(train_pdf[features], train_pdf["price"])

print(f"최적 파라미터: {grid_search.best_params_}")
print(f"최적 RMSE: {-grid_search.best_score_:.2f}")
```

#### Hyperopt + SparkTrials를 활용한 방식

Hyperopt + SparkTrials를 사용하면 하이퍼파라미터 탐색을 Spark 클러스터에서 병렬로 실행 가능.
scikit-learn, XGBoost 등 단일 머신 모델의 튜닝도 Spark로 가속화됨.
joblib 방식과 달리 Bayesian 최적화로 탐색 공간을 지능적으로 좁혀나감.

- **목적**
  - 하이퍼파라미터 탐색 공간을 Spark 워커들이 병렬로 분담하여 튜닝 시간 단축
  - Bayesian 최적화(Tree of Parzen Estimators)로 격자 탐색보다 효율적인 탐색 수행
- **이유**
  - 단일 머신에서 100개 조합을 순차 탐색하면 각 조합이 2분이면 총 200분 소요
  - Spark 10개 워커 병렬 실행 시 약 20분으로 단축 가능
  - Bayesian 최적화는 이전 시도 결과를 반영하여 유망한 영역을 더 탐색
- **결과**
  - 각 워커에서 독립적인 학습 실험이 병렬 실행됨
  - MLflow와 연동하면 각 시도 결과가 자동으로 기록됨
- **장점**
  - 격자 탐색 대비 같은 시간에 더 좋은 하이퍼파라미터를 발견할 가능성 높음
  - MLflow와 통합으로 탐색 이력 자동 기록
  - scikit-learn, XGBoost, LightGBM 등 단일 머신 모델 모두 적용 가능
- **단점**
  - Hyperopt 의존성과 SparkTrials 설정이 추가됨
  - Bayesian 최적화는 완전 병렬화가 어려움 (이전 결과 반영 대기 필요)

```python
from hyperopt import fmin, tpe, hp, SparkTrials, STATUS_OK
import mlflow
import xgboost as xgb
from sklearn.metrics import mean_squared_error
import numpy as np

# 탐색 공간 정의
search_space = {
    "max_depth": hp.quniform("max_depth", 3, 10, 1),
    "learning_rate": hp.loguniform("learning_rate", np.log(0.01), np.log(0.3)),
    "n_estimators": hp.quniform("n_estimators", 50, 300, 50),
    "subsample": hp.uniform("subsample", 0.6, 1.0),
}

# 목적 함수 정의 (각 워커에서 실행됨)
def objective(params):
    with mlflow.start_run(nested=True):
        model = xgb.XGBRegressor(
            max_depth=int(params["max_depth"]),
            learning_rate=params["learning_rate"],
            n_estimators=int(params["n_estimators"]),
            subsample=params["subsample"],
        )
        # train_pdf, val_pdf는 Pandas DataFrame (Spark에서 .toPandas() 변환)
        model.fit(train_pdf[features], train_pdf["price"])
        preds = model.predict(val_pdf[features])
        rmse = mean_squared_error(val_pdf["price"], preds, squared=False)

        mlflow.log_params(params)
        mlflow.log_metric("rmse", rmse)

        return {"loss": rmse, "status": STATUS_OK}

# Spark 병렬 탐색 실행
with mlflow.start_run(run_name="hyperopt-xgb"):
    best_params = fmin(
        fn=objective,
        space=search_space,
        algo=tpe.suggest,            # Bayesian 최적화
        max_evals=50,                # 총 탐색 횟수
        trials=SparkTrials(parallelism=8),  # 8개 워커 병렬 탐색
    )

print(f"최적 파라미터: {best_params}")
```

```
Hyperopt + SparkTrials 동작 방식
--------------------------------------------------------------
[드라이버]
  TPE 알고리즘이 다음 탐색할 파라미터 조합 생성
        │
        ▼
[워커 1~8] 병렬 학습
  각 워커: XGBoost 학습 + RMSE 계산 + MLflow 기록
        │
        ▼
[드라이버]
  결과 수집 → TPE 업데이트 → 다음 조합 생성 (반복)
        │
        ▼
  max_evals 도달 시 최적 파라미터 반환
--------------------------------------------------------------
```

> **트러블 로그** — `parallelism`을 클러스터 워커 수보다 크게 설정하면 오히려 성능이 저하됨.
> 예: 워커 4개인 클러스터에 `parallelism=20`으로 설정하면 20개 시도가 큐에 쌓이고
> 실제로 동시에 돌아가는 것은 4개뿐이라 스케줄링 오버헤드만 증가함.
> `parallelism`은 실제 사용 가능한 워커 수와 일치시키고, 탐색 횟수(`max_evals`)는 `parallelism`의 5~10배가 적절.

---

## 4. 요약

```
ML 파이프라인 관리 및 배포 핵심 요약
==========================================================================

컴포넌트                       역할
------------------------    -----------------------------------------------
MLflow Tracking             파라미터, 지표, 모델 아티팩트 자동 기록
MLflow Model Registry       모델 버전 관리 및 Staging → Production 프로세스
Batch Inference             대용량 주기적 스코어링, 가장 단순하고 안정적
Streaming Inference         실시간 이벤트 스코어링, Structured Streaming 활용
REST API (pyfunc)           저지연 단건 추론, Spark 없이 경량 서버 운영
Pandas UDF                  단일 머신 모델을 Spark로 분산 배치 추론
Hyperopt + SparkTrials      하이퍼파라미터 탐색을 Spark 클러스터에서 병렬화

==========================================================================

배포 방식 선택 가이드
--------------------------------------------------------------
요구사항                        추천 방식
---------------------------  ------------------------------------
수억 건 스코어링, 일 1회           배치 추론 (PipelineModel.transform)
이벤트 스트림 실시간 처리           스트리밍 추론 (Structured Streaming)
단건 요청 100ms 이하             모델 내보내기 → REST API
scikit-learn/XGBoost 분산      Pandas UDF + broadcast 모델
하이퍼파라미터 탐색 가속            Hyperopt + SparkTrials
--------------------------------------------------------------

운영 체크리스트
--------------------------------------------------------------
□ MLflow로 모든 실험 파라미터/지표 기록
□ Model Registry에서 Staging → Production 명시적 승인
□ 배포 방식을 지연시간/볼륨 요구사항에 맞게 선택
□ 배치 추론: replaceWhere로 파티션 단위 안전 교체
□ 스트리밍 추론: 모델 교체 시 체크포인트 경로 신규 설정
□ Pandas UDF: 모델 브로드캐스트로 네트워크 트래픽 최소화
□ Hyperopt: parallelism = 실제 워커 수, max_evals = parallelism × 5~10
--------------------------------------------------------------
```
