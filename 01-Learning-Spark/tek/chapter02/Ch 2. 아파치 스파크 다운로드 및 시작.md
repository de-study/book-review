- 스파크 셸에서의 모든 처리가 하나의 머신에서 이루어지는 로컬 모드 사용
	- 프레임워크 익히면서 반복적인 스파크 수행 가능 → 빠른 피드백
	- 분산 처리의 이득을 보기 원하는 큰 데이터나 실무 작업에는 로컬 모드 적합하지 않다.
		- YARN, 쿠버네티스 배포 모드 사용해야 한다.

## 로컬 머신 사용하기
- 스파크 연산들은 작업으로 표현된다.
	- 작업들은 태스크라고 불리는 저수준 RDD 바이트 코드로 변환 → 실행을 위해 스파크의 이그제큐터들에 분산된다.

## 3단계: 스파크 애플리케이션 개념의 이해
**애플리케이션**
- API를 써서 스카프 위에서 돌아가는 사용자 프로그램
- 드라이버 프로그램과 클러스터의 실행기로 이루어진다.

**SparkSession**
- 스파크 코어 기능들과 상호 이용할 수 있는 진입점을 제공하며, 그 API로 프로그래밍 할 수 있게 해주는 객체
	- 스파크 셸에서 스파크 드라이버는 기본적으로 `SparkSession` 제공
	- 스파크 애플리케이션에서는 사용자가 `SparkSession` 객체 생성해야 한다.

**잡 (job)**
- 스파크 액션 (e.g. `save()`, `collect()`)에 대한 응답으로 생성되는 여러 태스크로 이루어진 병렬 연산

**스테이지 (stage)**
- 각 잡은 스테이지라 불리는 서로 의존성 갖는 다수의 태스크 모음으로 나뉜다

**태스크 (task)**
- 스파크 이그제큐터로 보내지는 작업 실행의 가장 기본적인 단위

### 스파크 애플리케이션과 SparkSession
- 스파크 분산 아키텍처 위에서 스파크 드라이버와 통신하는 스파크 컴포넌트
	![스파크 컴포넌트](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FMKvcY%2FbtsJam0zm6O%2FAAAAAAAAAAAAAAAAAAAAAKiO7LB1nxpzCu3kPnmQO5_1gm8k70_rINajmQHu19sU%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1769871599%26allow_ip%3D%26allow_referer%3D%26signature%3DGAgbL1G3O1mC5PuSbd0u%252BbSomg0%253D)
- 모든 스파크 애플리케이션의 핵심에는 스파크 드라이버 프로그램이 있으며, 이 드라이버는 `SparkSession` 객체를 만든다
	- 스파크 셸을 써서 작업할 때 드라이버는 셸에 포함되어 있는 형태이며 `SparkSession` 객체가 미리 만들어 진다
- `SparkSession` 객체를 만들었으면, 이를 통해 스파크 연산을 수행하는 API를 써서 프로그래밍 가능
### 스파크 잡
- 스파크 셸로 상호 작용하는 작업 동안, 스파크 애플리케이션을 하나 이상의 스파크 잡으로 변환하고, 각각의 잡은 DAG로 변환된다.
	- 하나 이상의 스파크 잡을 생성하는 스파크 드라이버
		![스파크 잡](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2Fbig2X7%2FbtsJbhqGBSP%2FAAAAAAAAAAAAAAAAAAAAABFAxqFoj_WH_AKu-LM3XKes4D17uS1mHPQjjW39uLFa%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1769871599%26allow_ip%3D%26allow_referer%3D%26signature%3DQ3922kkJKE4JHZvtrshlAdtRzfo%253D)
- 본질적으로 스파크의 실행 계획이다.
- DAG 그래프에서 각각의 노드는 하나 이상의 스파크 스테이지에 해당한다.
### 스파크 스테이지
- 어떤 작업이 연속 혹은 병렬적으로 수행되는지에 맞춰 스테이지에 해당하는 DAG노드 생성된다.
- 모든 스파크 연산이 하나의 스테이지에서 실행될 수는 없다 → 여러 스테이지로 나눠야 한다.
	- 종종 스파크 이그제큐터끼리 데이터 전송이 이루어지는 연산 범위 경계 위에서 스테이지가 결저오디기도 한다.
		- Shuffle: 노드끼리의 데이터 교환이 스테이지의 경계가 되는 경우
- 하나 이상의 스테이지를 생성하는 스파크 잡
	![스파크 잡](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FcoDJ2q%2FbtsJaazh2Sq%2FAAAAAAAAAAAAAAAAAAAAADROaYmnZeyq-6DVnXeE_LL6NJoCeTDpEWC6wOGrLVw6%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1769871599%26allow_ip%3D%26allow_referer%3D%26signature%3DHK%252BWjWPCoWbTWpyScV0eEGHM1VA%253D)
### 스파크 태스크
- 각 스테이지는 최소 실행 단위이며, 스파크 이그제큐터들 위에서 연합 실행되는 스파크 태스크들로 이루어진다.
	- 이그제큐터들에서 분산처리되는 하나 이상의 태스크를 생성하는 스파크 스테이지
		![스파크 스테이지](https://i.imgur.com/Rnp2Uw4.png)

## 트랜스포메이션, 액션, 지연 평가 **(Transformations, Actions, and Lazy Evaluation)**
**분산 데이터의 스파크 연산은 트랜스포메이션과 액션으로 구분**
- **트랜스포메이션**
	- 불변성의 특징을 가진 원본 데이터를 수정하지 않고 하나의 스파크 데이터 프레임을 새로운 데이터 프레임으로 변형 (Transform)
		- `select()`, `filter()` 같은 연산은 원본 데이터프레임 수정하지 않는다.
		- 새로운 데이터프레임으로 연산 결과 만들어 되돌려 준다.
	- 모든 트랜스포메이션은 뒤늦게 평가된다.
		- 결과가 즉시 계산되는 것이 아니고, 계보 (Lineage)라 불리는 형태로 기록.
			- 기록된 리니지는 실행 계획 후반 쯤에 스파크가 확실한 트랜스포메이션끼리 재배열하거나 합치거나 해서 더 효율적으로 실행될 수 있게 최적화
- **지연 평가 (Laze Evaluation)**
	- 액션이 실행되는 시점이나 데이터에 실제 접근하는 시점 (디스크에서 읽거나 쓰는 시점)까지 실행을 미루는 스파크의 전략
- 하나의 액션은 모든 기록된 트랜스포메이션의 지연 연산 발동시킨다
	- 그림 2-6에서, 모든 트랜스포메이션 **T**는 액션 **A**를 호출할 때까지 기록된다.
		![지연평가](https://i.imgur.com/Q26MuQW.png)

**지연 평가는 스파크가 사용자의 연계된 트랜스포에이션들을 살펴봄으로써 쿼리 최적화를 가능하게 하는 반면, 리니지와 데이터 불변성은 장애에 대한 데이터 내구성 제공**
- 스파크는 리니지에 트랜스포메이션 기록해 놓고, 데이터프레임은 트랜스포메이션 거치는 동안 변하지 않는다.
	- 단순히 기록된 리니지 재실행하는 것만으로도 원래 상태를 다시 만들어낼 수 있다.
	- 따라서 장애 상황에도 유연성 확보 가능

**액션과 트랜스포메이션들은 스파크 쿼리 계획이 만들어지는 데 도움을 준다**
- 하나의 쿼리 계획 안의 어떤 것도 액션이 호출되기 전에는 실행되지 않는다.

### 좁은/넓은 트랜스포메이션
**트랜스포메이션은 스파크가 지연 평가하는 연산 종류**
**지연 연산 개념의 큰 이득은 스파크가 연산 쿼리를 분석하고 어디를 최적화할 지 알 수 있다는 점**

**트랜스포메이션은 좁은 의존성과 넓은 의존성으로 분류**
- 좁은 트랜스포메이션
	- 하나의 입력 파티션을 연산하여 하나의 결과 파티션 내놓는 트랜스포메이션
- 넓은 트랜스포메이션
	- 다른 파티션으로부터 데이터를 읽어 들여서 합치고 디스크에 쓰는 일을 한다.
		- e.g. `groupBy()`, `orderBy()`
![의존성](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdna%2FbHeZeX%2FbtsJb5wi2a1%2FAAAAAAAAAAAAAAAAAAAAAK-hWRZrrISKNfkAEeMDEBi618z5hdX50dDfz2Ckx7gH%2Fimg.png%3Fcredential%3DyqXZFxpELC7KVnFOS48ylbz2pIh7yKj8%26expires%3D1769871599%26allow_ip%3D%26allow_referer%3D%26signature%3DhTJys2G3XRlS55d38qUD4Q2WSYk%253D)

## 스파크 UI
**스파크는 GUI를 써서 스파크 애플리케이션을 살펴볼 수 있게 해주며, 다양한 레벨 (잡, 스테이지, 태스크 레벨)에서 확인 가능하다.**
- UI는 스파크 내부의 작업에 대한 디버깅과 검사 도구로서 현미경 역할을 한다.

## References
- https://na0-0.tistory.com/158
- https://anyflip.com/zakdl/bkqz/basic