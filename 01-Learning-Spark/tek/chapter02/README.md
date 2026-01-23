`mmcount.py` 실행 방법
- 현재 poetry 가상환경 사용
```python
poetry run python mmcount.py data/mnm_dataset.csv 

poetry run spark-submit mmcount.py data/mnm_dataset.csv    
```