Dependency notes:
- flink dependency only supports python 3.11
- dill causes serializing issues with feature_store.yaml
- pyarrow needs build tools
- kafka_producer needs six dependency because kafka-python relies on it  

TO DO:

- create google github runners for test + prod enviroments