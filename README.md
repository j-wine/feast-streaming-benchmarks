Dependency notes:
- feast 0.48 causes conflicting dependency versions with pyspark 3.5.0
- pyspark requires 3.5.0 to transform timestamp to pandas https://docs.tecton.ai/docs/tips-and-tricks/troubleshooting/conversion-from-pyspark-dataframe-to-pandas-dataframe-with-pandas-2-0
- dill causes serializing issues with feature_store.yaml
- pyarrow needs build tools
- kafka_producer needs six dependency because kafka-python relies on it
