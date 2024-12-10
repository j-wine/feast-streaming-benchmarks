FROM flink:1.2-alpine

COPY flink_job.py /opt/flink/usrlib/
CMD ["start-cluster.sh", "&&", "/opt/flink/bin/flink", "run", "/opt/flink/usrlib/flink_job.py"]
