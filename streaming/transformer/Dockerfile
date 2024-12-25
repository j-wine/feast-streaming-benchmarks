FROM flink:1.20

RUN apt-get update && apt-get install -y openssh-client python3 python3-pip  \
    && pip3 install apache-flink py4j

COPY conf/flink-conf.yaml /opt/flink/conf/
COPY flink_job.py /opt/flink/usrlib/
# sleep 10  to allow the Flink cluster to start fully before submitting the job
CMD ["/bin/sh", "-c", "/opt/flink/bin/start-cluster.sh  && sleep 10 && /opt/flink/bin/flink run /opt/flink/usrlib/flink_job.py"]
