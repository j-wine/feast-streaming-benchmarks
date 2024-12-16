FROM flink:1.2-alpine

# Install SSH and other necessary packages
RUN apk update && apk add --no-cache openssh

COPY test_file.txt /app/
COPY flink_job.py /opt/flink/usrlib/
CMD ["/bin/sh", "-c", "/opt/flink/bin/start-cluster.sh && /opt/flink/bin/flink run /opt/flink/usrlib/flink_job.py"]
