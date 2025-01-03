FROM apache/spark:latest
EXPOSE 10000
ENTRYPOINT ["/opt/spark/sbin/start-thriftserver.sh", "/opt/spark/bin/spark-shell"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]