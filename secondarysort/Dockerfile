FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

MAINTAINER Ivan Ermilov <ivan.s.ermilov@gmail.com>

COPY secondarysort.jar /opt/hadoop/applications/secondarysort.jar

ENV JAR_FILEPATH="/opt/hadoop/applications/secondarysort.jar"
ENV CLASS_TO_RUN="ldy.hello.SecondarySort"
ENV PARAMS="/datas/secondarysort-input /datas/secondarysort-output"

ADD run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
