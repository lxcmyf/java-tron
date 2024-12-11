ARG ARCH=x86

FROM tronprotocol/centos7:0.2 AS x86
FROM amazoncorretto:17.0.13 AS arm64

FROM ${ARCH} AS builder

ENV TMP_DIR="/tron-build"
ENV BASE_DIR="/java-tron"
ENV UNZIP_DISABLE_ZIPBOMB_DETECTION=TRUE

RUN if [ "$ARCH" = "x86" ]; then \
    yum -y install git wget \
    && wget -P /usr/local https://github.com/frekele/oracle-java/releases/download/8u202-b08/jdk-8u202-linux-x64.tar.gz \
    && echo "0029351f7a946f6c05b582100c7d45b7 /usr/local/jdk-8u202-linux-x64.tar.gz" | md5sum -c \
    && tar -zxf /usr/local/jdk-8u202-linux-x64.tar.gz -C /usr/local \
    && rm /usr/local/jdk-8u202-linux-x64.tar.gz \
    && export JAVA_HOME=/usr/local/jdk1.8.0_202 \
    && export CLASSPATH=$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar \
    && export PATH=$PATH:$JAVA_HOME/bin \
    && mkdir -p $TMP_DIR \
    && cd $TMP_DIR \
    && git clone https://github.com/tronprotocol/java-tron.git \
    && cd java-tron \
    && git checkout master \
    && ./gradlew build -x test \
    && cd build/distributions \
    && 7za x -y java-tron-1.0.0.zip \
    && mv java-tron-1.0.0 $BASE_DIR \
    && rm -rf $TMP_DIR \
    && rm -rf ~/.gradle \
    && mv $JAVA_HOME/jre /usr/local \
    && rm -rf $JAVA_HOME \
    && yum clean all; \
fi

RUN if [ "$ARCH" = "arm64" ]; then \
    yum -y install 7zip git wget \
    && mkdir -p $TMP_DIR \
    && cd $TMP_DIR \
    && git clone https://github.com/lxcmyf/java-tron.git \
    && cd java-tron \
    && git checkout arm64 \
    && ./gradlew build -x test \
    && cd build/distributions \
    && 7za x -y java-tron-1.0.0.zip \
    && mv java-tron-1.0.0 $BASE_DIR \
    && rm -rf $TMP_DIR \
    && rm -rf ~/.gradle \
    && yum clean all; \
fi

RUN wget -P $BASE_DIR/config https://raw.githubusercontent.com/tronprotocol/tron-deployment/master/main_net_config.conf

ENV JAVA_HOME="/usr/local/jre"
ENV PATH=$PATH:$JAVA_HOME/bin

COPY docker-entrypoint.sh $BASE_DIR/bin

WORKDIR $BASE_DIR

ENTRYPOINT ["./bin/docker-entrypoint.sh"]
