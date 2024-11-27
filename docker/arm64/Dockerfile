FROM amazonlinux:2023

ENV TMP_DIR="/tron-build"
ENV JDK_TAR="amazon-corretto-17-aarch64-linux-jdk.tar.gz"
ENV JDK_DIR="amazon-corretto-17.0.13.11.1-linux-aarch64"
ENV JDK_MD5="108a7c28b4ca3fd6bdd5f514114cf798"
ENV BASE_DIR="/java-tron"


RUN set -o errexit -o nounset \
    && yum -y install git wget tar p7zip \
    && wget -P /usr/local https://corretto.aws/downloads/latest/$JDK_TAR \
    && echo "$JDK_MD5 /usr/local/$JDK_TAR" | md5sum -c \
    && tar -zxf /usr/local/$JDK_TAR -C /usr/local\
    && rm /usr/local/$JDK_TAR \
    && export JAVA_HOME=/usr/local/$JDK_DIR \
    && export PATH=$PATH:$JAVA_HOME/bin \
    && echo "git clone" \
    && mkdir -p $TMP_DIR \
    && cd $TMP_DIR \
    && git clone https://github.com/tronprotocol/java-tron.git \
    && cd java-tron \
    && git checkout master \
    && ./gradlew build -x test \
    && cd build/distributions \
    && 7za x -y java-tron-1.0.0.zip  \
    && mv java-tron-1.0.0 $BASE_DIR \
    && rm -rf $TMP_DIR \
    && rm -rf ~/.gradle \
    && yum clean all

RUN wget -P $BASE_DIR/config https://raw.githubusercontent.com/tronprotocol/tron-deployment/master/main_net_config.conf

ENV JAVA_HOME=/usr/local/$JDK_DIR
ENV PATH=$PATH:$JAVA_HOME/bin

COPY docker-entrypoint.sh $BASE_DIR/bin

WORKDIR $BASE_DIR

ENTRYPOINT ["./bin/docker-entrypoint.sh"]