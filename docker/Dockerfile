FROM amazoncorretto:17.0.13

ENV TMP_DIR="/tron-build"
ENV BASE_DIR="/java-tron"


RUN set -o errexit -o nounset \
    && yum -y install git wget p7zip \
    && echo "git clone" \
    && mkdir -p $TMP_DIR \
    && cd $TMP_DIR \
    && git clone https://github.com/lxcmyf/java-tron.git \
    && cd java-tron \
    && git checkout arm64 \
    && ./gradlew build -x test \
    && cd build/distributions \
    && 7za x -y java-tron-1.0.0.zip  \
    && mv java-tron-1.0.0 $BASE_DIR \
    && rm -rf $TMP_DIR \
    && rm -rf ~/.gradle \
    && yum clean all

RUN wget -P $BASE_DIR/config https://raw.githubusercontent.com/tronprotocol/tron-deployment/master/main_net_config.conf

COPY docker-entrypoint.sh $BASE_DIR/bin

WORKDIR $BASE_DIR

ENTRYPOINT ["./bin/docker-entrypoint.sh"]