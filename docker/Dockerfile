# 定义可变参数
ARG ARCH=x86

# 基于架构选择基础镜像
FROM amazoncorretto:17.0.13 AS base-arm64
FROM tronprotocol/centos7:0.2 AS base-x86

# 使用条件逻辑选择阶段
FROM base-${ARCH} AS builder

# 将构建时的 `ARCH` 参数传递到运行环境中
ARG ARCH
ENV ARCH=${ARCH}

# 通用环境变量
ENV TMP_DIR="/tron-build"
ENV BASE_DIR="/java-tron"
ENV UNZIP_DISABLE_ZIPBOMB_DETECTION=TRUE

# 针对架构的构建逻辑
RUN set -o errexit -o nounset \
    && yum -y install git wget unzip \
    # arm64 的构建逻辑
    && if [ "$ARCH" = "arm64" ]; then \
         echo "Building for arm64 architecture" && \
         mkdir -p $TMP_DIR && \
         cd $TMP_DIR && \
         git clone https://github.com/lxcmyf/java-tron.git && \
         cd java-tron && \
         git checkout arm64 && \
         ./gradlew build -x test && \
         cd build/distributions && \
         unzip -o java-tron-1.0.0.zip && \
         mv java-tron-1.0.0 $BASE_DIR; \
    fi \
    # x86 的构建逻辑
    && if [ "$ARCH" = "x86" ]; then \
         echo "Building for x86 architecture" && \
         wget -P /usr/local https://github.com/frekele/oracle-java/releases/download/8u202-b08/jdk-8u202-linux-x64.tar.gz && \
         echo "0029351f7a946f6c05b582100c7d45b7  /usr/local/jdk-8u202-linux-x64.tar.gz" | md5sum -c && \
         tar -zxf /usr/local/jdk-8u202-linux-x64.tar.gz -C /usr/local && \
         rm /usr/local/jdk-8u202-linux-x64.tar.gz && \
         export JAVA_HOME=/usr/local/jdk1.8.0_202 && \
         export PATH=$JAVA_HOME/bin:$PATH && \
         export CLASSPATH=$JAVA_HOME/lib/tools.jar:$JAVA_HOME/lib/dt.jar && \
         mkdir -p $TMP_DIR && \
         cd $TMP_DIR && \
         git clone https://github.com/tronprotocol/java-tron.git && \
         cd java-tron && \
         git checkout master && \
         ./gradlew build -x test && \
         cd build/distributions && \
         unzip -o java-tron-1.0.0.zip && \
         mv java-tron-1.0.0 $BASE_DIR && \
         mv $JAVA_HOME/jre /usr/local/jre && \
         rm -rf $JAVA_HOME; \
    fi \
    && rm -rf $TMP_DIR \
    && yum clean all

# 公共逻辑：下载配置文件
RUN wget -P $BASE_DIR/config https://raw.githubusercontent.com/tronprotocol/tron-deployment/master/main_net_config.conf

# 根据架构动态设置环境变量
RUN if [ "$ARCH" = "x86" ]; then \
        echo "Setting JAVA_HOME for x86" && \
        echo "export JAVA_HOME=/usr/local/jre" >> /etc/profile && \
        echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> /etc/profile; \
    fi

# 仅在 x86 架构下设置环境变量
ARG ARCH
ENV JAVA_HOME=$([ "$ARCH" = "x86" ] && echo "/usr/local/jre" || echo "")
ENV PATH=$([ "$ARCH" = "x86" ] && echo "$PATH:/usr/local/jre/bin" || echo "$PATH")

# 拷贝入口脚本
COPY docker-entrypoint.sh $BASE_DIR/bin

# 设置工作目录
WORKDIR $BASE_DIR

# 入口命令
ENTRYPOINT ["./bin/docker-entrypoint.sh"]