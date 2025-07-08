FROM debian:trixie-slim

RUN mkdir /opt/python3.12

# To avoid .pyc files and save space
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install all dependecnies you need to compile Python3.12
RUN apt update \
    && apt install -y --no-install-recommends\
    wget \
    libffi-dev\
    gcc \
    build-essential \
    curl \
    tcl-dev\
    tk-dev \
    uuid-dev \
    libssl-dev libsqlite3-dev \
    ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Download Python source code from official site and build it
RUN wget --no-check-certificate https://www.python.org/ftp/python/3.12.0/Python-3.12.0.tgz \
    && tar -zxvf Python-3.12.0.tgz \
    && cd Python-3.12.0 && ./configure --prefix=/opt/python3.12 && make && make install && cd .. \
    && rm Python-3.12.0.tgz \
    && rm -r Python-3.12.0/ \
    && ln -s /opt/python3.12/bin/python3 /usr/bin/python \
    && ln -s /opt/python3.12/bin/pip3 /usr/bin/pip
ARG CI_COMMIT_SHORT_SHA=xxxxxx
ARG CI_COMMIT_TAG=0.0.0
##########
WORKDIR /app
##########
ARG local_user=app_user
ARG local_group=app_grp
RUN useradd -ms /bin/bash $local_user\
    && groupadd $local_group\ 
    && usermod -a -G $local_group $local_user
ENV LOCAL_USER=$local_user \
    LOCAL_GROUP=$local_group \
    GIT_HASH=$CI_COMMIT_SHORT_SHA \
    VERSION=$CI_COMMIT_TAG \
    PYTHONPATH=${PYTHONPATH}:/opt/python3.12/lib/python3.12/site-packages/:/home/${local_user}/.local/lib/python3.12/site-packages\
    PATH=$PATH:/opt/python3.12/bin:/home/${local_user}/.local/bin
##########
COPY --chown=$LOCAL_USER:$LOCAL_GROUP start.sh .
# Need to allow the user to create folder in the application installation folder
RUN chmod +x start.sh \
    && chown $LOCAL_USER /app 
##########
# Allow current user to add libraries to normal site-packages 
RUN chown -R $LOCAL_USER:$LOCAL_GROUP /usr/bin/pip /usr/bin/python

##########
USER $local_user
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && uv --version
##########
EXPOSE 5000
CMD ["/app/start.sh"]
