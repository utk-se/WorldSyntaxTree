FROM robobenklein/home:latest

USER root
RUN install_clean python3-dev libghc-bzlib-dev
ARG UNAME=testuser
ARG UID=1000
ARG GID=1000
RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -G sudo -o -s /bin/zsh $UNAME
USER $UNAME
WORKDIR /home/$UNAME
