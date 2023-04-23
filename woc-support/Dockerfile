FROM robobenklein/home:latest

USER root
# enable sshd
RUN rm -f /etc/service/sshd/down
RUN /etc/my_init.d/00_regen_ssh_host_keys.sh
RUN install_clean python3-dev libghc-bzlib-dev
ARG UNAME=testuser
ARG UID=1000
ARG GID=1000
RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -G sudo -o -s /bin/zsh $UNAME
RUN usermod -aG docker_env $UNAME
RUN chown -R :docker_env /etc/container_environment /etc/container_environment.sh /etc/container_environment.json
RUN chmod -R g+rwX /etc/container_environment /etc/container_environment.sh /etc/container_environment.json
USER $UNAME
CMD sudo /sbin/my_init
WORKDIR /home/$UNAME
