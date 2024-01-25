FROM amazoncorretto:17-al2023-jdk

# Disable caching to make the dnf upgrade effective (the build job passes the build time as the value of this argument)
ARG CACHEBUST=1

# Add a custom trusted root ca certificate to the image
ARG CUSTOM_TRUSTED_ROOT_CA_CERTIFICATE_URL

# Create/run user/grouop configuration
ARG APP_USER=javauser
ARG APP_USER_UID=1000
ARG APP_USER_GROUP=javagroup
ARG APP_USER_GID=1000
ARG RUN_USER=javauser
ARG RUN_USER_GROUP=javagroup

# Upgrade the system
RUN dnf -y --releasever=latest --setopt=install_weak_deps=False upgrade && \
# Add less, vi, nano, ps, ping, ss, traceroute, telnet, dig, find and unzip (curl is already included in the image)
    dnf -y --releasever=latest --setopt=install_weak_deps=False install less vim nano procps-ng iputils iproute traceroute telnet bind-utils findutils unzip && \
# Create the non-root user to run the application
    dnf -y --releasever=latest --setopt=install_weak_deps=False install shadow-utils && \
    groupadd --system --gid ${APP_USER_GID} ${APP_USER_GROUP} && \
    useradd --uid ${APP_USER_UID} --gid ${APP_USER_GID} --no-user-group --home-dir /app --create-home --shell /bin/bash ${APP_USER} && \
    chown -R ${APP_USER}:${APP_USER_GROUP} /app && \
    dnf -y remove shadow-utils && \
# Clean up the yum cache
    dnf -y clean all && \
    rm -rf /var/cache/dnf

RUN if [ -n "${CUSTOM_TRUSTED_ROOT_CA_CERTIFICATE_URL}" ]; then \
# Add the custom trusted root ca certificate
        curl "${CUSTOM_TRUSTED_ROOT_CA_CERTIFICATE_URL}" -o "/etc/pki/ca-trust/source/anchors/${CUSTOM_TRUSTED_ROOT_CA_CERTIFICATE_URL##*/}" && \
        update-ca-trust ; \
    fi

# Expose the application's listening port
EXPOSE 5000 8080

# Add a healthcheck (note that this only works locally, Kubernetes explicitly disables this one)
HEALTHCHECK CMD curl --fail http://localhost:8080/actuator/health || exit 1

# Add the application itself
WORKDIR /app
COPY target/*.jar .
RUN chown -R ${RUN_USER}:${RUN_USER_GROUP} .
USER ${RUN_USER}:${RUN_USER_GROUP}

# Run the application
CMD java -jar *.jar
