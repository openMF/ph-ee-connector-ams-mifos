FROM amazoncorretto:17-al2023-jdk

# Disable caching to make the dnf upgrade effective (the build job passes the build time as the value of this argument)
ARG CACHEBUST=1

# Upgrade the system
RUN dnf -y --releasever=latest --setopt=install_weak_deps=False upgrade && \
# Add less, vi, nano, ps, ping, netstat, ss, traceroute, telnet (curl is already included in the image)
    dnf -y --releasever=latest --setopt=install_weak_deps=False install less vim nano procps-ng iputils net-tools iproute traceroute telnet && \
# Create the non-root user to run the application
    dnf -y --releasever=latest --setopt=install_weak_deps=False install shadow-utils && \
    groupadd --system --gid 1000 javagroup && \
    useradd --uid 1000 --gid javagroup --no-user-group --home-dir /app --create-home --shell /bin/bash javauser && \
    chown -R javauser:javagroup /app && \
    dnf -y remove shadow-utils && \
# Clean up the yum cache
    dnf -y clean all && \
    rm -rf /var/cache/dnf

# Expose the application's listening port
EXPOSE 5000 8080

# Add a healthcheck (note that this only works locally, Kubernetes explicitly disables this one)
HEALTHCHECK CMD curl --fail http://localhost:8080/actuator/health || exit 1

# Add the application itself
WORKDIR /app
COPY keystore.jks /app/
COPY target/*.jar .
RUN chown -R 1000:1000 .
USER javauser:javagroup

# Run the application
CMD java -jar *.jar

