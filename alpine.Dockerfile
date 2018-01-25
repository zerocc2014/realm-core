FROM alpine:3.6

RUN apk add --no-cache \
        build-base \
        cmake \
        git \
        openssl-dev \
        procps-dev  

# undefine _GNU_SOURCE to trick core and define _BSD_SOURCE to trick musl
ENV CXXFLAGS="-U_GNU_SOURCE -D_BSD_SOURCE -DREALM_LIBC_MUSL"

VOLUME /source
VOLUME /out

WORKDIR /source