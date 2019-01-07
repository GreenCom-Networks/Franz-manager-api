FROM maven AS builder

COPY . /app/.

WORKDIR /app

RUN mvn clean package

FROM openjdk:8-jdk-alpine

ENV APP franz-manager-api

WORKDIR /usr/local/$APP

COPY apidoc apidoc

RUN apk update && apk add --no-cache libc6-compat

COPY --from=builder /app/target/$APP-jar-with-dependencies.jar $APP.jar

CMD java -Xmx${JVM_HEAP_SIZE:-1024}m -XX:+ExitOnOutOfMemoryError -jar $APP.jar