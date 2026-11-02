#!/bin/bash

# Build settings
MAVEN_BUILD="true"
DOCKER_BUILD="true"
DOCKER_PUSH="true"

# Docker build settings
DOCKER_BASE_IMAGE="alpine/openjdk17:17.0.7.7.05"
# Base image for applications whose pom.xml sets java.version to 21
DOCKER_BASE_IMAGE_JAVA21="alpine/openjdk21:21.0.3.9.04"

# Dockerfiles that will use agent inside the base image
# DOCKER_FILE_QUARKUS="docker/Dockerfile.jvm"
# DOCKER_FILE_SPRING_BOOT="docker/Dockerfile"

# Dockerfiles with logic to add custom agents, can be used for local builds
DOCKER_FILE_QUARKUS="docker/Dockerfile.jvm-custom-local"
DOCKER_FILE_SPRING_BOOT="docker/Dockerfile.custom-local"

# Parts of docker image name with which image can be push on docker registry
DOCKER_REGISTRY="github.com:8080"
DOCKER_REGISTRY_PATH="personal"
DOCKER_TAG="build4"

# Script variables
applications=(
    "quarkus-2-vertx"
    "quarkus-3-vertx"
    "quarkus-3-vertx-resteasy"
    "spring-boot-2-jetty"
    "spring-boot-2-tomcat"
    "spring-boot-2-undertow"
    "spring-boot-3-jetty"
    "spring-boot-3-tomcat"
    "spring-boot-3-undertow"
    "spring-boot-4-tomcat"
)

built_images=()

# Main logic

echo "=> Build applications via maven ..."

if [[ "${MAVEN_BUILD}" == "true" ]]; then
    for application in "${applications[@]}"; do
        echo "=> Build ${application} application ..."
        mvn clean package -f "${application}/pom.xml"
    done
else
    echo "=> Skip applications maven build!"
fi

echo "=> Copy agent installer-cloud.zip to all applications ..."

for application in "${applications[@]}"; do
    echo "=> Copy agent archive to ${application} ..."
    rm -rf "./${application}/agent"
    rm -rf "./${application}/target/agent"
    mkdir -p "./${application}/target/agent"
    cp ./target/* "./${application}/target/agent/"
done

echo "=> Building docker images ..."

if [[ "${DOCKER_BUILD}" == "true" ]]; then
    for application in "${applications[@]}"; do
        echo "=> Building and push ${application} docker image ..."

        tag="${DOCKER_REGISTRY}/${DOCKER_REGISTRY_PATH}/${application}:${DOCKER_TAG}"

        echo "=> Docker tag of new image ${tag} ..."

        dockerfile="${application}/${DOCKER_FILE_SPRING_BOOT}"
        if [[ "${application}" == *"quarkus"* ]]; then
            dockerfile="${application}/${DOCKER_FILE_QUARKUS}"
        fi

        java_version=$(sed -n 's:.*<java.version>\([0-9]*\)</java.version>.*:\1:p' "${application}/pom.xml" | head -n 1)
        case "${java_version}" in
            11|17) base_image="${DOCKER_BASE_IMAGE}" ;;
            21) base_image="${DOCKER_BASE_IMAGE_JAVA21}" ;;
            *)
                echo "=> No base image for ${application}: java.version '${java_version}' in ${application}/pom.xml is not 11, 17, or 21" >&2
                exit 1
                ;;
        esac

        docker build \
            --tag "${tag}" \
            --build-arg IMAGE="${base_image}" \
            --file "${dockerfile}" ./${application}/

        built_images=("${built_images[@]}" "$tag")
        if [[ "${DOCKER_PUSH}" == "true" ]]; then
            echo "==> Built and pushed image: ${tag}"
            docker push ${tag}
        else
            echo "=> Skip built docker image push!"
            echo "==> Built image: ${tag}"
        fi
    done
else
    echo "=> Skip applications docker image build!"
fi

echo "=> Built docker images:"
for image in "${built_images[@]}"; do
    echo "${image}"
done
