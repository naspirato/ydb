# Generated by devtools/yamaker (pypi).

PY3_LIBRARY()

VERSION(7.1.0)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/requests
    contrib/python/urllib3
)

NO_LINT()

NO_CHECK_IMPORTS(
    docker.transport.npipeconn
    docker.transport.npipesocket
    docker.transport.sshconn
)

PY_SRCS(
    TOP_LEVEL
    docker/__init__.py
    docker/_version.py
    docker/api/__init__.py
    docker/api/build.py
    docker/api/client.py
    docker/api/config.py
    docker/api/container.py
    docker/api/daemon.py
    docker/api/exec_api.py
    docker/api/image.py
    docker/api/network.py
    docker/api/plugin.py
    docker/api/secret.py
    docker/api/service.py
    docker/api/swarm.py
    docker/api/volume.py
    docker/auth.py
    docker/client.py
    docker/constants.py
    docker/context/__init__.py
    docker/context/api.py
    docker/context/config.py
    docker/context/context.py
    docker/credentials/__init__.py
    docker/credentials/constants.py
    docker/credentials/errors.py
    docker/credentials/store.py
    docker/credentials/utils.py
    docker/errors.py
    docker/models/__init__.py
    docker/models/configs.py
    docker/models/containers.py
    docker/models/images.py
    docker/models/networks.py
    docker/models/nodes.py
    docker/models/plugins.py
    docker/models/resource.py
    docker/models/secrets.py
    docker/models/services.py
    docker/models/swarm.py
    docker/models/volumes.py
    docker/tls.py
    docker/transport/__init__.py
    docker/transport/basehttpadapter.py
    docker/transport/npipeconn.py
    docker/transport/npipesocket.py
    docker/transport/sshconn.py
    docker/transport/unixconn.py
    docker/types/__init__.py
    docker/types/base.py
    docker/types/containers.py
    docker/types/daemon.py
    docker/types/healthcheck.py
    docker/types/networks.py
    docker/types/services.py
    docker/types/swarm.py
    docker/utils/__init__.py
    docker/utils/build.py
    docker/utils/config.py
    docker/utils/decorators.py
    docker/utils/fnmatch.py
    docker/utils/json_stream.py
    docker/utils/ports.py
    docker/utils/proxy.py
    docker/utils/socket.py
    docker/utils/utils.py
    docker/version.py
)

RESOURCE_FILES(
    PREFIX contrib/python/docker/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()