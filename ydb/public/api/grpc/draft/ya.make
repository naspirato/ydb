PROTO_LIBRARY(api-grpc-draft)

MAVEN_GROUP_ID(com.yandex.ydb)

GRPC()

SRCS(
    dummy.proto
    fq_v1.proto
    ydb_backup_v1.proto
    ydb_clickhouse_internal_v1.proto
    ydb_datastreams_v1.proto
    ydb_dynamic_config_v1.proto
    ydb_logstore_v1.proto
    ydb_maintenance_v1.proto
    ydb_persqueue_v1.proto
    ydb_object_storage_v1.proto
    ydb_replication_v1.proto
<<<<<<< HEAD
    ydb_tablet_v1.proto
=======
    ydb_view_v1.proto
    ydb_ymq_v1.proto
>>>>>>> ed811cc157dc8464da65356f6d68ee5bfc65f40e
)

PEERDIR(
    ydb/public/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
