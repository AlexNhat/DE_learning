from pyspark.sql import SparkSession
from typing import Dict, Any

class SparkConfigs:
    """
    A utility class containing all major Apache Spark configuration properties as dictionaries.
    Each entry is in the format: "config_key": "description (default: value)".
    Configurations are grouped by categories for better organization, based on official Spark documentation.
    Note: This is not an exhaustive list (Spark has hundreds of configs), but covers key ones from core categories.
    For full details, refer to the official docs: https://spark.apache.org/docs/latest/configuration.html
    """

    APPLICATION_PROPERTIES: Dict[str, str] = {
        "spark.app.name": "The name of your application. This will appear in the UI and in log data. (default: (none))",
        "spark.driver.cores": "Number of cores to use for the driver process, only in cluster mode. (default: 1)",
        "spark.driver.maxResultSize": "Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. (default: 1g)",
        "spark.driver.memory": "Amount of memory to use for the driver process, i.e. where SparkContext is initialized, in the same format as JVM memory strings with a size unit suffix ('k', 'm', 'g' or 't') (e.g. 512m, 2g). (default: 1g)",
        "spark.driver.memoryOverhead": "Amount of non-heap memory to be allocated per driver process in cluster mode, in MiB unless otherwise specified. This is memory that accounts for things like VM overheads, interned strings, other native overheads, etc. (default: driverMemory * spark.driver.memoryOverheadFactor, with minimum of spark.driver.minMemoryOverhead)",
        "spark.driver.minMemoryOverhead": "The minimum amount of non-heap memory to be allocated per driver process in cluster mode, in MiB unless otherwise specified, if spark.driver.memoryOverhead is not defined. (default: 384m)",
        "spark.driver.memoryOverheadFactor": "Fraction of driver memory to be allocated as additional non-heap memory per driver process in cluster mode. (default: 0.10)",
        "spark.driver.resource.{resourceName}.amount": "Amount of a particular resource type to use on the driver. If this is used, you must also specify the spark.driver.resource.{resourceName}.discoveryScript for the driver to find the resource on startup. (default: 0)",
        "spark.driver.resource.{resourceName}.discoveryScript": "A script for the driver to run to discover a particular resource type. This should write to STDOUT a JSON string in the format of the ResourceInformation class. (default: None)",
        "spark.driver.resource.{resourceName}.vendor": "Vendor of the resources to use for the driver. This option is currently only supported on Kubernetes and is actually both the vendor and domain following the Kubernetes device plugin naming convention. (default: None)",
        "spark.resources.discoveryPlugin": "Comma-separated list of class names implementing org.apache.spark.api.resource.ResourceDiscoveryPlugin to load into the application. (default: org.apache.spark.resource.ResourceDiscoveryScriptPlugin)",
        "spark.executor.memory": "Amount of memory to use per executor process, in the same format as JVM memory strings with a size unit suffix ('k', 'm', 'g' or 't') (e.g. 512m, 2g). (default: 1g)",
        "spark.executor.pyspark.memory": "The amount of memory to be allocated to PySpark in each executor, in MiB unless otherwise specified. (default: Not set)",
        "spark.executor.memoryOverhead": "Amount of additional memory to be allocated per executor process, in MiB unless otherwise specified. (default: executorMemory * spark.executor.memoryOverheadFactor, with minimum of spark.executor.minMemoryOverhead)",
        "spark.executor.minMemoryOverhead": "The minimum amount of non-heap memory to be allocated per executor process, in MiB unless otherwise specified, if spark.executor.memoryOverhead is not defined. (default: 384m)",
        "spark.executor.memoryOverheadFactor": "Fraction of executor memory to be allocated as additional non-heap memory per executor process. (default: 0.10)",
        "spark.executor.resource.{resourceName}.amount": "Amount of a particular resource type to use per executor process. (default: 0)",
        "spark.executor.resource.{resourceName}.discoveryScript": "A script for the executor to run to discover a particular resource type. (default: None)",
        "spark.executor.resource.{resourceName}.vendor": "Vendor of the resources to use for the executors. (default: None)",
        "spark.extraListeners": "A comma-separated list of classes that implement SparkListener; when initializing SparkContext, instances of these classes will be created and registered with Spark's listener bus. (default: (none))",
        "spark.local.dir": "Directory to use for 'scratch' space in Spark, including map output files and RDDs that get stored on disk. (default: /tmp)",
        "spark.logConf": "Logs the effective SparkConf as INFO when a SparkContext is started. (default: false)",
        "spark.master": "The cluster manager to connect to. (default: (none))",
        "spark.submit.deployMode": "The deploy mode of Spark driver program, either 'client' or 'cluster'. (default: client)",
        "spark.log.callerContext": "Application information that will be written into Yarn RM log/HDFS audit log when running on Yarn/HDFS. (default: (none))",
        "spark.log.level": "When set, overrides any user-defined log settings as if calling SparkContext.setLogLevel() at Spark startup. (default: (none))",
        "spark.driver.supervise": "If true, restarts the driver automatically if it fails with a non-zero exit status. Only has effect in Spark standalone mode. (default: false)",
        "spark.driver.timeout": "A timeout for Spark driver in minutes. 0 means infinite. (default: 0min)",
        "spark.driver.log.localDir": "Specifies a local directory to write driver logs and enable Driver Log UI Tab. (default: (none))",
        "spark.driver.log.dfsDir": "Base directory in which Spark driver logs are synced, if spark.driver.log.persistToDfs.enabled is true. (default: (none))",
        "spark.driver.log.persistToDfs.enabled": "If true, spark application running in client mode will write driver logs to a persistent storage. (default: false)",
        "spark.driver.log.layout": "The layout for the driver logs that are synced. (default: %d{yy/MM/dd HH:mm:ss.SSS} %t %p %c{1}: %m%n%ex)"
    }

    DYNAMIC_ALLOCATION: Dict[str, str] = {
        "spark.dynamicAllocation.enabled": "Enables dynamic allocation of executors. (default: (none))",
        "spark.dynamicAllocation.minExecutors": "Minimum number of executors to maintain. (default: (none))",
        "spark.dynamicAllocation.maxExecutors": "Maximum number of executors to allocate. (default: (none))",
        "spark.dynamicAllocation.initialExecutors": "Initial number of executors to start with. (default: (none))",
        "spark.dynamicAllocation.schedulerBacklogTimeout": "Timeout for requesting new executors when tasks are backlogged. (default: 1s)",
        "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout": "Timeout for subsequent executor requests after backlog. (default: schedulerBacklogTimeout)",
        "spark.dynamicAllocation.shuffleTracking.enabled": "Enables shuffle file tracking for dynamic allocation without external shuffle service. (default: true)",
        "spark.dynamicAllocation.shuffleTracking.timeout": "Timeout for executors holding shuffle data when tracking is enabled. (default: infinity)"
    }

    THREAD_CONFIGURATIONS: Dict[str, str] = {
        "spark.driver.rpc.io.serverThreads": "Number of threads in server thread pool for driver. (default: Fall back on spark.rpc.io.serverThreads)",
        "spark.driver.rpc.io.clientThreads": "Number of threads in client thread pool for driver. (default: Fall back on spark.rpc.io.clientThreads)",
        "spark.driver.rpc.netty.dispatcher.numThreads": "Number of threads in RPC message dispatcher for driver. (default: Fall back on spark.rpc.netty.dispatcher.numThreads)",
        "spark.executor.rpc.io.serverThreads": "Number of threads in server thread pool for executor. (default: Fall back on spark.rpc.io.serverThreads)",
        "spark.executor.rpc.io.clientThreads": "Number of threads in client thread pool for executor. (default: Fall back on spark.rpc.io.clientThreads)",
        "spark.executor.rpc.netty.dispatcher.numThreads": "Number of threads in RPC message dispatcher for executor. (default: Fall back on spark.rpc.netty.dispatcher.numThreads)"
    }

    SPARK_CONNECT_SERVER: Dict[str, str] = {
        "spark.api.mode": "Mode for Spark Connect server: classic or connect. (default: classic)",
        "spark.connect.grpc.binding.address": "Address for Spark Connect server to bind. (default: (none))",
        "spark.connect.grpc.binding.port": "Port for Spark Connect server to bind. (default: 15002)",
        "spark.connect.grpc.port.maxRetries": "Maximum port retry attempts for gRPC server binding. (default: 0)",
        "spark.connect.grpc.interceptor.classes": "Comma-separated list of gRPC server interceptors. (default: (none))",
        "spark.connect.grpc.arrow.maxBatchSize": "Maximum size of Arrow batch sent from server to client. (default: 4m)",
        "spark.connect.grpc.maxInboundMessageSize": "Maximum inbound gRPC message size in bytes. (default: 134217728)",
        "spark.connect.extensions.relation.classes": "Classes implementing RelationPlugin for custom Relation types. (default: (none))",
        "spark.connect.extensions.expression.classes": "Classes implementing ExpressionPlugin for custom Expression types. (default: (none))",
        "spark.connect.extensions.command.classes": "Classes implementing CommandPlugin for custom Command types. (default: (none))",
        "spark.connect.ml.backend.classes": "Classes implementing MLBackendPlugin to replace Spark ML operators. (default: (none))",
        "spark.connect.jvmStacktrace.maxSize": "Maximum stack trace size when JVM stacktrace is enabled. (default: 1024)",
        "spark.sql.connect.ui.retainedSessions": "Number of client sessions retained in Spark Connect UI history. (default: 200)",
        "spark.sql.connect.ui.retainedStatements": "Number of statements retained in Spark Connect UI history. (default: 200)",
        "spark.sql.connect.enrichError.enabled": "Enrich errors with full exception messages and server-side stacktrace. (default: true)",
        "spark.sql.connect.serverStacktrace.enabled": "Include server-side stacktrace in user-facing Spark exceptions. (default: true)",
        "spark.connect.grpc.maxMetadataSize": "Maximum size of metadata fields, e.g., in ErrorInfo. (default: 1024)",
        "spark.connect.progress.reportInterval": "Interval for reporting query progress to client; negative disables reports. (default: 2s)"
    }

    SPARK_SQL_RUNTIME: Dict[str, str] = {
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "Advisory size in bytes for shuffle partitions during adaptive optimization. (default: (value of spark.sql.adaptive.shuffle.targetPostShuffleInputSize))",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "Maximum size in bytes for broadcasting tables in adaptive joins; -1 disables broadcasting. (default: (none))",
        "spark.sql.adaptive.coalescePartitions.enabled": "Coalesce contiguous shuffle partitions to target size when adaptive execution is enabled. (default: true)",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "Initial number of shuffle partitions before coalescing; defaults to spark.sql.shuffle.partitions. (default: (none))",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "Minimum size of shuffle partitions after coalescing. (default: 1MB)",
        "spark.sql.adaptive.coalescePartitions.parallelismFirst": "Prioritize parallelism over target size when coalescing partitions. (default: true)",
        "spark.sql.adaptive.customCostEvaluatorClass": "Custom cost evaluator class for adaptive execution; defaults to SimpleCostEvaluator. (default: (none))",
        "spark.sql.adaptive.enabled": "Enable adaptive query execution with runtime statistics re-optimization. (default: true)",
        "spark.sql.adaptive.forceOptimizeSkewedJoin": "Force enable OptimizeSkewedJoin even if it adds extra shuffle. (default: false)",
        "spark.sql.adaptive.localShuffleReader.enabled": "Use local shuffle reader when partitioning is not needed after join conversion. (default: true)",
        "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "Maximum size per partition for local hash map in shuffled hash joins. (default: 0b)",
        "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled": "Optimize skewed shuffle partitions in RebalancePartitions by splitting them. (default: true)",
        "spark.sql.adaptive.optimizer.excludedRules": "List of optimizer rules to disable, separated by comma. (default: (none))",
        "spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor": "Factor to merge small partitions during splitting. (default: 0.2)",
        "spark.sql.adaptive.skewJoin.enabled": "Dynamically handle skew in shuffled joins by splitting skewed partitions. (default: true)",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "Factor to determine if a partition is skewed relative to median size. (default: 5.0)",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "Absolute size threshold for a partition to be considered skewed. (default: 256MB)",
        "spark.sql.allowNamedFunctionArguments": "Enable support for named parameters in functions that implement it. (default: true)",
        "spark.sql.ansi.doubleQuotedIdentifiers": "Treat double-quoted literals as identifiers when ANSI is enabled. (default: false)",
        "spark.sql.ansi.enabled": "Use ANSI SQL dialect, throwing exceptions for invalid inputs. (default: true)",
        "spark.sql.ansi.enforceReservedKeywords": "Enforce ANSI reserved keywords, forbidding their use as identifiers or aliases. (default: false)",
        "spark.sql.ansi.relationPrecedence": "Apply ANSI JOIN precedence over comma when combining relations. (default: false)",
        "spark.sql.autoBroadcastJoinThreshold": "Maximum size for broadcasting tables in joins; -1 disables broadcasting. (default: 10MB)",
        "spark.sql.avro.compression.codec": "Compression codec for AVRO files; supported: uncompressed, deflate, snappy, bzip2, xz, zstandard. (default: snappy)",
        "spark.sql.avro.deflate.level": "Compression level for deflate codec in AVRO; -1 defaults to level 6. (default: -1)",
        "spark.sql.avro.filterPushdown.enabled": "Enable filter pushdown to Avro datasource. (default: true)",
        "spark.sql.avro.xz.level": "Compression level for xz codec in AVRO files. (default: 6)",
        "spark.sql.avro.zstandard.bufferPool.enabled": "Enable buffer pool for ZSTD JNI when writing AVRO files. (default: false)",
        "spark.sql.avro.zstandard.level": "Compression level for zstandard codec in AVRO files. (default: 3)",
        "spark.sql.binaryOutputStyle": "Output style for binary data; values: UTF-8, BASIC, BASE64, HEX, HEX_DISCRETE. (default: (none))",
        "spark.sql.broadcastTimeout": "Timeout in seconds for broadcast wait time in broadcast joins. (default: 300)",
        "spark.sql.bucketing.coalesceBucketsInJoin.enabled": "Coalesce buckets of different counts in bucketed table joins. (default: false)",
        "spark.sql.bucketing.coalesceBucketsInJoin.maxBucketRatio": "Maximum ratio of bucket counts for coalescing to apply. (default: 4)",
        "spark.sql.catalog.spark_catalog": "Catalog implementation for Spark's built-in v1 catalog as v2 interface. (default: builtin)",
        "spark.sql.cbo.enabled": "Enable cost-based optimization for plan statistics estimation. (default: false)",
        "spark.sql.cbo.joinReorder.dp.star.filter": "Apply star-join filter heuristics in cost-based join enumeration. (default: false)",
        "spark.sql.cbo.joinReorder.dp.threshold": "Maximum number of joined nodes in dynamic programming for join reorder. (default: 12)",
        "spark.sql.cbo.joinReorder.enabled": "Enable join reordering in cost-based optimization. (default: false)",
        "spark.sql.cbo.planStats.enabled": "Fetch row counts and column statistics from catalog for logical plan. (default: false)",
        "spark.sql.cbo.starSchemaDetection": "Enable join reordering based on star schema detection. (default: false)",
        "spark.sql.charAsVarchar": "Replace CHAR type with VARCHAR in DDL operations for new tables. (default: false)",
        "spark.sql.chunkBase64String.enabled": "Truncate base64 strings into lines of at most 76 characters. (default: true)",
        "spark.sql.cli.print.header": "Print column names in spark-sql CLI query output. (default: false)",
        "spark.sql.columnNameOfCorruptRecord": "Name of column for storing unparsed JSON/CSV records. (default: _corrupt_record)",
        "spark.sql.csv.filterPushdown.enabled": "Enable filter pushdown to CSV datasource. (default: true)",
        "spark.sql.datetime.java8API.enabled": "Use java.time classes for TimestampType and DateType instead of java.sql. (default: false)",
        "spark.sql.debug.maxToStringFields": "Maximum fields to convert to strings in debug output; excess replaced by placeholder. (default: 25)",
        "spark.sql.defaultCacheStorageLevel": "Default storage level for caching datasets and tables. (default: MEMORY_AND_DISK)",
        "spark.sql.defaultCatalog": "Default catalog name when no catalog is explicitly set. (default: spark_catalog)",
        "spark.sql.error.messageFormat": "Format of error messages: PRETTY, MINIMAL, STANDARD (JSON). (default: PRETTY)",
        "spark.sql.execution.arrow.enabled": "Deprecated; use spark.sql.execution.arrow.pyspark.enabled instead. (default: false)",
        "spark.sql.execution.arrow.fallback.enabled": "Deprecated; use spark.sql.execution.arrow.pyspark.fallback.enabled instead. (default: true)",
        "spark.sql.execution.arrow.localRelationThreshold": "Threshold for using local collections vs. deserialized Arrow batches. (default: 48MB)",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "Maximum records per ArrowRecordBatch; 0 or negative means no limit. (default: 10000)",
        "spark.sql.execution.arrow.pyspark.enabled": "Enable Apache Arrow for PySpark columnar data transfers. (default: (value of spark.sql.execution.arrow.enabled))",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "Enable fallback to non-optimized implementations on Arrow errors in PySpark. (default: (value of spark.sql.execution.arrow.fallback.enabled))",
        "spark.sql.execution.arrow.pyspark.selfDestruct.enabled": "Use Apache Arrow self-destruct for reduced memory in PySpark conversions (experimental). (default: false)",
        "spark.sql.execution.arrow.sparkr.enabled": "Enable Apache Arrow for SparkR columnar data transfers. (default: false)",
        "spark.sql.execution.arrow.transformWithStateInPandas.maxRecordsPerBatch": "Maximum state records per ArrowRecordBatch in TransformWithStateInPandas. (default: 10000)",
        "spark.sql.execution.arrow.useLargeVarTypes": "Use large variable-width vectors for strings and binaries in Arrow. (default: false)",
        "spark.sql.execution.interruptOnCancel": "Interrupt all running tasks if a query is canceled. (default: true)",
        "spark.sql.execution.pandas.inferPandasDictAsMap": "Infer Pandas Dict as MapType when converting to Spark. (default: false)",
    }

    SHUFFLE_PUSH: Dict[str, str] = {
        "spark.shuffle.push.numPushThreads": "Specify the number of threads in the block pusher pool. (default: (none))",
        "spark.shuffle.push.maxBlockSizeToPush": "The max size of an individual block to push to the remote external shuffle services. (default: 1m)",
        "spark.shuffle.push.maxBlockBatchSize": "The max size of a batch of shuffle blocks to be grouped into a single push request. (default: 3m)",
        "spark.shuffle.push.merge.finalizeThreads": "Number of threads used by driver to finalize shuffle merge. (default: 8)",
        "spark.shuffle.push.minShuffleSizeToWait": "Driver will wait for merge finalization to complete only if total shuffle data size is more than this threshold. (default: 500m)",
        "spark.shuffle.push.minCompletedPushRatio": "Fraction of minimum map partitions that should be push complete before driver starts shuffle merge finalization during push based shuffle. (default: 1.0)"
    }

    # Các phần khác có thể thêm nếu cần, nhưng để ngắn gọn, tôi dừng ở đây dựa trên ví dụ trước.
    # Bạn có thể mở rộng bằng cách thêm các dictionary khác như SHUFFLE, COMPRESSION, v.v. từ docs Spark.

    @staticmethod
    def get_all_configs() -> Dict[str, Dict[str, str]]:
        """Trả về tất cả các configs dưới dạng dict lớn."""
        return {
            "APPLICATION_PROPERTIES": SparkConfigs.APPLICATION_PROPERTIES,
            "DYNAMIC_ALLOCATION": SparkConfigs.DYNAMIC_ALLOCATION,
            "THREAD_CONFIGURATIONS": SparkConfigs.THREAD_CONFIGURATIONS,
            "SPARK_CONNECT_SERVER": SparkConfigs.SPARK_CONNECT_SERVER,
            "SPARK_SQL_RUNTIME": SparkConfigs.SPARK_SQL_RUNTIME,
            "SHUFFLE_PUSH": SparkConfigs.SHUFFLE_PUSH,
            # Thêm các category khác nếu cần
        }

    @staticmethod
    def apply_configs(spark_builder: SparkSession.Builder, selected_configs: Dict[str, Any]) -> SparkSession.Builder:
        """
        Áp dụng các config từ dict vào SparkSession.Builder.
        Ví dụ: selected_configs = {"spark.sql.execution.arrow.pyspark.enabled": "true"}
        """
        for key, value in selected_configs.items():
            spark_builder = spark_builder.config(key, value)
        return spark_builder