using Azure;
using Azure.Data.Tables;

namespace AzureAPITest
{
    public record FileMetadata : ITableEntity
    {
        /// <inheritdoc/>
        public string RowKey { get; set; } = default!;

        /// <inheritdoc/>
        public string PartitionKey { get; set; } = default!;

        /// <inheritdoc/>
        public ETag ETag { get; set; } = default!;

        /// <inheritdoc/>
        public DateTimeOffset? Timestamp { get; set; } = default!;

        /// <summary>
        /// LocalPath(blob name, without storage account name and container name)
        /// </summary>
        public string LocalPath { get; set; } = default!;

        /// <summary>
        /// Created time
        /// </summary>
        public DateTime CreatedOn { get; set; } = default!;

        /// <summary>
        /// Enqueue time
        /// </summary>
        public DateTime EnqueueTime { get; set; } = default!;

        /// <summary>
        /// logtype name
        /// </summary>
        public string LogTypeName { get; set; } = default!;

        /// <summary>
        /// Tier
        /// </summary>
        public int Tier { get; set; } = default!;

        /// <summary>
        /// Kusto cluster name
        /// </summary>
        public string KustoClusterName { get; set; } = default!;

        /// <summary>
        /// Kusto database name
        /// </summary>
        public string KustoDatabaseName { get; set; } = default!;

        /// <summary>
        /// Kusto table name
        /// </summary>
        public string KustoTableName { get; set; } = default!;

        /// <summary>
        /// operation id
        /// </summary>
        public string? OperationId { get; set; } = default!;

        /// <summary>
        /// last updated time
        /// </summary>
        public DateTime LastUpdatedTime { get; set; } = default!;

        /// <summary>
        /// Should not retry
        /// </summary>
        public bool ShouldNotRetry { get; set; } = default!;

        /// <summary>
        /// storage account name
        /// </summary>
        public string StorageAccountName { get; set; } = default!;

        /// <summary>
        /// Container name
        /// </summary>
        public string ContainerName { get; set; } = default!;

        /// <summary>
        /// FileSizeInBytes
        /// </summary>
        public long FileSizeInBytes { get; set; } = default!;

        /// <summary>
        /// current retry count for kusto ingestion.
        /// </summary>
        public int RetryCount { get; set; }
    }
}
