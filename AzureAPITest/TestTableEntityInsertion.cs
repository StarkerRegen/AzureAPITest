using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace AzureAPITest
{
    public class TestTableEntityInsertion
    {
        private const int TestCount = 500;

        private const int BatchSize = 50;

        private const string FileMetadataTableName = "InsertTest";

        private readonly string[] TestClusters = ["cluster1", "cluster2"];

        private readonly string[] TestLogtypes = ["logtype1", "logtype2"];

        private readonly ILogger _logger;

        private TableServiceClient _tableServiceClient {  get; set; }

        public TestTableEntityInsertion(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<TestTableEntityInsertion>();
            _tableServiceClient = new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
        }

        [Function("TestTableEntityInsertion")]
        public async Task RunAsync([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            await TestQueryWithoutPKAndInsert();
            await TestQueryPKAndBatchInsert();
            await TestGetEntityAndBatchInsert();
        }

        public async Task TestQueryWithoutPKAndInsert()
        {
            CreateTable(FileMetadataTableName);
            _logger.LogInformation($"Test original method： Query without PR + AddEntityAsync...");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            foreach (var cluster in TestClusters)
            {
                foreach (var localPath in TestLogtypes)
                {
                    for (int i = 0; i < TestCount; i++)
                    {
                        var path = localPath + "/" + Guid.NewGuid() + "/test.log";
                        if (!IsMetaDataEntityExist(path, cluster))
                        {
                            await InsertEntity(FileMetadataTableName, cluster, path);
                        }
                    }
                }
            }
            stopwatch.Stop();
            _logger.LogInformation($"Test original method： Query without PR + AddEntityAsync... Time elapsed: {stopwatch.ElapsedMilliseconds}ms");
        }

        public async Task TestQueryPKAndBatchInsert()
        {
            CreateTable(FileMetadataTableName + "1");
            _logger.LogInformation($"Test method 1： Query with PR + BatchAddEntity...");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            var tableClient = _tableServiceClient.GetTableClient(FileMetadataTableName + "1");
            List<FileMetadata> metadata = new List<FileMetadata>();
            foreach (var cluster in TestClusters)
            {
                foreach (var localPath in TestLogtypes)
                {
                    for (int i = 0; i < TestCount; i++)
                    {
                        var path = localPath + "/" + Guid.NewGuid() + "/test.log";
                        var pk = GetHashCode(cluster);
                        var rk = GetHashCode(cluster + path);
                        if (!QueryIsEntityExisted(tableClient, pk, rk))
                        {
                            metadata.Add(new FileMetadata
                            {
                                PartitionKey = pk,
                                RowKey = rk,
                                CreatedOn = DateTime.UtcNow,
                                EnqueueTime = DateTime.UtcNow,
                                LogTypeName = "test",
                                Tier = 1,
                                KustoClusterName = "test",
                                KustoDatabaseName = "test",
                                KustoTableName = "test",
                                LocalPath = "test",
                                ShouldNotRetry = false,
                                RetryCount = 0,
                                OperationId = null,
                                LastUpdatedTime = DateTime.UtcNow,
                                StorageAccountName = "test",
                                ContainerName = "local",
                                FileSizeInBytes = 100,
                            });
                        }

                        if (metadata.Count % BatchSize == 0 && metadata.Count > 0)
                        {
                            await InsertBatch(FileMetadataTableName + "1", metadata);
                            metadata.Clear();
                        }
                    }
                }
            }

            stopwatch.Stop();
            _logger.LogInformation($"Test method 1： Query with PR + BatchAddEntity... Time elapsed: {stopwatch.ElapsedMilliseconds}ms");
        }

        public async Task TestGetEntityAndBatchInsert()
        {
            CreateTable(FileMetadataTableName + "2");
            _logger.LogInformation($"Test method 2： GetEntity + BatchAddEntity... ");
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            var tableClient2 = _tableServiceClient.GetTableClient(FileMetadataTableName + "2");
            List<FileMetadata> metadata2 = new List<FileMetadata>();
            foreach (var cluster in TestClusters)
            {
                foreach (var localPath in TestLogtypes)
                {
                    for (int i = 0; i < TestCount; i++)
                    {
                        var path = localPath + "/" + Guid.NewGuid() + "/test.log";
                        var pk = GetHashCode(cluster);
                        var rk = GetHashCode(cluster + path);
                        if (!IsEntityExisted(tableClient2, pk, rk))
                        {
                            metadata2.Add(new FileMetadata
                            {
                                PartitionKey = pk,
                                RowKey = rk,
                                CreatedOn = DateTime.UtcNow,
                                EnqueueTime = DateTime.UtcNow,
                                LogTypeName = "test",
                                Tier = 1,
                                KustoClusterName = "test",
                                KustoDatabaseName = "test",
                                KustoTableName = "test",
                                LocalPath = "test",
                                ShouldNotRetry = false,
                                RetryCount = 0,
                                OperationId = null,
                                LastUpdatedTime = DateTime.UtcNow,
                                StorageAccountName = "test",
                                ContainerName = "local",
                                FileSizeInBytes = 100,
                            });
                        }

                        if (metadata2.Count % BatchSize == 0 && metadata2.Count > 0)
                        {
                            await InsertBatch(FileMetadataTableName + "2", metadata2);
                            metadata2.Clear();
                        }
                    }
                }
            }
            stopwatch.Stop();
            _logger.LogInformation($"Test method 2： GetEntity + BatchAddEntity... Time elapsed: {stopwatch.ElapsedMilliseconds}ms");
        }

        public void CreateTable(string name)
        {
            try
            {
                TableItem table = _tableServiceClient.CreateTableIfNotExists(name);
            }
            catch
            {
                _logger.LogError($"Error creating table {name}");
            }
        }

        public async Task InsertEntity(string tableName, string cluster, string localPath)
        {
            try
            {
                TableClient tableClient = _tableServiceClient.GetTableClient(tableName);
                var entity = new FileMetadata
                {
                    PartitionKey = GetHashCode(cluster),
                    RowKey = GetHashCode(cluster + localPath),
                    CreatedOn = DateTime.UtcNow,
                    EnqueueTime = DateTime.UtcNow,
                    LogTypeName = "test",
                    Tier = 1,
                    KustoClusterName = "test",
                    KustoDatabaseName = "test",
                    KustoTableName = "test",
                    LocalPath = "test",
                    ShouldNotRetry = false,
                    RetryCount = 0,
                    OperationId = null,
                    LastUpdatedTime = DateTime.UtcNow,
                    StorageAccountName = "test",
                    ContainerName = "local",
                    FileSizeInBytes = 100,
                };
                await tableClient.AddEntityAsync(entity).ConfigureAwait(false);
            }
            catch
            {
                _logger.LogError($"Error inserting entity into table {tableName}");
            }
        }

        public async Task InsertBatch(string tableName, List<FileMetadata> metadata)
        {
            try
            {
                TableClient tableClient = _tableServiceClient.GetTableClient(tableName);
                List<TableTransactionAction> addEntitiesBatch = [.. metadata.Select(m => new TableTransactionAction(TableTransactionActionType.Add, m))];
                Response<IReadOnlyList<Response>> responses = await tableClient.SubmitTransactionAsync(addEntitiesBatch).ConfigureAwait(false);
            }
            catch
            {
                _logger.LogError($"Error inserting batch into table {tableName}");
            }
        }

        public static string GetHashCode(string str)
        {
            int hashCode = 0;

            // Unicode Encode Covering all characterset
            byte[] byteContents = Encoding.Unicode.GetBytes(str);
            using (var hash = SHA256.Create())
            {
                byte[] hashText = hash.ComputeHash(byteContents);
                int hashCodeStart = BitConverter.ToInt32(hashText, 0);
                int hashCodeMedium = BitConverter.ToInt32(hashText, 8);
                int hashCodeEnd = BitConverter.ToInt32(hashText, 24);
                hashCode = hashCodeStart ^ hashCodeMedium ^ hashCodeEnd;
            }

            return $"{hashCode}".Replace("-", "0");
        }

        public static bool IsEntityExisted(TableClient tableClient, string partitionKey, string rowKey)
        {
            try
            {
                Response<TableEntity> response = tableClient.GetEntity<TableEntity>(partitionKey, rowKey);
                return response.Value != null;
            }
            catch
            {
                return false;
            }
        }

        public bool QueryIsEntityExisted(TableClient tableClient, string partitionKey, string rowKey)
        {
            try
            {
                Pageable<FileMetadata> entities = tableClient.Query<FileMetadata>(ent => ent.PartitionKey == partitionKey && ent.RowKey == rowKey);
                return entities.Any();
            }
            catch
            {
                return false;
            }
        }

        public bool IsMetaDataEntityExist(string localPath, string cluster)
        {
            if (_tableServiceClient == null)
            {
                _logger.LogError("Table service client is null.");
                return false;
            }

            try
            {
                var tableClient = _tableServiceClient.GetTableClient(FileMetadataTableName);
                Pageable<FileMetadata> queryEntityResults = tableClient.Query<FileMetadata>(ent => ent.LocalPath == localPath && ent.KustoClusterName == cluster);
                return queryEntityResults.Any();
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError("Failed to query file metadata entity. {Message} {Uri}", ex.Message, localPath);
                return false;
            }
        }
    }
}
