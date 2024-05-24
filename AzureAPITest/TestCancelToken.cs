using Azure.Storage.Blobs.Models;
using Azure;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using System.Diagnostics;

namespace AzureAPITest
{
    public class TestCancelToken
    {
        private readonly ILogger _logger;

        public TestCancelToken(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<TestCancelToken>();
        }

        [Function("TestCancelToken")]
        public async Task RunAsync([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            try
            {
                var cancelToken = new CancellationTokenSource();
                var blobContainerClient = new BlobContainerClient("UseDevelopmentStorage=true", "local");
                var blobList = blobContainerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, "TestLogType").AsPages(default, 1).WithCancellation(cancelToken.Token);
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                int count = 0;
                await foreach (Page<BlobItem> blobPage in blobList)
                {
                    if (++count > 2)
                    {
                        cancelToken.Cancel();
                    }

                    await GetTask(cancelToken.Token);
                    _logger.LogInformation($"Page {blobPage.Values.Count}");
                }

                stopwatch.Stop();
                _logger.LogInformation($"Time elapsed: {stopwatch.Elapsed}");
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Operation canceled");
            }
        }

        public async Task GetTask(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Delay(10000, cancellationToken);
        }
    }
}
