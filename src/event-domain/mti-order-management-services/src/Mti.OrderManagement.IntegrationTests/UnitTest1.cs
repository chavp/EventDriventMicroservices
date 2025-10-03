using FluentResults;
using FluentResults.Extensions;

namespace Mti.OrderManagement.IntegrationTests
{
    public class UnitTest1
    {
        [Fact]
        public async Task Test1()
        {
            var result = await getHello()
                .Bind(resp => concat(resp))
                .Bind(resp => concat2(resp))
                .MapSuccesses(resp =>
                {
                    Console.WriteLine($"Response: {resp}");
                    return resp;
                })
                .MapErrors(errors =>
                {
                    foreach (var error in errors.Reasons)
                    {
                        
                        Console.WriteLine($"Error: {error.Message}");
                    }
                    return errors;
                });

        }

        public async Task<Result<string>> getHello()
        {
            var resp = "Hell";
            try
            {
                return Result.Ok(resp);
            }
            catch (Exception ex)
            {
                return Result.Fail(new Error("An error occurred while processing the request.")
                    .CausedBy(ex)
                    .WithMetadata("ErrorCode", "TestError")
                    .WithMetadata("Details", "This is a test error for demonstration purposes."));
            }
        }

        public async Task<Result<string>> concat(string con)
        {
            var resp = $"Con {con}";
            try
            {
                return Result.Ok(resp);
            }
            catch (Exception ex)
            {
                return Result.Fail(new Error("An error occurred while processing the request.")
                    .CausedBy(ex)
                    .WithMetadata("ErrorCode", "TestError")
                    .WithMetadata("Details", "This is a test error for demonstration purposes."));
            }
        }
        public async Task<Result<string>> concat2(string con)
        {
            var resp = $"Con2 {con}";
            try
            {
                throw new Exception("Test exception for demonstration purposes.");
                return Result.Ok(resp);
            }
            catch (Exception ex)
            {
                return Result.Fail(new Error("An error occurred while processing the request.")
                    .CausedBy(ex)
                    .WithMetadata("ErrorCode", "TestError")
                    .WithMetadata("Details", "This is a test error for demonstration purposes."));
            }
        }
    }
}