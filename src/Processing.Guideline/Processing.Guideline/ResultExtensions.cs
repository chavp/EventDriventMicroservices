using FluentResults;
using Microsoft.AspNetCore.Mvc;

namespace Processing.Guideline
{
    public static class ResultExtensions
    {
        public static Result<TValue> Success<TValue>(TValue value) => Result.Ok(value);

        public static Result<TValue> Failure<TValue>(IReadOnlyCollection<Error> errors) => Result.FailIfNotEmpty(errors);

        public static Result<TValue> Create<TValue>(TValue value, List<Error> errors)
            where TValue : class
            => value is null ? Failure<TValue>(errors) : Success(value);

        public static async Task<TOut> Match<TIn, TOut>(
            this Task<Result<TIn>> resultTask,
            Func<TIn, TOut> onSuccess,
            Func<IReadOnlyList<IError>, TOut> onFailure)
        {
            Result<TIn> result = await resultTask;

            return result.IsSuccess ? onSuccess(result.Value) : onFailure(result.Errors);
        }

        public static TOut Match<TIn, TOut>(
            this Result<TIn> result,
            Func<TIn, TOut> onSuccess,
            Func<IReadOnlyList<IError>, TOut> onFailure)
        {
            return result.IsSuccess ? onSuccess(result.Value) : onFailure(result.Errors);
        }
    }
}
