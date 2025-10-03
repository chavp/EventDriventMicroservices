using FluentResults;
using Processing.Guideline.Contracts;
using static Processing.Guideline.Domain.Errors;

namespace Processing.Guideline.Applications
{
    public class ValidationService
    {
        public async Task<Result<DataResponse>> ValidationProcess(DataRequest request)
        {
            var errors = new List<Error>();
            // pre-processiong
            if (string.IsNullOrEmpty(request.Request1))
                errors.Add(new RequiredError("Request1", "Required request1", "REQ_REQUEST1"));

            if (string.IsNullOrEmpty(request.Request2))
                errors.Add(new RequiredError("Request2", "Required request2", "REQ_REQUEST2"));

            if (request.Request3 > request.Request4)
                errors.Add(new RangeError("Request3", "Request4", "Can't process Req3 > Req4", "REQ_REQUEST1"));

            Result<DataResponse> result = Result.FailIfNotEmpty(errors);
            if(result.IsFailed) return result;

            // processing
            var resp = new DataResponse
            {
                Response1 = request.Request2,
                Response2 = request.Request1,
                Response3 = request.Request3 + request.Request4
            };

            return Result.Ok(resp);
        }
    }
}
