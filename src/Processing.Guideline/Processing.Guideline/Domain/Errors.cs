using FluentResults;

namespace Processing.Guideline.Domain
{
    public static class Errors
    {
        public abstract class CodeError : Error
        {
            public string Code { get; }
            public CodeError(string code, string message)
                : base(message)
            {
                Code = code;
            }
        }

        public class RequiredError : CodeError
        {
            public string PropertyName { get; }

            public RequiredError(string propertyName, string message, string errorCode)
                : base(errorCode, message)
            {
                PropertyName = propertyName;
            }
        }

        public class RangeError : CodeError
        {
            public string PropertyName1 { get; }
            public string PropertyName2 { get; }

            public RangeError(string propertyName1, string propertyName2, string message, string errorCode)
                : base(errorCode, message)
            {
                PropertyName1 = propertyName1;
                PropertyName2 = propertyName2;
            }
        }

    }
}
