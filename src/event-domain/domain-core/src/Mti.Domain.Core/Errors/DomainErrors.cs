using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.Domain.Core.Primitives;

namespace Mti.Domain.Core.Errors
{
    public static class DomainErrors
    {
        public static class General
        {
            public static Error UnProcessableRequest => new Error(
                "General.UnProcessableRequest",
                "The server could not process the request.");

            public static Error ServerError => new Error("General.ServerError", "The server encountered an unrecoverable error.");
        }
    }
}
