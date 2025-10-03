# See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
USER $APP_UID
WORKDIR /app

# This stage is used to build the service project
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Mti.ProductManagement.Services.Api/Mti.ProductManagement.Services.Api.csproj", "Mti.ProductManagement.Services.Api/"]
COPY ["Mti.ProductManagement.Application/Mti.ProductManagement.Application.csproj", "Mti.ProductManagement.Application/"]
COPY ["Mti.ProductManagement.Messaging/Mti.ProductManagement.Messaging.csproj", "Mti.ProductManagement.Messaging/"]
COPY ["Mti.ProductManagement.Persistance/Mti.ProductManagement.Persistance.csproj", "Mti.ProductManagement.Persistance/"]
COPY ["Mti.ProductManagement.Domain/Mti.ProductManagement.Domain.csproj", "Mti.ProductManagement.Domain/"]
RUN dotnet restore "./Mti.ProductManagement.Services.Api/Mti.ProductManagement.Services.Api.csproj"
COPY . .
WORKDIR "/src/Mti.ProductManagement.Services.Api"
RUN dotnet build "./Mti.ProductManagement.Services.Api.csproj" -c $BUILD_CONFIGURATION -o /app/build

# This stage is used to publish the service project to be copied to the final stage
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Mti.ProductManagement.Services.Api.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# This stage is used in production or when running from VS in regular mode (Default when not using the Debug configuration)
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Mti.ProductManagement.Services.Api.dll"]