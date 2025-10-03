# See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081


# This stage is used to build the service project
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Mti.PartyManagement.Services.Api/Mti.PartyManagement.Services.Api.csproj", "Mti.PartyManagement.Services.Api/"]
COPY ["Mti.PartyManagement.Domain/Mti.PartyManagement.Domain.csproj", "Mti.PartyManagement.Domain/"]
COPY ["Mti.PartyManagement.Persistence/Mti.PartyManagement.Persistence.csproj", "Mti.PartyManagement.Persistence/"]
RUN dotnet restore "./Mti.PartyManagement.Services.Api/Mti.PartyManagement.Services.Api.csproj"
COPY . .
WORKDIR "/src/Mti.PartyManagement.Services.Api"
RUN dotnet build "./Mti.PartyManagement.Services.Api.csproj" -c $BUILD_CONFIGURATION -o /app/build

# This stage is used to publish the service project to be copied to the final stage
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Mti.PartyManagement.Services.Api.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# This stage is used in production or when running from VS in regular mode (Default when not using the Debug configuration)
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Mti.PartyManagement.Services.Api.dll"]