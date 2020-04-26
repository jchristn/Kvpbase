#
# Built using Docker Desktop with Windows containers
# See: https://docs.docker.com/engine/examples/dotnetcore/
#

# Use SDK
FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build

# Set the local working directory, copy in the project file, and perform a restore
WORKDIR /app
COPY *.csproj .
RUN dotnet restore

# Copy everything else and build
COPY . ./
RUN dotnet build   -f netcoreapp3.1 -c Release
RUN dotnet publish -f netcoreapp3.1 -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/core/runtime:3.1
WORKDIR /app
EXPOSE 8000/tcp
COPY --from=build /app/out .

# If you have a System.json file or a Kvpbase.db, be sure to copy them in by 
# uncommenting the following lines.
# COPY ./System.json .
# RUN mkdir Logs
# RUN mkdir Storage

# Set the entrypoint for the container
ENTRYPOINT ["dotnet", "Kvpbase.StorageServer.dll"]