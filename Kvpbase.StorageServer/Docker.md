# Running Kvpbase in Docker

Keywords: docker dotnet httplistener http.sys http c#

Getting an ```HttpListener``` application (such as Kvpbase or any application using Watson Webserver, HTTP.sys, etc) up and running in Docker can be rather tricky given how 1) Docker acts as a network proxy and 2) HttpListener isn't friendly to ```HOST``` header mismatches.  Thus, it is **critical** that you run your containers using ```--user ContainerAdministrator``` to bypass the ```HttpListener``` restrictions.  There are likely ways around this, but I have been unable to find one.  

## Steps to Run Kvpbase in Docker

1) View and modify the ```Dockerfile``` as appropriate for your application.

2) Execute the Docker build process:
```
$ docker build -t kvpbase -f Dockerfile .
```

3) Verify the image exists:
```
$ docker images
REPOSITORY                              TAG                 IMAGE ID            CREATED             SIZE
kvpbase                                 latest              047e29f37f9c        2 seconds ago       328MB
mcr.microsoft.com/dotnet/core/sdk       3.1                 abbb476b7b81        11 days ago         737MB
mcr.microsoft.com/dotnet/core/runtime   3.1                 4b555235dfc0        11 days ago         327MB
```
 
4) Execute the container:
```
$ docker run -d -p 8000:8000 kvpbase 
```

5) Connect to Kvpbase in your browser: 
```
http://localhost:8000
```

6) Get the container name:
```
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED              STATUS              PORTS                    NAMES
3627b4e812fd        kvpbase             "dotnet Kvpbase.Stor…"   About a minute ago   Up About a minute   0.0.0.0:8000->8000/tcp   silly_khayyam
```

7) Kill a running container:
```
$ docker kill [CONTAINER ID]
```

8) Delete a container image:
```
$ docker rmi [IMAGE ID] -f
```
