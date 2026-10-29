# Docker Deep Dive (2025)

## Questions?

WHat is WebAssembly?
WebGPU?
containerd
LABEL, ONBUILD, HEALTHCHECK


Servers -> VMs -> Containers -> Docker Containers -> Web Assembly

## Docker

- the cli (client)
	- docker build, push, run, scout, ...
- the engine (server)
	- docker daemon

## Container-related standards and projects

- the OCI (works under Linux Foundation)
	- image-spec (buildKit)
	- runtime-spec (runtime for containers)
	- distribution-spec (DockerHub)
- CNCF (under Linux Foundation)
	- Kubernetes
	- containerd
	- Notary?
	- Prometheus
	- Cilium?
	- etc,...
- The Moby Project

## Big Picture

#### Ops perpective
	- start, stop, delete container, exec cmds inside
#### Dev Perspective
	- app side
	- app source code, building it into image, and running

## Docker Engine
two parts:
	- the docker daemon - monolithic binary containing all the logic of buiding images, run containers, managing volumes, plugins, etc
		- getting slower
		- harder to main because of being monolethic
		- **runc**: for running containers
		- **containerd**: for building images
	- the LXC 
		Linux only
		Interacts with linux kernal for creating namesapces and cgroups
		*docker replaces this with libcontaier to be multi-platform

docker-cli -> docker daemon -> containerd -> runc & shim (parent of container ps)


## Docker Images

registory/<org|user>/repo/imagename:tag

### layers - readonly

digests

docker buildx imagetools <image>

dokcer pull registry/user|org/repo/name:digest


- images digests are a crypto hash of the imag's manifest file
- layer digests are a crypto hash of the layer contents

content hashes vs distribution hashes
- content hash: uncompressed
- distribution hash: compressed for storage in registory

### Multi-architecure images | Platforms
- manifest lists: list of architectures supported by the image tag
	- each supported architecture has it own manifest file

docker inspect <image>

docker buildx imagetools inspect <image>

docker manifest inspect <image>

using docker buildx we can build images of different architecture from any architecture machine
Two ways to achieve this:
	- Emulation (uses QEMU VM - works most of the time but slow and dont have shared cache)
	- Build cloud (subscription sevice by Docker Inc builds in the target architecture in the cloud)
		- integrates well with Docker Desktop and Github Actions
		- Teammates can use shared cache


### Vulnerability Scaning with Docker Scout (paid service)
scout.docker.com

install docker scout plugin
```bash
curl -sSfL https://raw.githubusercontent.com/docker/scout-cli/main/install.sh | sh -s --
```

docker scout quickview <image>

docker scout cves <image>

### delete docker images

docker rmi <image|short id|name|SHA>

deletes image from local. But keeps the layer cache.
docker prevents deleting when image is being used by any container

you can use `**-f**` flag to force delete the image even a container is running, but this will untag the container from the image and becomes a **dangling contianer**


## Docker Containers

Container are runtime instances of images. We can start/stop a container from an image.

- Images are read-only but conatiners are writable 
- Containers are statless and ephimeral while VMs are stateful and resumes where they are stopped
- Containers are meant to be immutable - mwans if one fails we shouuld create another in its place instead of connecting to it and making a live fix
- Containers should only run one process and we use them to build microservices

Containers vs VMs
- VMs virtualize the hardware
- Containers virtualize the OS


Images are read-only.
But images will creating a Read-write layer of the containers. Id an image update layer it'll ne stored by docker even the container is stopped and restored the changes will be there. These will be sropped once the container is deleted.:w

CMD & Entrpoint

### Connecting to containers
	- interactive
		- Connect to shell
		- docker exec -it webserver sh
	- remote-execution
		- Send commands to running container
		- docker exec webserver ps


`docker inspect <container name|id>`

### Docker debug
To shell into distroless / slim images
- changes made while debugging a live ccontainer are persisted
- changes made while debugging images / stopped contaienrs are not persisted


## Self-healing containers (restart policies)

apply restart policies per containers.

Docker supports four restart policies:
- no (default)
- no-failure
- always
- unless-stopped

|Restart Policy|Non-zero exit code|Zero exit code|Docker stop command|restart when daemon restarts|
|--------------|------------------|--------------|-------------------|----------------------------|
|no|N|N|N|N|
|on-failure|Y|N|N|Y|
|always|Y|Y|Y|Y|
|unless-stopped|Y|Y|N|N|


# Containerizing an App

- Instructions that add context like files, etc will create new layers (FROM, RUN, COPY, WORKDIR)

`docker build -t <tag> .`

`docker tag <old tag> <newtag>`

`docker history <image>`   -- to see instructions that created the image

## Multi stage builds - for production

- big means slow
- big means more vulnarabilities
- big means larger attach surface

`docker build -t multi:client --target prod-client -f Dockerfile`


`docker build -t multi:server --target prod-server -f Dockerfile`

# Buildx, BuildKit, drivers and Build Cloud


Docker build sytem has a client and server:
- Client: Buildx
- Server: BuildKit

`docker buildx ls --no-trunc`Goal: Implement LRU eviction with a max-capacity cache. Confront the hardest ownership challenge: a data structure with multiple access paths to the same data.

** Ensure instructions that are most likely to invalidate the cache should be at the bottomi

`--no-cache`

# Docker Compose

to manage Multi container apps






# Books
Quick Start Kubernetes
The Kubernetes Book 



#till page 216
