Executar o comando dentro do diretório "Docker" - teste01

<Comando para criar a imagem>
docker build -t docker_dh_image .
---------------------------------------------------------
<Comando para listar imagens docker>
docker images
---------------------------------------------------------
<Comando para rodar e entrar no container da imagem>
docker run -it 11821a430bc6 /bin/sh
---------------------------------------------------------
Dentro do container
<Comando para entrar no diretório da aplicação>
cd api-dh-v2
---------------------------------------------------------
<Comando para iniciar a aplicação>
sh meudesconto_api.sh start dev
---------------------------------------------------------
<Comando para ver o log da aplicação>
cat nohup.out

=========================================================

Resumo - print dos comandos em um terminal Windows

Microsoft Windows [versão 10.0.19041.867]
(c) 2020 Microsoft Corporation. Todos os direitos reservados.

C:\Users\resource\Documents\GPA\Sprint-35\Poc_DH_Docker\Docker>docker build -t docker_dh_image .
[+] Building 7.3s (11/11) FINISHED
 => [internal] load build definition from Dockerfile                                                               0.7s
 => => transferring dockerfile: 31B                                                                                0.0s
 => [internal] load .dockerignore                                                                                  0.9s
 => => transferring context: 2B                                                                                    0.0s
 => [internal] load metadata for docker.io/library/python:3                                                        2.2s
 => [auth] library/python:pull token for registry-1.docker.io                                                      0.0s
 => [internal] load build context                                                                                  0.6s
 => => transferring context: 5.20kB                                                                                0.0s
 => [1/5] FROM docker.io/library/python:3@sha256:168fd55b03929f88cd3e1e05b9ebe8f9cc1c095af8b53a8c0cd50da04a8c3a40  0.0s
 => CACHED [2/5] WORKDIR /home/ec2-user                                                                            0.0s
 => CACHED [3/5] COPY requirements.txt ./                                                                          0.0s
 => CACHED [4/5] RUN pip install --no-cache-dir -r requirements.txt                                                0.0s
 => [5/5] COPY . .                                                                                                 1.3s
 => exporting to image                                                                                             1.8s
 => => exporting layers                                                                                            0.8s
 => => writing image sha256:11821a430bc6452ffcd381d03983444af6c970776d04951ea29d86cadd2698d1                       0.1s
 => => naming to docker.io/library/docker_dh_image                                                                 0.1s

C:\Users\resource\Documents\GPA\Sprint-35\Poc_DH_Docker\Docker>docker images
REPOSITORY               TAG       IMAGE ID       CREATED          SIZE
docker_dh_image          latest    11821a430bc6   22 seconds ago   1.19GB
poc-dh-gpa               latest    a62d5c1e25a2   9 days ago       1.2GB
portainer/portainer-ce   latest    96a1c6cc3d15   6 weeks ago      209MB

C:\Users\resource\Documents\GPA\Sprint-35\Poc_DH_Docker\Docker>docker run -it 11821a430bc6 /bin/sh
# cd api-dh-v2
# sh meudesconto_api.sh start dev
dev
meudesconto_api.sh: 23: cd: can't cd to /home/ec2-user/api-dh
starting dev server at 9999
starting dev server at 9998
# nohup: nohup: appending output to 'nohup.out'appending output to 'nohup.out'



