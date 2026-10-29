# PySpark Lab

1. install Docker / Podman
2. Uncommment line 11 (```RUN wget```) and comment line 14 (```COPY ./```) in ```pyspark/Dockerfile```
3. ```cd pyspark```
4. ```docker compose up -d scale=spark-worker=4 ```
    **Note:** Scale based on resource availability. The worker memory was set to 2g in **compose.yml** modify it as you wish. 
5. Lauch spark master UI at ```localhost:8080```, spark history server at ```localhost:18080```, rustfs UI at ```localhost:9001```
6. Launch jupyter notebook at ```localhost:8888``` and working directory is in ```pyspark/work_dir```
7. Credientials and other configuration can be found in ```compose.yml```