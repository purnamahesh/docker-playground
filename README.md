# PySpark Lab

1. install Docker / Podman
2. ```bash cd pyspark```
3. ```bash docker compose up -d scale=spark-worker=4 ```
    **Note:** Scale based on resource availability. The worker memory was set to 2g in **compose.yml** modify it as you wish. 
4. Lauch spark master UI at ```localhost:8080```, spark history server at ```localhost:18080```, rustfs UI at ```localhost:9001```
5. Launch jupyter notebook at ```localhost:8888``` and working directory is in ```pyspark/work_dir```
6. Credientials and other configuration can be found in ```compose.yml```