# Apache-Sedona-Spatial-Analytics
The following instructions will help you get up and running with Apache Sedona, a distributed computing framework for geospatial analytics. To learn more about Sedona, check out the **[documentation](https://docs.cloudera.com/machine-learning/cloud/index.html)**.

### Sedona Setup
To use Sedona with PySpark, you will need to download the package from PyPI. This can be done with a pip install:
```
pip install apache-sedona
``` 

You may notice that Sedona has a dependency on PySpark, which can cause confusion, especially when running Spark in a containerized environment. You can run the attached requirements file to install Sedona without PySpark
```
pip install -r requirements.txt --no-deps
```