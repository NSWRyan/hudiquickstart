# Back to Hadoop!
* Hudi = Hadoop Upserts and Incrementals
* If you don't know, I love Hadoop especially HDFS.
* This repository is my self study to explore Hudi features.
* This is based on Hudi spark quick start.
    * https://hudi.apache.org/docs/quick-start-guide/

## Environment setup
### Spark setup
```bash
pushd /tmp/
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar -xzvf spark-3.3.2-bin-hadoop3.tgz
sudo mv spark-3.3.2-bin-hadoop3 /opt/spark-3.3.2
sudo ln -s /opt/spark-3.3.2 /opt/spark
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
rm -rf spark-3.3.2-bin-hadoop3.tgz
popd
```

### Java setup
```bash
# We are going with Java 11 this time.
sudo apt-get install openjdk-11-jdk
# Switch to openjdk-11-jdk.
sudo update-alternatives --config java 
sudo update-alternatives --config javac 
java -version
export JAVA_HOME=$(realpath $(dirname $(realpath $(which java)))/..)
```

### Scala setup
```bash
cs install scala:2.12.15 scalac:2.12.15
cs install scalafmt
scala -version
```

### Hadoop setup
```bash
# Download Hadoop release.
pushd /tmp/
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz ;
# Extract it
tar -xzvf hadoop-3.4.0.tar.gz ;
sudo mv hadoop-3.4.0 /opt/hadoop-3.4.0 ;
sudo ln -s /opt/hadoop-3.4.0 /opt/hadoop ;
rm -rf hadoop-3.4.0.tar.gz

# Add Hadoop to path
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc
echo 'export SPARK_DIST_CLASSPATH=$(hadoop classpath)' >> ~/.bashrc
# Restart shell or source .bashrc
source ~/.bashrc
rm -rf 
popd
```

### Jupyter Scala kernel
```bash
cs launch --use-bootstrap almond --scala 2.12.15 -- --install --force
```

### Hudi jar
```bash
wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3-bundle_2.12/1.0.0/hudi-spark3-bundle_2.12-1.0.0.jar
wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-hive_2.12/3.3.2/spark-hive_2.12-3.3.2.jar
```

## Prework.
* The get started code requires CustomMergeIntoConnector to work on the "Merging Data" section.
* To fix this do:
    * https://github.com/apache/hudi/issues/12777
    * https://gist.github.com/bhasudha/7ea07f2bb9abc5c6eb86dbd914eec4c6
    * Download or just use the one in this repository.
    * To compile the library, we also need org.apache.hudi:hudi-common.
    * Make sure the library is in the class path.
    * Run:
        ```bash
        javac -cp "${SPARK_HOME}/jars/avro-1.11.0.jar:${SPARK_HOME}/jars/hudi-spark3-bundle_2.12-1.0.0.jar:" \
        -d out -sourcepath src $(find src/main/java -name "*.java") ;
        jar cf CustomMergeIntoConnector.jar -C out .
        ```
    * Add to the classpath.
        ```scala
        import $cp.`/path/to/CustomMergeIntoConnector.jar`
        ```


## Todo
* MERGE_ON_READ table vs Copy-On-Write.
* https://hudi.apache.org/docs/table_types/