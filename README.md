# Back to Hadoop!
* Hudi = Hadoop Upserts and Incrementals
* If you don't know, I love Hadoop especially HDFS.
* This repository is my self study to explore Hudi features.
* This is absed on Hudi spark quick start.
    * https://hudi.apache.org/docs/quick-start-guide/

## Hudi setup
### Spark setup
```bash
pushd /tmp/
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar -xzvf spark-3.3.2-bin-hadoop3.tgz
sudo mv spark-3.3.2-bin-hadoop3 /opt/spark-3.3.2
echo 'export SPARK_HOME=/opt/spark-3.3.2' >> ~/.bashrc
rm -rf spark-3.3.2-bin-hadoop3.tgz
popd
```

### Java setup
```bash
# We are going with Java 11 this time.
sudo apt-get install openjdk-11-jdk
# /usr/lib/jvm/java-11-openjdk-amd64/bin/java
# /usr/lib/jvm/java-17-openjdk-amd64/bin/java
# /usr/lib/jvm/jdk1.8.0_421/bin/java
sudo update-alternatives --set java /usr/lib/jvm/java-11-openjdk-amd64/bin/java
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
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz ;
# Extract it
tar -xzvf hadoop-3.4.0.tar.gz ;
sudo mv hadoop-3.4.0 /opt/hadoop-3.4.0 ;
sudo ln -s /opt/hadoop-3.4.0 /opt/hadoop ;

# Add Hadoop to path
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc
echo 'export SPARK_DIST_CLASSPATH=$(hadoop classpath)' >> ~/.bashrc
# Restart shell or source .bashrc
source ~/.bashrc
```

### Jupyter Scala kernel
```bash
cs launch --use-bootstrap almond --scala 2.12.15 -- --install --force
```

### Hudi jar
```bash
wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3-bundle_2.12/1.0.0/hudi-spark3-bundle_2.12-1.0.0.jar
wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-hive_2.12/3.3.2/spark-hive_2.12-3.3.2.jar
# For CustomMergeIntoConnector
wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/apache/hudi/hudi-common/1.0.0/hudi-common-1.0.0.jar
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
        javac -cp "${SPARK_HOME}/jars/*:" -d out -sourcepath src $(find src/main/java -name "*.java")
        jar cf CustomMergeIntoConnector.jar -C out .
        ```
    * Add to the classpath.
        ```scala
        import $cp.`hudi/CustomMergeIntoConnector.jar`
        ```


## Todo
* MERGE_ON_READ table vs Copy-On-Write.
* https://hudi.apache.org/docs/table_types/