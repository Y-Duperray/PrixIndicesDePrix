package Readers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class HDFSReader implements Supplier<Dataset<Row>> {
    private FileSystem hdfs;
    private String inputPathStr;
    private SparkSession sparkSession;

    public HDFSReader(String path, SparkSession session){
        try{
            this.hdfs = FileSystem.get(new Configuration());
        }catch(Exception e){
            e.printStackTrace();
        }
        this.inputPathStr = path;
        this.sparkSession = session;
    }

    @Override
    public Dataset<Row> get() {
        try {
            if(hdfs.exists(new Path(inputPathStr))){
                StringWriter writer = new StringWriter();
                FSDataInputStream inputStream = hdfs.open(new Path(inputPathStr));
                IOUtils.copy(inputStream, writer);
                List<String> lines = Arrays.stream(writer.toString() .split("\n")).filter(l -> !l.startsWith("\"")).collect(Collectors.toList());
                Dataset<Row> ds = sparkSession.createDataset(lines, Encoders.STRING()).toDF();
                return ds;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
