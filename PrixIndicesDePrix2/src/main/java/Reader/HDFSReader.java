package Reader;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Supplier;

@Slf4j
public class HDFSReader implements Supplier<Dataset<Row>> {
    private FileSystem hdfs;
    private final String inputPathStr;

    public HDFSReader(String inputFilePath){
        System.out.println("New FileReader created");
        this.inputPathStr = inputFilePath;
    }

    @Override
    public Dataset<Row> get() {
        System.out.println("FileReader.get()");
        try {
            hdfs = FileSystem.get(new Configuration());
            if(hdfs.exists(new Path(inputPathStr))){
                FSDataInputStream fsDataInputStream = hdfs.open(new Path(inputPathStr));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
