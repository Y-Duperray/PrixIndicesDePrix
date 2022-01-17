package Reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

public class HDFSReader implements Supplier<LocatedFileStatus> {
    String inputPathStr = "";
    SparkSession sparkSession;
    public HDFSReader(String inputFile, SparkSession session){
        this.sparkSession = session;
        this.inputPathStr = inputFile;
    }
    @Override
    public LocatedFileStatus get() {
        LocatedFileStatus fileStatus = null;
        try{
            FileSystem fs = FileSystem.get(new Configuration());
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(inputPathStr), true);
            while(iterator.hasNext()){
                fileStatus = iterator.next();
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return fileStatus;
    }
}
