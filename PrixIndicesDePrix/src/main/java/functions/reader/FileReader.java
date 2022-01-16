package functions.reader;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

@Slf4j
public class FileReader implements Supplier<Dataset<Row>> {
    private final String inputFile;
    private final SparkSession sparkSession;

    public FileReader(String inputFilePath, SparkSession session){
        System.out.println("New FileReader created");
        this.inputFile = inputFilePath;
        this.sparkSession = session;
    }

    @Override
    public Dataset<Row> get() {
        System.out.println("FileReader.get()");
        try{
            Dataset<Row> csv = sparkSession.read().option("delimiter", ";").option("header", "true").csv(inputFile);
            return csv;
        }catch(Exception e){
            log.info("FileReader : Exception : {}", e);
        }
        return null;
    }
}
