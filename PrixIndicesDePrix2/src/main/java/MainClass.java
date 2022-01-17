import Functions.Refactor;
import Reader.FileReader;
import Writer.FileWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class MainClass {
    public static void main(String[] args){
        log.info("Starting program. ");

        Config config = ConfigFactory.load();
        String inputPathStr = config.getString("3il.path.input");
        String outputPathStr = config.getString("3il.path.output");

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        FileReader fileReader = new FileReader(inputPathStr, sparkSession);
        Dataset<Row> loadedDatas = fileReader.get();
        loadedDatas.show();

        Refactor refactor = new Refactor(sparkSession);
        Dataset<Float> processedDatas = refactor.apply(loadedDatas);

        FileWriter fileWriter = new FileWriter(outputPathStr);
        fileWriter.accept(processedDatas);
    }
}
