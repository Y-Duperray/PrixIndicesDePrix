package functions.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Consumer;

@Slf4j
public class FileWriter implements Consumer<Dataset<Row>> {
    private final String outputDirectory;
    public FileWriter(String outputDirectoryPath){
        this.outputDirectory = outputDirectoryPath;
    }

    @Override
    public void accept(Dataset<Row> dataset) {
        System.out.println("FileWriter.accept()");
        dataset.coalesce(1).write().mode("overwrite").csv(outputDirectory);
    }
}
