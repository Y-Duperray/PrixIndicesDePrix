package functions.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.File;
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
        File outputFile = new File(outputDirectory+"/resultat.csv");
        if(outputFile.exists()){
            dataset.write().format("csv").mode(SaveMode.Overwrite).save(outputDirectory+"/resultat.csv");
        }else{
            dataset.write().format("csv").save(outputDirectory+"/resultat.csv");
        }
    }
}
