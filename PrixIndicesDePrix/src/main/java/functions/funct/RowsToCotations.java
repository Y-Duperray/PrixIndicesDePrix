package functions.funct;

import functions.Cotation;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class RowsToCotations implements Function<Dataset<Row>, Dataset<Cotation>> {
    SparkSession sparkSession;
    public RowsToCotations(SparkSession session){
        System.out.println("New RowsToCotations created. ");
        this.sparkSession = session;
    }
    @Override
    public Dataset<Cotation> apply(Dataset<Row> lines) {
        System.out.println("RowsToCotations.apply()");
        List<Cotation> liste = new ArrayList();
        lines.foreach(line -> {
            Cotation cotation = new Cotation(line);
            liste.add(cotation);
        });
        Dataset<Cotation> cotations = sparkSession.createDataset(liste, Encoders.bean(Cotation.class));

        return cotations;
    }
}
