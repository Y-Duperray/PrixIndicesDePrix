package fr.iiil.bigdata.spark.reader;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.util.function.Supplier;

@Slf4j
public class HBaseReader implements Supplier<Dataset<Row>> {
    private final SparkSession sparkSession;
    private final String catalog;
    
    public HBaseReader(String catalog, SparkSession session){
        this.catalog = catalog;
        this.sparkSession = session;
    }

    @Override
    public Dataset<Row> get() {
        log.info("reading table from catalog={}", catalog);
        Dataset<Row> ds = sparkSession.emptyDataFrame();
        try {
            ds = sparkSession.read()
                    .option(HBaseTableCatalog.tableCatalog(), catalog)
                    .format("org.apache.spark.sql.execution.datasources.hbase")
                    .load();
        } catch (Exception e){
            e.printStackTrace();
        }
        return ds;
    }
}
