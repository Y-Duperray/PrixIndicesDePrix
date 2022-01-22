package Writer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class HBaseWriter implements Consumer<Dataset<Row>> {
    private final String catalogPath;

    public HBaseWriter(String path) {
        this.catalogPath = path;
    }

    @Override
    public void accept(Dataset<Row> ds) {
        String catalog = Files.lines(Paths.get(catalogPath)).map(String::trim).collect(Collectors.joining(""));

        ds.write()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .option(HBaseTableCatalog.newTable(), "5")
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .save();
    }
}
