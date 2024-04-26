package com.telos.spark.ml;

import com.telos.spark.data.ArangoDataframeLoader;
import com.telos.spark.data.MongoDataframeLoader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TelosMLExecutor {

    private final ArangoDataframeLoader arangoDataframeLoader;

    private final MongoDataframeLoader mongoDataframeLoader;

    public void execute() {
        log.info("Loading Movies data from ArangoDB...");
        Dataset<Row> moviesDf = this.arangoDataframeLoader.load();
        moviesDf.printSchema();
        moviesDf.show(5);

        log.info("Loading People data from MongoDB...");
        Dataset<Row> peopleDf = this.mongoDataframeLoader.load();
        peopleDf.printSchema();
        peopleDf.show(5);
    }
}
