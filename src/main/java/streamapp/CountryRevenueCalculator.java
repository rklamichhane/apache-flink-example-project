package main.java.streamapp;

import main.java.datamodels.CountryRevenue;
import main.java.datamodels.SalesRecord;
import main.java.sinks.MongodbSink;
import main.java.sources.SalesRecordsFileSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.api.java.utils.ParameterTool;
import scala.xml.PrettyPrinter;

import java.io.InputStream;
import java.net.URI;


public class CountryRevenueCalculator {

    public static void main(String[] args) throws Exception {

        //Variables initialization, parsing configs for later use
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.get("input") == null){
            System.out.println("Please provide input file with --input. Exiting program");
            System.exit(-1);
        }
        if (params.get("config") == null){
            System.out.println("Please provide config file with --config. Exiting program");
            System.exit(-1);
        }

        final ParameterTool config = ParameterTool.fromPropertiesFile(params.get("config"));

        final String inputPath = params.get("input");
//
//        System.out.println(params.get("config"));
//        System.out.println(params.get("mapping"));
//        System.out.println(inputPath);

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // Checkpoint configuration
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // making sure 500 ms of difference between two consecutive checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        /* Creating Flink stream source from csv file.
           We are simply ignoring Event-time for given dataset as there is no reliable time data and seems random.
           so we assume a record comes in long time span like a day and we process it and store/update result
           in custom MongoDB sink.
           We can use tumbling-window on stream based on process-time or count to minimize database hit but for this
           example we can just proceed with this.

           Here in the source we convert the incoming stream i.e csv records to SalesRecord POJO model so that our
           app becomes free of parsing logic.

         */
        DataStream<SalesRecord> records = SalesRecordsFileSource.getRecords(env, inputPath);

        DataStream<CountryRevenue> groups = records
                .keyBy(r -> r.getCountry())
                // We can partition the stream by the country as we are concerned with total revenue of each country
                // monitor events and aggregate revenue
                .process(new CalculateCountryRevenue());

        groups.addSink(new MongodbSink(config.get("db.url"),config.get("db.user"),
                config.get("db.password"),config.get("db.database"),config.get("db.collection")));
        env.execute("Flink Test Example");

    }
    public static class CalculateCountryRevenue extends KeyedProcessFunction<String, SalesRecord, CountryRevenue>{
        /* Defining single value state so that flink manages checkpointing this variable for failure tolerance.
        We update this value when a record comes.
         */
        ValueState<Double> totalRevenue;
        @Override
        public void open(Configuration conf){
            totalRevenue  = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("totalRevenue",Types.DOUBLE));

        }

        @Override
        public void processElement(SalesRecord record, Context ctx, Collector<CountryRevenue> out ) throws Exception {

            /*
            Accumulating revenue from record and returning CountryRevenue POJO
             */
            Double prevTotal = totalRevenue.value();
            if (prevTotal == null){
                prevTotal = 0.0;
            }
            prevTotal += record.getTotalRevenue();
            totalRevenue.update(prevTotal);
            out.collect(new CountryRevenue(record.getCountry(),prevTotal));

        }
    }
}
