package main.java.sources;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import main.java.datamodels.SalesRecord;
import scala.xml.PrettyPrinter;


public class SalesRecordsFileSource {
    // Defining FieldType we will use as input and output for function
    // It's always good to model input data completely, Even if we use few columns
    // This requires more data validation but for this example we can assume data is perfect.
    private static TypeInformation[] inputFieldTypes = new TypeInformation[]{
            Types.STRING,
            Types.STRING,
            Types.STRING,
            Types.STRING,
            Types.STRING,
            Types.STRING,
            Types.LONG,
            Types.STRING,
            Types.INT,
            Types.FLOAT,
            Types.FLOAT,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
    };


    /**
     * Reads the csv file and returns each row as SalesRecord.
     *
     * @param env     The execution environment.
     * @param csvFile The path of the CSV file to read.
     * @return A DataStream of SalesRecord events.
     */
    public static DataStream<SalesRecord> getRecords(StreamExecutionEnvironment env, String csvFile) {
        // Defining Input format and parameters for csv parsing.
        RowCsvInputFormat inputFormat = new RowCsvInputFormat(
                null,
                inputFieldTypes,
                "\n",
                ",");
        inputFormat.setSkipFirstLineAsHeader(true);
        DataStream<Row> parsedRows = env.readFile(inputFormat,csvFile)
                .returns(Types.ROW(inputFieldTypes))
                .setParallelism(1);

        // Convert parsed row to custom defined model and return
        return parsedRows.map(new RowToSalesRecordMapper());

    }
    public static class RowToSalesRecordMapper extends RichMapFunction<Row, SalesRecord>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

    @Override
    public SalesRecord map(Row row) throws Exception{
        SalesRecord record = new SalesRecord();
        record.setRegion((String)row.getField(0));
        record.setCountry((String) row.getField(1));
        record.setTotalRevenue((double) row.getField(11));
        return record;
    }
    }
}

