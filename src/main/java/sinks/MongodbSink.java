package main.java.sinks;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import main.java.datamodels.CountryRevenue;
import com.mongodb.MongoSecurityException;

import org.bson.Document;


public class MongodbSink extends RichSinkFunction<CountryRevenue>{
    private final String url;
    private final String user;
    private final String password;
    private final String database;
    private final String collection;

    private static final ReplaceOptions REPLACE_OPTIONS = ReplaceOptions.createReplaceOptions(new UpdateOptions().upsert(true));


    private MongoClient mongoClient = null;
    private MongoDatabase mongoDatabase = null;
    private MongoCollection<Document> mongoCollection = null;


    public MongodbSink(String url, String user,String password,String database,String collection) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.database = database;
        this.collection = collection;
    }

    @Override
    public void invoke(CountryRevenue countryRevenue,Context ctx) throws Exception {
    System.out.println("Invoked with");

        Document doc = new Document("_id",countryRevenue.getCountry()).append("country", countryRevenue.getCountry())
            .append("revenue", countryRevenue.getTotalRevenue());
    mongoCollection.replaceOne(new BasicDBObject("_id", countryRevenue.getCountry()),doc,REPLACE_OPTIONS);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialise client
        try {
            MongoCredential credential = MongoCredential.createCredential(user, database, password.toCharArray());
            ConnectionString connectionString = new ConnectionString(url);
            MongoClientSettings settings = MongoClientSettings.builder()
                    .credential(credential)
                    .applyConnectionString(connectionString)
                    .build();
            mongoClient = MongoClients.create(settings);
            mongoDatabase = mongoClient.getDatabase(database);
            mongoCollection = mongoDatabase.getCollection(collection);

        }
        catch (MongoSecurityException ex){
            System.out.println("Could not establish connection with MongoDB. Please check security credentials.");
        }
    }

    @Override
    public void close() throws Exception {
        System.out.println("closing the console");
        if (mongoClient!=null){
            mongoClient.close();
        }

    }

}
