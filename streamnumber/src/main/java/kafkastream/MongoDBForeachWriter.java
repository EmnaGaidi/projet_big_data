package kafkastream;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDBForeachWriter extends ForeachWriter<Row> {

    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> collection;
    private long batchId;

    @Override
    public boolean open(long partitionId, long version) {
        // Connexion à MongoDB
        mongoClient = MongoClients.create("mongodb://localhost:27017/");
        // Sélection de la base de données
        database = mongoClient.getDatabase("BigData_Project");
        // Sélection de la collection
        collection = database.getCollection("streamKafka");
        return true;
    }

    @Override
    public void process(Row value) {
        // Création d'un document à insérer dans la collection MongoDB
        Document document = new Document("artist", value.getString(0))
                                .append("max_streams", value.getInt(1))
                                .append("track_name", value.getString(2))
                                .append("batch_id",batchId);
        // Insertion du document dans la collection
        collection.insertOne(document);
    }

    @Override
    public void close(Throwable errorOrNull) {
        // Fermeture de la connexion à MongoDB
        mongoClient.close();
    }
    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }
}

