package org.example;
import com.datastax.astra.client.Collection;
import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.Database;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        //MongoDatabase mongodb = connectToMongo();
        //Jedis redis = connectToRedis();
        Database cassandra = connectToCassandra();
        System.out.println("hola");

    }
    private static MongoDatabase connectToMongo() {
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            MongoDatabase database = mongoClient.getDatabase("Clase_04");
            database.createCollection("coleccion");
            createOneMongo(database);
            updateOneMongo(database);
            System.out.println(findOneMongo(database));
            deleteOneMongo(database);
            return database;
        } catch (Exception e) {
            System.err.println("Error connecting to the MongoDB database: " + e.getMessage());
            return null;
        }
    }
    private static Jedis connectToRedis() {
        try (Jedis jedis = new Jedis("localhost")) {
            System.out.println("Server is running: " + jedis.ping());
            createOrUpdateOneRedis(jedis);
            findOneRedis(jedis);
            deleteOneRedis(jedis);
            return jedis;
        } catch (Exception e) {
            System.err.println("Error connecting to Redis: " + e.getMessage());
            return null;
        }
    }
    private static Database connectToCassandra(){
        DataAPIClient client = new DataAPIClient("AstraCS:GbpeEUCNjOhAegZchKuHtXzS:4d5d7f4039723abb730393b72a1165e1e4adbe0fab8bfbe480b8b477c1577b4a");
        Database db = client.getDatabase("https://1ad6a7b8-e074-44f9-80c2-09de36c403f9-us-east-2.apps.astra.datastax.com", "parcial");
        System.out.println("Connected to AstraDB " + db.getNamespaceName());
        db.createCollection("miTabla");
        Collection<com.datastax.astra.client.model.Document> tabla = db.getCollection("miTabla");
        createOneCassandra(tabla);
        //updateOneCassandra(tabla);
        //findOneCassandra(tabla);
        //deleteOneCassandra(tabla);
        return db;
    }
    private static void createOneMongo(MongoDatabase database){
        MongoCollection<Document> miColeccion = database.getCollection("coleccion");
        Document document = new Document("name", "John")
                .append("age", 30)
                .append("city", "New York");
        miColeccion.insertOne(document);
    }
    private static void updateOneMongo(MongoDatabase database) {
        MongoCollection<Document> miColeccion = database.getCollection("coleccion");
        Document filter = new Document("name", "John");
        Document update = new Document("$set", new Document("age", 35));
        miColeccion.updateOne(filter, update);
    }
    private static Document findOneMongo(MongoDatabase database) {
        MongoCollection<Document> miColeccion = database.getCollection("coleccion");
        Document filter = new Document("name", "John");
        Document result = miColeccion.find(filter).first();
        return result;
    }
    private static void deleteOneMongo(MongoDatabase database){
        MongoCollection<Document> miColeccion = database.getCollection("coleccion");
        Document filter = new Document("name", "John");
        miColeccion.deleteOne(filter);
    }
    private static void createOrUpdateOneRedis(Jedis database){
        database.set("item", "Precio");
    }
    private static String findOneRedis(Jedis database) {
        String valor = database.get("item");
        System.out.println("Valor leído de Redis: " + valor);
        return valor;
    }
    private static void deleteOneRedis(Jedis database){
        database.del("item");
    }

    private static void deleteOneCassandra(Collection<com.datastax.astra.client.model.Document> tabla) {

    }

    private static void findOneCassandra(Collection<com.datastax.astra.client.model.Document> tabla) {
    }

    private static void updateOneCassandra(Collection<com.datastax.astra.client.model.Document> tabla) {
    }

    private static void createOneCassandra(Collection<com.datastax.astra.client.model.Document> tabla) {
        Document document = new Document("name", "John")
                .append("age", 30)
                .append("city", "New York");
        tabla.insertOne(com.datastax.astra.client.model.Document.create(document));
    }
}
