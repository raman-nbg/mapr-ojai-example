package singh;

import com.mapr.db.MapRDB;
import org.ojai.Document;
import org.ojai.store.*;

import java.util.Iterator;
import java.util.UUID;
import java.util.stream.StreamSupport;

public class App {
    private static final String TABLE_NAME = "/tables/my_first_json_table";
    private final Connection connection;
    private final DocumentStore store;

    public static void main(String[] args) {
        App app = new App();
        app.read();
    }

    private App() {
        connection = DriverManager.getConnection("ojai:mapr:");

        if (!MapRDB.tableExists(TABLE_NAME)) {
            MapRDB.createTable(TABLE_NAME);
        }

        store = connection.getStore(TABLE_NAME);
    }

    private void write() {
        Document doc = connection.newDocument();
        doc.set("_id", UUID.randomUUID().toString())
                .set("name", "Hans");

        store.insert(doc);
    }

    private void read() {
        Query query = connection.newQuery().limit(1).build();

        System.out.println("Executing query...");
        QueryResult queryResult = store.find(query);
        System.out.println("Executed query");

        StreamSupport.stream(queryResult.spliterator(), false).forEach(document -> {
            String json = document.asJsonString();
            System.out.println(json);
        });
    }
}