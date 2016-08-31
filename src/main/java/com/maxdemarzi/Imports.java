package com.maxdemarzi;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.FileReader;
import java.io.Reader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Path("/import")
public class Imports {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    protected static final Map<String, Node> occupationNodeMap = new HashMap<>();
    protected static final Map<String, Node> titleNodeMap = new HashMap<>();

    @POST
    @Path("/occupations/{filename : (.+)?}")
    public Response importOccupations(@PathParam("filename") String filename, @Context GraphDatabaseService db) throws Exception {
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
        for (CSVRecord record : records) {
            try (Transaction tx = db.beginTx()) {
                count++;
                Node occupation = db.createNode(Labels.Occupation);

                occupation.setProperty("code", record.get("O*NET-SOC Code"));
                occupation.setProperty("title", record.get("Title"));
                occupation.setProperty("description", record.get("Description"));

                Node title = db.createNode(Labels.Title);
                title.setProperty("name", record.get("Title"));
                Relationship relationship =  occupation.createRelationshipTo(title, RelationshipTypes.HAS_TITLE);
                relationship.setProperty("primary", true);

                tx.success();
            }
        }
        Map<String, Object> results = new HashMap<>();
        results.put("Imported Occupations Count", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @POST
    @Path("/titles/{filename : (.+)?}")
    public Response importTitlesNaive(@PathParam("filename") String filename, @Context GraphDatabaseService db) throws Exception {
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        for (CSVRecord record : records) {
            try (Transaction tx = db.beginTx()) {
                count++;
                Node occupation = db.findNode(Labels.Occupation, "code", record.get("O*NET-SOC Code"));
                if (occupation != null) {
                    Node title = db.findNode(Labels.Title, "name", record.get("Alternate Title"));
                    if (title == null ) {
                        title = db.createNode(Labels.Title);
                        title.setProperty("name", record.get("Alternate Title") );
                    }

                    occupation.createRelationshipTo(title, RelationshipTypes.HAS_TITLE);
                }

                if (count % 1000 == 0) {
                    System.out.println("Imported " + count + " at " + new Date());
                }
                tx.success();
            }
        }

        Map<String, Object> results = new HashMap<>();
        results.put("Imported Titles Count", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @POST
    @Path("/titles_batched/{filename : (.+)?}")
    public Response importTitlesBatched(@PathParam("filename") String filename, @Context GraphDatabaseService db) throws Exception {
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
        Transaction tx = db.beginTx();
        try {
            for (CSVRecord record : records) {
                count++;
                Node occupation = db.findNode(Labels.Occupation, "code", record.get("O*NET-SOC Code"));
                if (occupation != null) {
                    Node title = db.findNode(Labels.Title, "name", record.get("Alternate Title"));
                    if (title == null ) {
                        title = db.createNode(Labels.Title);
                        title.setProperty("name", record.get("Alternate Title") );
                    }

                    occupation.createRelationshipTo(title, RelationshipTypes.HAS_TITLE);
                }
                if (count % 1000 == 0) {
                    System.out.println("Imported " + count + " at " +  new Date());
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
                tx.success();
            }
            tx.success();
        } finally {
            tx.close();
        }


        Map<String, Object> results = new HashMap<>();
        results.put("Imported Titles Count", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @POST
    @Path("/titles_cache_occupations/{filename : (.+)?}")
    public Response importTitlesCachedOccupations(@PathParam("filename") String filename, @Context GraphDatabaseService db) throws Exception {
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
        Transaction tx = db.beginTx();
        try {
            for (CSVRecord record : records) {
                count++;
                Node occupation = getOccupation(db, record.get("O*NET-SOC Code"));
                if (occupation != null) {
                    Node title = db.findNode(Labels.Title, "name", record.get("Alternate Title"));
                    if (title == null ) {
                        title = db.createNode(Labels.Title);
                        title.setProperty("name", record.get("Alternate Title") );
                    }

                    occupation.createRelationshipTo(title, RelationshipTypes.HAS_TITLE);
                }
                if (count % 1000 == 0) {
                    System.out.println("Imported " + count + " at " +  new Date());
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
                tx.success();
            }
            tx.success();
        } finally {
            tx.close();
        }


        Map<String, Object> results = new HashMap<>();
        results.put("Imported Titles Count", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @POST
    @Path("/titles_less_lookups/{filename : (.+)?}")
    public Response importTitlesLessLookups(@PathParam("filename") String filename, @Context GraphDatabaseService db) throws Exception {
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
        Transaction tx = db.beginTx();
        try {
            for (CSVRecord record : records) {
                count++;
                Node occupation = getOccupation(db, record.get("O*NET-SOC Code"));
                if (occupation != null) {
                    Node title = getTitle(db, record.get("Alternate Title"));
                    occupation.createRelationshipTo(title, RelationshipTypes.HAS_TITLE);
                }
                if (count % 1000 == 0) {
                    System.out.println("Imported " + count + " at " +  new Date());
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
                tx.success();
            }
            tx.success();
        } finally {
            tx.close();
        }


        Map<String, Object> results = new HashMap<>();
        results.put("Imported Titles Count", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    private Node getOccupation(@Context GraphDatabaseService db, String code) {
        Node occupation = occupationNodeMap.get(code);
        if (occupation == null){
            occupation = db.findNode(Labels.Occupation, "code", code);
            occupationNodeMap.put(code, occupation);
        }

        return occupation;
    }

    private Node getTitle(@Context GraphDatabaseService db, String name) {
        Node title = titleNodeMap.get(name);
        if (title == null){
            title = db.createNode(Labels.Title);
            title.setProperty("name", name);
            titleNodeMap.put(name, title);
        }

        return title;
    }
}
