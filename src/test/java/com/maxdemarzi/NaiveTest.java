package com.maxdemarzi;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.io.File;
import java.net.URL;
import java.util.HashMap;

public class NaiveTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withExtension("/v1", Imports.class);

    @Test
    public void shouldImportTitles() {
        HTTP.POST(neo4j.httpURI().resolve("/v1/schema/create").toString());
        URL url = this.getClass().getResource("/data/occupations.csv");
        File file = new File(url.getFile());
        HTTP.POST(neo4j.httpURI().resolve("/v1/import/occupations" + file.getAbsolutePath()).toString());

        url = this.getClass().getResource("/data/alternate_titles.csv");
        file = new File(url.getFile());
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/import/titles" + file.getAbsolutePath()).toString());
        HashMap actual = response.content();
        Assert.assertEquals(expected, actual);
    }

    private static final HashMap expected = new HashMap<String, Object>() {{
        put("Imported Titles Count", 59972);
    }};
}
