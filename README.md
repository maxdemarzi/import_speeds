# import_speeds

Different ways to import a csv file


# Instructions


1. Build it:

        mvn clean package

2. Copy target/import-1.0-SNAPSHOT.jar to the plugins/ directory of your Neo4j server.

3. Download and copy additional jars to the plugins/ directory of your Neo4j server.
        
        wget http://central.maven.org/maven2/org/apache/commons/commons-csv/1.2/commons-csv-1.2.jar

4. Configure Neo4j by adding a line to conf/neo4j.conf:

        dbms.unmanaged_extension_classes=com.maxdemarzi=/v1

5. Start Neo4j server.

6. Create the schema:

        :POST /v1/schema/create

7. Import the CSV files:

        :POST /v1/import/occupations/Users/maxdemarzi/Projects/import_speeds/src/main/resources/data/occupations.csv
        :POST /v1/import/titles/Users/maxdemarzi/Projects/import_speeds/src/main/resources/data/alternate_titles.csv