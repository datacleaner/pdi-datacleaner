package org.pentaho.di.profiling.datacleaner;

import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Table;

public class KettleDataContextTest extends TestCase {

    public void testReadSimpleFile() throws Exception {
        KettleDataContext dc = new KettleDataContext("src/test/resources/simple_name_and_age_data.kettlestream");

        assertEquals("[information_schema, hmm]", Arrays.toString(dc.getSchemaNames()));

        Table[] tables = dc.getDefaultSchema().getTables();
        assertEquals(1, tables.length);

        Table table = tables[0];
        assertEquals("Table[name=Data Grid,type=TABLE,remarks=null]", table.toString());

        assertEquals(2, table.getColumnCount());
        assertEquals(
                "Column[name=name,columnNumber=0,type=VARCHAR,nullable=true,indexed=false,nativeType=String,columnSize=10]",
                table.getColumns()[0].toString());
        assertEquals(
                "Column[name=age,columnNumber=1,type=DOUBLE,nullable=true,indexed=false,nativeType=Number,columnSize=10]",
                table.getColumns()[1].toString());

        DataSet ds = dc.query().from(table).select(table.getColumns()).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[Kasper, 30.0]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[Trine, 30.0]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[Vera, 2.0]]", ds.getRow().toString());
        assertFalse(ds.next());
    }
}
