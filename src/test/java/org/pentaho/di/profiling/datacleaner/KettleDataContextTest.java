package org.pentaho.di.profiling.datacleaner;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.List;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Table;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.row.value.ValueMetaString;

import junit.framework.TestCase;

public class KettleDataContextTest extends TestCase {

    public void testReadSimpleFile() throws Exception {
        ValueMetaPluginType pluginType = ValueMetaPluginType.getInstance();
        pluginType.searchPlugins();

        PluginRegistry.init();
        PluginRegistry.addPluginType(pluginType);

        final String filename = "target/simple_name_and_age_data.kettlestream";

        // create the source
        final RowMeta rowMeta = new RowMeta();
        rowMeta.addValueMeta(new ValueMetaString("name"));
        rowMeta.addValueMeta(new ValueMetaNumber("age"));

        DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(filename));
        try {
            dataOutputStream.writeUTF("hmm");
            dataOutputStream.writeUTF("Data Grid");
            rowMeta.writeMeta(dataOutputStream);
            rowMeta.writeData(dataOutputStream, new Object[] { "Kasper", 30.0 });
            rowMeta.writeData(dataOutputStream, new Object[] { "Trine", 30.0 });
            rowMeta.writeData(dataOutputStream, new Object[] { "Vera", 2.0 });
        } finally {
            dataOutputStream.flush();
            dataOutputStream.close();
        }

        KettleDataContext dc = new KettleDataContext(filename);

        assertEquals("[information_schema, hmm]", dc.getSchemaNames().toString());

        List<Table> tables = dc.getDefaultSchema().getTables();
        assertEquals(1, tables.size());

        Table table = tables.get(0);
        assertEquals("Table[name=Data Grid,type=TABLE,remarks=null]", table.toString());

        assertEquals(2, table.getColumnCount());
        assertEquals("Column[name=name,columnNumber=0,type=STRING,nullable=true,nativeType=String,columnSize=-1]",
                table.getColumns().get(0).toString());
        assertEquals("Column[name=age,columnNumber=1,type=DOUBLE,nullable=true,nativeType=Number,columnSize=-1]",
                table.getColumns().get(1).toString());

        DataSet ds = dc.query().from(table).select(table.getColumns()).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[Kasper, 30.0]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[Trine, 30.0]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[Vera, 2.0]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }
}
