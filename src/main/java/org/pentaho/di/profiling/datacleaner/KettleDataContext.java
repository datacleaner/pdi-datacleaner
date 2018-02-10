package org.pentaho.di.profiling.datacleaner;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.FileHelper;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KettleDataContext extends QueryPostprocessDataContext {

    private static final Logger logger = LoggerFactory.getLogger(KettleDataContext.class);

    /**
     * The filename containing the data produced by Kettle
     */
    private final String filename;
    private String transformationName;
    private String stepName;
    private RowMetaInterface rowMeta;

    /**
     * This constructor opens up a file containing all the metadata and data
     * needed to profile The data in the file is comprised of the following
     * item:
     * <p>
     * 
     * - The name of the transformation<br>
     * - The name of the step being profiled<br>
     * - The RowMeta object of the output of the step<br>
     * - Rows of data corresponding to RowMeta<br>
     * <br>
     * 
     * @param filename
     *            the filename to read from
     */
    public KettleDataContext(String filename) {
        if (filename == null || filename.length() == 0) {
            throw new IllegalArgumentException(
                    "You need to provide a Kettle serialized file for DataCleaner to read from");
        }

        this.filename = filename;
    }

    /**
     * Constructor used only for schema navigation (while building DC job)
     * 
     * @param transformationName
     * @param stepName
     * @param rowMeta
     */
    public KettleDataContext(String transformationName, String stepName, RowMetaInterface rowMeta) {
        this.filename = null;
        this.transformationName = transformationName;
        this.stepName = stepName;
        this.rowMeta = rowMeta;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        DataInputStream inputStream = createInputStream();

        // skip through the metadata section
        try {
            readMetadataSection(inputStream);
        } catch (RuntimeException e) {
            FileHelper.safeClose(inputStream);
            throw e;
        }

        DataSet dataSet = new KettleDataSet(columns, inputStream, rowMeta);
        if (maxRows >= 0) {
            dataSet = new MaxRowsDataSet(dataSet, maxRows);
        }
        return dataSet;
    }

    private void readMetadataSection(DataInputStream inputStream) {
        try {
            ValueMetaPluginType pluginType = ValueMetaPluginType.getInstance();
            pluginType.searchPlugins();

            PluginRegistry.init();
            PluginRegistry.addPluginType(pluginType);
        } catch (KettleException e) {
            throw new MetaModelException(e);
        }

        // transformation name, step name & RowMeta ...
        try {
            transformationName = inputStream.readUTF();
            logger.debug("Read transformation name: {}", transformationName);
            stepName = inputStream.readUTF();
            logger.debug("Read step name: {}", stepName);
            rowMeta = new RowMeta(inputStream);
            logger.debug("Read row meta: {}", rowMeta);
        } catch (Exception e) {
            throw new MetaModelException("Error while reading metadata section", e);
        }
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        MutableSchema schema = new MutableSchema(getTransformationName());
        MutableTable table = new MutableTable(getStepName(), TableType.TABLE);
        table.setSchema(schema);
        RowMetaInterface rowMeta = getRowMeta();
        for (int i = 0; i < rowMeta.size(); i++) {
            ValueMetaInterface valueMeta = rowMeta.getValueMeta(i);
            MutableColumn column = new MutableColumn(valueMeta.getName(), getColumnType(valueMeta), table, i,
                    Integer.valueOf(valueMeta.getLength()), valueMeta.getTypeDesc(), true, valueMeta.getComments(),
                    false, "");
            table.addColumn(column);
        }
        schema.addTable(table);
        return schema;
    }

    private ColumnType getColumnType(ValueMetaInterface valueMeta) {
        switch (valueMeta.getType()) {
        case ValueMetaInterface.TYPE_STRING:
            return ColumnType.VARCHAR;
        case ValueMetaInterface.TYPE_INTEGER:
            return ColumnType.INTEGER;
        case ValueMetaInterface.TYPE_DATE:
            return ColumnType.DATE;
        case ValueMetaInterface.TYPE_BOOLEAN:
            return ColumnType.BOOLEAN;
        case ValueMetaInterface.TYPE_NUMBER:
            return ColumnType.DOUBLE;
        case ValueMetaInterface.TYPE_BINARY:
            return ColumnType.BINARY;
        case ValueMetaInterface.TYPE_BIGNUMBER:
            return ColumnType.DECIMAL;
        }
        throw new RuntimeException("It is currently not possible to profile values of type: " + valueMeta.getTypeDesc());
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return getTransformationName();
    }

    public String getFilename() {
        return filename;
    }

    public RowMetaInterface getRowMeta() {
        if (rowMeta == null) {
            readMetadata();
        }
        return rowMeta;
    }

    public String getStepName() {
        if (stepName == null) {
            readMetadata();
        }
        return stepName;
    }

    public String getTransformationName() {
        if (transformationName == null) {
            readMetadata();
        }
        return transformationName;
    }

    private DataInputStream createInputStream() {
        try {
            return new DataInputStream(new FileInputStream(filename));
        } catch (FileNotFoundException e) {
            throw new MetaModelException("Unable to open input stream", e);
        }
    }

    private void readMetadata() {
        final DataInputStream inputStream = createInputStream();
        try {
            readMetadataSection(inputStream);
        } finally {
            FileHelper.safeClose(inputStream);
        }
    }
}
