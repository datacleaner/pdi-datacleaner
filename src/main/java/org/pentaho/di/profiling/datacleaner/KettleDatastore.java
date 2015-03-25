package org.pentaho.di.profiling.datacleaner;

import org.datacleaner.api.Configured;
import org.datacleaner.connection.Datastore;
import org.datacleaner.connection.DatastoreConnectionImpl;
import org.datacleaner.connection.PerformanceCharacteristics;
import org.datacleaner.connection.PerformanceCharacteristicsImpl;
import org.datacleaner.connection.UsageAwareDatastore;
import org.datacleaner.connection.UsageAwareDatastoreConnection;
import org.pentaho.di.core.row.RowMetaInterface;

public class KettleDatastore extends UsageAwareDatastore<KettleDataContext> implements Datastore {

    private static final long serialVersionUID = 4122274303140036370L;

    @Configured
    String name;

    @Configured
    String filename;

    private RowMetaInterface rowMeta;

    public KettleDatastore(String name, String filename) {
        super(name);
        this.name = name;
        this.filename = filename;
        this.rowMeta = null;
    }

    public KettleDatastore(String name, String description, RowMetaInterface rowMeta) {
        super(name);
        this.name = name;
        setDescription(description);
        this.rowMeta = rowMeta;
    }

    public KettleDatastore() {
        this(null, null);
    }

    @Override
    protected UsageAwareDatastoreConnection<KettleDataContext> createDatastoreConnection() {
        KettleDataContext kettleDataContext;
        if (rowMeta == null) {
            // Get the row metadata from the remote socket
            kettleDataContext = new KettleDataContext(filename);
        } else {
            // local configuration building, use what we got.
            kettleDataContext = new KettleDataContext(name, getDescription(), rowMeta);
        }
        return new DatastoreConnectionImpl<KettleDataContext>(kettleDataContext, this);
    }

    @Override
    public PerformanceCharacteristics getPerformanceCharacteristics() {
        return new PerformanceCharacteristicsImpl(false, true);
    }

    @Override
    public final String getName() {
        return name;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }
}
