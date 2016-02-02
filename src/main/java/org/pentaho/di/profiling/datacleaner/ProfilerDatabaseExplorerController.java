package org.pentaho.di.profiling.datacleaner;

import java.io.OutputStream;
import java.util.Date;
import java.util.List;

import org.apache.commons.vfs2.FileObject;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.datacleaner.api.InputColumn;
import org.datacleaner.beans.BooleanAnalyzer;
import org.datacleaner.beans.DateAndTimeAnalyzer;
import org.datacleaner.beans.NumberAnalyzer;
import org.datacleaner.beans.StringAnalyzer;
import org.datacleaner.configuration.DataCleanerConfiguration;
import org.datacleaner.configuration.DataCleanerConfigurationImpl;
import org.datacleaner.connection.Datastore;
import org.datacleaner.connection.DatastoreConnection;
import org.datacleaner.connection.JdbcDatastore;
import org.datacleaner.job.JaxbJobWriter;
import org.datacleaner.job.builder.AnalysisJobBuilder;
import org.datacleaner.kettle.configuration.DataCleanerSpoonConfiguration;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.ui.core.database.dialog.XulDatabaseExplorerController;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

public class ProfilerDatabaseExplorerController extends AbstractXulEventHandler {

    private XulDatabaseExplorerController dbExplorerController;

    public ProfilerDatabaseExplorerController() {
    }

    public String getName() {
        return "profiler_database"; //$NON-NLS-1$
    }

    public void profileDbTable() throws Exception {
        final Spoon spoon = ((Spoon) SpoonFactory.getInstance());

        try {
            final DataCleanerSpoonConfiguration dataCleanerSpoonConfiguration = DataCleanerSpoonConfiguration.load();

            getDbController();
            // Close the db explorer...
            dbExplorerController.close();

            final DatabaseMeta dbMeta = dbExplorerController.getDatabaseMeta();
            final String tableName = dbExplorerController.getSelectedTable();
            final String schemaName = dbExplorerController.getSelectedSchema();
            final String schemaTable = dbMeta.getQuotedSchemaTableCombination(schemaName, tableName);

            // Pass along the configuration of the KettleDatabaseStore...
            final DataCleanerConfiguration dataCleanerConfiguration = new DataCleanerConfigurationImpl();
            try (final AnalysisJobBuilder analysisJobBuilder = new AnalysisJobBuilder(dataCleanerConfiguration)) {

                final Datastore datastore = new JdbcDatastore(dbMeta.getName(), dbMeta.getURL(),
                        dbMeta.getDriverClass(), dbMeta.getUsername(), dbMeta.getPassword(), false);
                analysisJobBuilder.setDatastore(datastore);

                try (DatastoreConnection connection = datastore.openConnection()) {
                    DataContext dataContext = connection.getDataContext();

                    // add all columns of a table
                    Table table = dataContext.getTableByQualifiedLabel(schemaTable);
                    if (table == null) {
                        Schema schema = dataContext.getSchemaByName(schemaName);
                        if (schema != null) {
                            table = schema.getTableByName(tableName);
                        }
                    }

                    final FileObject jobFile;
                    if (table == null) {
                        // Could not resolve table, this sometimes happens
                        jobFile = null;
                    } else {
                        Column[] customerColumns = table.getColumns();
                        analysisJobBuilder.addSourceColumns(customerColumns);

                        List<InputColumn<?>> numberColumns = analysisJobBuilder.getAvailableInputColumns(Number.class);
                        if (!numberColumns.isEmpty()) {
                            analysisJobBuilder.addAnalyzer(NumberAnalyzer.class).addInputColumns(numberColumns);
                        }

                        List<InputColumn<?>> dateColumns = analysisJobBuilder.getAvailableInputColumns(Date.class);
                        if (!dateColumns.isEmpty()) {
                            analysisJobBuilder.addAnalyzer(DateAndTimeAnalyzer.class).addInputColumns(dateColumns);
                        }

                        List<InputColumn<?>> booleanColumns = analysisJobBuilder
                                .getAvailableInputColumns(Boolean.class);
                        if (!booleanColumns.isEmpty()) {
                            analysisJobBuilder.addAnalyzer(BooleanAnalyzer.class).addInputColumns(booleanColumns);
                        }

                        List<InputColumn<?>> stringColumns = analysisJobBuilder.getAvailableInputColumns(String.class);
                        if (!stringColumns.isEmpty()) {
                            analysisJobBuilder.addAnalyzer(StringAnalyzer.class).addInputColumns(stringColumns);
                        }

                        // Write the job.xml to a temporary file...
                        jobFile = KettleVFS.createTempFile("datacleaner-job", ".xml",
                                System.getProperty("java.io.tmpdir"), new Variables());
                        OutputStream jobOutputStream = null;
                        try {
                            jobOutputStream = KettleVFS.getOutputStream(jobFile, false);
                            new JaxbJobWriter(dataCleanerConfiguration).write(analysisJobBuilder.toAnalysisJob(),
                                    jobOutputStream);
                            jobOutputStream.close();
                        } finally {
                            if (jobOutputStream != null) {
                                jobOutputStream.close();
                            }
                        }
                    }

                    // Write the conf.xml to a temporary file...
                    //
                    String confXml = generateConfXml(dbMeta.getName(), dbMeta.getURL(), dbMeta.getDriverClass(),
                            dbMeta.getUsername(), dbMeta.getPassword());
                    final FileObject confFile = KettleVFS.createTempFile("datacleaner-conf", ".xml",
                            System.getProperty("java.io.tmpdir"), new Variables());
                    OutputStream confOutputStream = null;
                    try {
                        confOutputStream = KettleVFS.getOutputStream(confFile, false);
                        confOutputStream.write(confXml.getBytes(Const.XML_ENCODING));
                        confOutputStream.close();
                    } finally {
                        if (confOutputStream != null) {
                            confOutputStream.close();
                        }
                    }

                    // Launch DataCleaner and point to the generated
                    // configuration and job XML files...
                    //

                    // Launch DataCleaner and point to the generated
                    // configuration and job XML files...
                    //
                    Spoon.getInstance().getDisplay().syncExec(new Runnable() {
                        public void run() {
                            new Thread() {
                                public void run() {
                                    final String jobFileName;
                                    if (jobFile == null) {
                                        jobFileName = null;
                                    } else {
                                        jobFileName = KettleVFS.getFilename(jobFile);
                                    }
                                    ModelerHelper.launchDataCleaner(dataCleanerSpoonConfiguration,
                                            KettleVFS.getFilename(confFile), jobFileName, dbMeta.getName(), null,null,null,null,true);
                                }
                            }.start();
                        }
                    });
                }
            }

        } catch (final Exception ex) {
            new ErrorDialog(spoon.getShell(), "Error", "unexpected error occurred", ex);
        }

    }

    private String generateConfXml(String name, String url, String driver, String username, String password) {
        StringBuilder xml = new StringBuilder();

        xml.append(XMLHandler.getXMLHeader());
        xml.append("<configuration xmlns=\"http://eobjects.org/analyzerbeans/configuration/1.0\"  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">");
        xml.append(XMLHandler.openTag("datastore-catalog"));

        xml.append("<jdbc-datastore name=\"" + name
                + "\" description=\"Database defined in Pentaho Data Integration\">");
        xml.append(XMLHandler.addTagValue("url", url));
        xml.append(XMLHandler.addTagValue("driver", driver));
        xml.append(XMLHandler.addTagValue("username", username));
        xml.append(XMLHandler.addTagValue("password", password));

        xml.append(XMLHandler.closeTag("jdbc-datastore"));
        xml.append(XMLHandler.closeTag("datastore-catalog"));

        xml.append("<multithreaded-taskrunner max-threads=\"30\" />");
        xml.append(XMLHandler.openTag("classpath-scanner"));
        xml.append("<package recursive=\"true\">org.eobjects.analyzer.beans</package> <package>org.eobjects.analyzer.result.renderer</package> <package>org.eobjects.datacleaner.output.beans</package> <package>org.eobjects.datacleaner.panels</package> <package recursive=\"true\">org.eobjects.datacleaner.widgets.result</package> <package recursive=\"true\">com.hi</package>");
        xml.append(XMLHandler.closeTag("classpath-scanner"));

        xml.append(XMLHandler.closeTag("configuration"));

        return xml.toString();
    }

    private XulDatabaseExplorerController getDbController() throws XulException {
        if (dbExplorerController == null) {
            dbExplorerController = (XulDatabaseExplorerController) this.getXulDomContainer().getEventHandler(
                    "dbexplorer");
        }
        return dbExplorerController;
    }

    public void setData(Object aDatabaseDialog) {
        this.dbExplorerController = (XulDatabaseExplorerController) aDatabaseDialog;
    }
}
