package plugin;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.vfs2.FileObject;
import org.datacleaner.kettle.configuration.DataCleanerSpoonConfiguration;
import org.datacleaner.kettle.jobentry.DataCleanerJobEntryConfiguration;
import org.datacleaner.kettle.jobentry.DataCleanerJobEntryDialog;
import org.datacleaner.kettle.jobentry.DataCleanerOutputType;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.profiling.datacleaner.ModelerHelper;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * A job entry for executing DataCleaner jobs
 * 
 * @author Kasper Sorensen
 * @since 22-03-2012
 */
@JobEntry(id = "DataCleanerJobEntry", categoryDescription = "Utility", image = "org/datacleaner/logo.png", name = DataCleanerJobEntry.NAME, description = "Executes a DataCleaner job")
public class DataCleanerJobEntry extends JobEntryBase implements JobEntryInterface, Cloneable {

    public static final String NAME = "Execute DataCleaner job";
	public static final String LOGCHANNEL_NAME = "DataCleaner";

    private final DataCleanerJobEntryConfiguration configuration = new DataCleanerJobEntryConfiguration();

    @Override
    public Result execute(Result result, int nr) throws KettleException {
		LogChannelInterface log = new LogChannel( LOGCHANNEL_NAME );
        int exitCode = -1;

        final DataCleanerSpoonConfiguration dataCleanerSpoonConfiguration = ModelerHelper
                .getDataCleanerSpoonConfigurationOrShowError();
        final String outputFilename = environmentSubstitute(configuration.getOutputFilename());
        final String jobFilename = environmentSubstitute(configuration.getJobFilename());
        final String outputFiletype = configuration.getOutputType().toString();
        final String additionalArguments = environmentSubstitute( configuration.getAdditionalArguments() );
		
        if (dataCleanerSpoonConfiguration != null) {
            exitCode = ModelerHelper.launchDataCleanerSimple(dataCleanerSpoonConfiguration, jobFilename, outputFiletype,
                    outputFilename, additionalArguments);
        }
        result.setExitStatus(exitCode);
        result.setResult(true);

        if (configuration.isOutputFileInResult()) {
            File outputFile = new File(outputFilename);
            if (!outputFile.exists()) {
                outputFile = new File(dataCleanerSpoonConfiguration.getPluginFolderPath(), outputFilename);
            }

            if (outputFile.exists()) {
                final Map<String, ResultFile> files = new ConcurrentHashMap<String, ResultFile>();
                FileObject fileObject;
                try {
                    fileObject = KettleVFS.getFileObject(outputFile.getCanonicalPath(), this);
                } catch (IOException e) {
                    log.logError("Exception " + e.getMessage());
                    throw new KettleException("IO exception" + e.getMessage());
                }
                final ResultFile resultFile = new ResultFile(ResultFile.FILE_TYPE_GENERAL, fileObject, parentJob
                        .getJobname(), toString());
                files.put(outputFilename, resultFile);
                result.setResultFiles(files);
            }
        }

        if (exitCode != 0) {
            result.setResult(false);
        }

        return result;

    }

    @Override
    public DataCleanerJobEntry clone() {
        final DataCleanerJobEntry clone = (DataCleanerJobEntry) super.clone();
        return clone;
    }

    public DataCleanerJobEntryConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public String getDialogClassName() {
        return DataCleanerJobEntryDialog.class.getName();
    }
   

    public String getXML() {
        final StringBuilder retval = new StringBuilder();

        retval.append(super.getXML());
        retval.append("      ").append(XMLHandler.addTagValue("job_file", configuration.getJobFilename()));
        retval.append("      ").append(XMLHandler.addTagValue("output_file", configuration.getOutputFilename()));
        retval.append("      ").append(XMLHandler.addTagValue("output_type", configuration.getOutputType().toString()));
        retval.append("      ").append(XMLHandler.addTagValue("output_file_in_result", configuration
                .isOutputFileInResult()));
        retval.append("      ").append(XMLHandler.addTagValue("additional_arguments", configuration
                .getAdditionalArguments()));

        return retval.toString();
    }

    public void loadXML(Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep)
            throws KettleXMLException {
        try {
            super.loadXML(entrynode, databases, slaveServers);

            configuration.setJobFilename(XMLHandler.getTagValue(entrynode, "job_file"));
            configuration.setOutputFilename(XMLHandler.getTagValue(entrynode, "output_file"));
            configuration.setOutputType(DataCleanerOutputType.valueOf(XMLHandler.getTagValue(entrynode,
                    "output_type")));
            final String outputFileInResult = XMLHandler.getTagValue(entrynode, "output_file_in_result");
            if ("false".equalsIgnoreCase(outputFileInResult)) {
                configuration.setOutputFileInResult(false);
            }

            configuration.setAdditionalArguments(XMLHandler.getTagValue(entrynode, "additional_arguments"));

        } catch (KettleXMLException xe) {
            throw new KettleXMLException("Unable to load job entry from XML node", xe);
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_job) throws KettleException {
        super.saveRep(rep, metaStore, id_job);

        rep.saveJobEntryAttribute(id_job, getObjectId(), "job_file", configuration.getJobFilename());
        rep.saveJobEntryAttribute(id_job, getObjectId(), "output_file", configuration.getOutputFilename());
        rep.saveJobEntryAttribute(id_job, getObjectId(), "output_type", configuration.getOutputType().toString());
        rep.saveJobEntryAttribute(id_job, getObjectId(), "output_file_in_result", configuration.isOutputFileInResult());
        rep.saveJobEntryAttribute(id_job, getObjectId(), "additional_arguments", configuration
                .getAdditionalArguments());
    }

    @Override
    public void loadRep(Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
            List<SlaveServer> slaveServers) throws KettleException {
        super.loadRep(rep, metaStore, id_jobentry, databases, slaveServers);

        configuration.setJobFilename(rep.getJobEntryAttributeString(id_jobentry, "job_file"));
        configuration.setOutputFilename(rep.getJobEntryAttributeString(id_jobentry, "output_file"));
        configuration.setOutputType(DataCleanerOutputType.valueOf(rep.getJobEntryAttributeString(id_jobentry,
                "output_type")));
        configuration.setOutputFileInResult(rep.getJobEntryAttributeBoolean(id_jobentry, "output_file_in_result",
                true));
        configuration.setAdditionalArguments(rep.getJobEntryAttributeString(id_jobentry, "additional_arguments"));
    }
}
