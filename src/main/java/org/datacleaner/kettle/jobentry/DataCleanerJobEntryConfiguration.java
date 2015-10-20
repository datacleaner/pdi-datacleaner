package org.datacleaner.kettle.jobentry;

import java.io.Serializable;

import org.datacleaner.kettle.configuration.DataCleanerSpoonConfiguration;
import org.datacleaner.kettle.configuration.DataCleanerSpoonConfigurationException;
import org.pentaho.di.profiling.datacleaner.ModelerHelper;

public class DataCleanerJobEntryConfiguration implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private String executableFilename;
    private String jobFilename;
    private DataCleanerOutputType outputType;
    private String outputFilename;
    private String additionalArguments;
    private boolean outputFileInResult = true;

    public String getJobFilename() {
        if (jobFilename == null) {
            jobFilename = "examples/employees.analysis.xml";
        }
        return jobFilename;
    }

    public void setJobFilename(String jobFile) {
        this.jobFilename = jobFile;
    }

    public String getExecutableFilename() {
        if (executableFilename == null) {
            try {
                DataCleanerSpoonConfiguration dcSpoonConfiguration = DataCleanerSpoonConfiguration.load();
                final String dataCleanerInstallationFolderPath = dcSpoonConfiguration
                        .getDataCleanerInstallationFolderPath();
                executableFilename = dataCleanerInstallationFolderPath + "/DataCleaner.jar";
            } catch (DataCleanerSpoonConfigurationException e) {
                ModelerHelper.showErrorMessage("DataCleaner configuration error",
                        "DataCleaner configuration not available", e);
                return null;
            }

        }
        return executableFilename;
    }

    public void setExecutableFilename(String executableFile) {
        this.executableFilename = executableFile;
    }

    public String getOutputFilename() {
        if (outputFilename == null) {
            outputFilename = "out." + getOutputType().getFileExtension();
        }
        return outputFilename;
    }

    public void setOutputFilename(String outputFile) {
        this.outputFilename = outputFile;
    }

    public DataCleanerOutputType getOutputType() {
        if (outputType == null) {
            outputType = DataCleanerOutputType.SERIALIZED;
        }
        return outputType;
    }

    public void setOutputType(DataCleanerOutputType outputType) {
        this.outputType = outputType;
    }

    public String getAdditionalArguments() {
        if (additionalArguments == null) {
            additionalArguments = "";
        }
        return additionalArguments;
    }

    public void setAdditionalArguments(String additionalArguments) {
        this.additionalArguments = additionalArguments;
    }

    public void setOutputFileInResult(boolean outputFileInResult) {
        this.outputFileInResult = outputFileInResult;
    }

    public boolean isOutputFileInResult() {
        return outputFileInResult;
    }
}
