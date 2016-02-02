package org.datacleaner.kettle.jobentry;

import java.io.Serializable;

public class DataCleanerJobEntryConfiguration implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

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
