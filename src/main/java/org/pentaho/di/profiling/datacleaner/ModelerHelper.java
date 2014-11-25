package org.pentaho.di.profiling.datacleaner;

import java.io.File;
import java.io.FilenameFilter;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.vfs.FileObject;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.Column;
import org.eobjects.analyzer.beans.BooleanAnalyzer;
import org.eobjects.analyzer.beans.DateAndTimeAnalyzer;
import org.eobjects.analyzer.beans.NumberAnalyzer;
import org.eobjects.analyzer.beans.StringAnalyzer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreConnection;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.job.JaxbJobWriter;
import org.eobjects.analyzer.job.builder.AnalysisJobBuilder;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.csvinput.CsvInputMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonPluginType;
import org.pentaho.di.ui.trans.dialog.TransExecutionConfigurationDialog;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

public class ModelerHelper extends AbstractXulEventHandler implements ISpoonMenuController {

    private static final String SYSTEM_PROPERTY_DATATYPE_FACTORY = "javax.xml.datatype.DatatypeFactory";
    private static ModelerHelper instance = null;

    // private static Logger logger =
    // LoggerFactory.getLogger(ModelerHelper.class);

    private ModelerHelper() {
    }

    public static ModelerHelper getInstance() {
        if (instance == null) {
            instance = new ModelerHelper();
            Spoon spoon = ((Spoon) SpoonFactory.getInstance());
            spoon.addSpoonMenuController(instance);
        }
        return instance;
    }

    /**
     * this method is used to see if a valid TableOutput step has been selected
     * in a trans graph before attempting to model or quick vis
     * 
     * @return true if valid
     */
    public static boolean isValidStepSelected() {
        StepMeta stepMeta = getCurrentStep();
        if (stepMeta == null) {
            return false;
        }

        if (stepMeta.getStepMetaInterface() instanceof CsvInputMeta)
            return true;

        return false;
    }

    public static StepMeta getCurrentStep() {
        Spoon spoon = ((Spoon) SpoonFactory.getInstance());
        TransMeta transMeta = spoon.getActiveTransformation();
        if (transMeta == null || spoon.getActiveTransGraph() == null) {
            return null;
        }
        StepMeta stepMeta = spoon.getActiveTransGraph().getCurrentStep();
        return stepMeta;
    }

    public String getName() {
        return "profiler"; //$NON-NLS-1$
    }

    public static void launchDataCleaner(String confFile, String jobFile, String datastore, String dataFile) {

        LogChannelInterface log = Spoon.getInstance().getLog();

        try {
            // figure out path for DC
            //
            final String pluginFolderPath = getPluginFolderPath();

            final String kettleLibPath = pluginFolderPath + "/../../lib";

            final String pluginPath = pluginFolderPath + "/DataCleaner-PDI-plugin.jar";
            final String kettleCorePath = getJarFile(kettleLibPath, "kettle-core");
            final String commonsVfsPath = getJarFile(kettleLibPath, "commons-vfs");
            final String scannotationPath = getJarFile(kettleLibPath, "scannotation");
            final String javassistPath = getJarFile(kettleLibPath, "javassist");

            // Assemble the class path for DataCleaner
            final String[] paths = new String[] { pluginPath, kettleCorePath, commonsVfsPath, scannotationPath,
                    javassistPath };
            final StringBuilder classPathBuilder = new StringBuilder();
            for (String path : paths) {
                if (classPathBuilder.length() > 0) {
                    classPathBuilder.append(File.pathSeparator);
                }
                classPathBuilder.append(path);
            }

            final List<String> cmds = new ArrayList<String>();
            cmds.add(System.getProperty("java.home") + "/bin/java");
            cmds.add("-cp");
            cmds.add(classPathBuilder.toString());
            cmds.add("-Ddatacleaner.ui.visible=true");
            cmds.add("-Ddatacleaner.embed.client=Kettle");
            cmds.add("-DDATACLEANER_HOME=" + pluginFolderPath);

            // Finally, the class to launch
            //
            cmds.add("org.eobjects.datacleaner.Main");

            // The optional arguments for DataCleaner
            //
            if (!Const.isEmpty(confFile)) {
                cmds.add("-conf");
                cmds.add(confFile);
            }
            if (!Const.isEmpty(jobFile)) {
                cmds.add("-job");
                cmds.add(jobFile);
            }
            if (!Const.isEmpty(datastore)) {
                cmds.add("-ds");
                cmds.add(datastore);
            }

            // Log the command
            //
            StringBuilder commandString = new StringBuilder();
            for (String cmd : cmds) {
                commandString.append(cmd).append(" ");
            }

            log.logBasic("DataCleaner launch commands : " + commandString);

            ProcessBuilder processBuilder = new ProcessBuilder(cmds);
            processBuilder.environment().put("DATACLEANER_HOME", pluginFolderPath);
            Process process = processBuilder.start();

            ProcessStreamReader psrStdout = new ProcessStreamReader(process.getInputStream(), log, false);
            ProcessStreamReader psrStderr = new ProcessStreamReader(process.getErrorStream(), log, true);
            psrStdout.start();
            psrStderr.start();

            process.waitFor();

            psrStdout.join();
            psrStderr.join();

            // When DC finishes we clean up the temporary files...
            //
            if (!Const.isEmpty(confFile)) {
                new File(confFile).delete();
            }
            if (!Const.isEmpty(jobFile)) {
                new File(jobFile).delete();
            }
            if (!Const.isEmpty(dataFile)) {
                new File(dataFile).delete();
            }
        } catch (Throwable e) {
            new ErrorDialog(Spoon.getInstance().getShell(), "Error launching DataCleaner",
                    "There was an unexpected error launching DataCleaner", e);
        }
    }

    private static String getJarFile(String libPath, final String filename) {
        final File directory = new File(libPath);
        assert directory.exists() && directory.isDirectory();

        final String[] filenames = directory.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(filename);
            }
        });

        return libPath + "/" + filenames[0];
    }

    private static String getPluginFolderPath() throws XulException {
        final String pluginFolderPath;
        try {
            final PluginInterface spoonPlugin = PluginRegistry.getInstance().findPluginWithId(SpoonPluginType.class,
                    "SpoonDataCleaner");
            pluginFolderPath = KettleVFS.getFilename(KettleVFS.getFileObject(spoonPlugin.getPluginDirectory()
                    .toString()));
        } catch (Exception e) {
            throw new XulException(
                    "Unable to determine location of the spoon profile plugin.  It is needed to know where DataCleaner is installed.");
        }

        final LogChannelInterface log = Spoon.getInstance().getLog();
        log.logBasic("DataCleaner plugin path = '" + pluginFolderPath + "'");
        return pluginFolderPath;
    }

    public void openProfiler() throws Exception {
        launchDataCleaner(null, null, null, null);
    }

    public void profileStep() throws Exception {

        final Spoon spoon = ((Spoon) SpoonFactory.getInstance());

        try {

            final TransMeta transMeta = spoon.getActiveTransformation();
            if (transMeta == null || spoon.getActiveTransGraph() == null) {
                return;
            }

            StepMeta stepMeta = spoon.getActiveTransGraph().getCurrentStep();
            if (stepMeta == null) {
                return;
            }

            // TODO: show the transformation execution configuration dialog
            //
            //
            TransExecutionConfiguration executionConfiguration = spoon.getTransPreviewExecutionConfiguration();
            TransExecutionConfigurationDialog tecd = new TransExecutionConfigurationDialog(spoon.getShell(),
                    executionConfiguration, transMeta);
            if (!tecd.open())
                return;

            // Pass the configuration to the transMeta object:
            //
            String[] args = null;
            Map<String, String> arguments = executionConfiguration.getArguments();
            if (arguments != null) {
                args = convertArguments(arguments);
            }
            transMeta.injectVariables(executionConfiguration.getVariables());

            // Set the named parameters
            Map<String, String> paramMap = executionConfiguration.getParams();
            Set<String> keys = paramMap.keySet();
            for (String key : keys) {
                transMeta.setParameterValue(key, Const.NVL(paramMap.get(key), "")); //$NON-NLS-1$
            }

            transMeta.activateParameters();

            // Do we need to clear the log before running?
            //
            if (executionConfiguration.isClearingLog()) {
                spoon.getActiveTransGraph().transLogDelegate.clearLog();
            }

            // Now that we have the transformation and everything we can run it
            // and profile it...
            //
            Trans trans = new Trans(transMeta, Spoon.loggingObject);
            trans.prepareExecution(executionConfiguration.getArgumentStrings());
            trans.setSafeModeEnabled(executionConfiguration.isSafeModeEnabled());
            trans.setPreview(true);
            trans.prepareExecution(args);
            trans.setRepository(spoon.rep);

            // Open a server socket. This thing will block on init() until
            // DataCleaner connects to it...
            final DataCleanerKettleFileWriter writer = new DataCleanerKettleFileWriter(trans, stepMeta);
            try {
                writer.run();
            } finally {
                writer.close();
            }

            // Pass along the configuration of the KettleDatabaseStore...
            //
            final AnalyzerBeansConfiguration analyzerBeansConfiguration = new AnalyzerBeansConfigurationImpl();
            try (final AnalysisJobBuilder analysisJobBuilder = new AnalysisJobBuilder(analyzerBeansConfiguration)) {

                final Datastore datastore = new KettleDatastore(transMeta.getName(), stepMeta.getName(),
                        transMeta.getStepFields(stepMeta));
                analysisJobBuilder.setDatastore(datastore);

                try (DatastoreConnection connection = datastore.openConnection();) {
                    DataContext dataContext = connection.getDataContext();

                    // add all columns of a table
                    Column[] customerColumns = dataContext.getTableByQualifiedLabel(stepMeta.getName()).getColumns();
                    analysisJobBuilder.addSourceColumns(customerColumns);

                    List<InputColumn<?>> numberColumns = analysisJobBuilder.getAvailableInputColumns(Number.class);
                    if (!numberColumns.isEmpty()) {
                        analysisJobBuilder.addAnalyzer(NumberAnalyzer.class).addInputColumns(numberColumns);
                    }

                    List<InputColumn<?>> dateColumns = analysisJobBuilder.getAvailableInputColumns(Date.class);
                    if (!dateColumns.isEmpty()) {
                        analysisJobBuilder.addAnalyzer(DateAndTimeAnalyzer.class).addInputColumns(dateColumns);
                    }

                    List<InputColumn<?>> booleanColumns = analysisJobBuilder.getAvailableInputColumns(Boolean.class);
                    if (!booleanColumns.isEmpty()) {
                        analysisJobBuilder.addAnalyzer(BooleanAnalyzer.class).addInputColumns(booleanColumns);
                    }

                    List<InputColumn<?>> stringColumns = analysisJobBuilder.getAvailableInputColumns(String.class);
                    if (!stringColumns.isEmpty()) {
                        analysisJobBuilder.addAnalyzer(StringAnalyzer.class).addInputColumns(stringColumns);
                    }

                    // Write the job.xml to a temporary file...
                    //
                    FileObject jobFile = KettleVFS.createTempFile("datacleaner-job", ".xml",
                            System.getProperty("java.io.tmpdir"), new Variables());
                    OutputStream jobOutputStream = null;

                    // prevent lame xerces issue
                    final String previousSystemProperty = System.setProperty(SYSTEM_PROPERTY_DATATYPE_FACTORY,
                            "org.apache.xerces.jaxp.datatype.DatatypeFactoryImpl");
                    try {
                        try {
                            jobOutputStream = KettleVFS.getOutputStream(jobFile, false);
                            final JaxbJobWriter jobWriter = new JaxbJobWriter(analyzerBeansConfiguration);
                            jobWriter.write(analysisJobBuilder.toAnalysisJob(), jobOutputStream);
                        } catch (Exception e) {
                            final LogChannelInterface log = Spoon.getInstance().getLog();
                            log.logError("Failed to save DataCleaner job", e);
                            jobFile = null;
                        } finally {
                            if (jobOutputStream != null) {
                                jobOutputStream.close();
                            }
                        }
                    } finally {
                        if (previousSystemProperty == null) {
                            System.clearProperty(SYSTEM_PROPERTY_DATATYPE_FACTORY);
                        } else {
                            System.setProperty(SYSTEM_PROPERTY_DATATYPE_FACTORY, previousSystemProperty);
                        }
                    }

                    final String jobFilename;
                    if (jobFile == null) {
                        jobFilename = null;
                    } else {
                        jobFilename = KettleVFS.getFilename(jobFile);
                    }

                    // Write the conf.xml to a temporary file...
                    //
                    String confXml = generateConfXml(transMeta.getName(), stepMeta.getName(), writer.getFilename());
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
                    // configuration
                    // and job XML files...
                    //
                    Spoon.getInstance().getDisplay().syncExec(new Runnable() {
                        public void run() {
                            new Thread() {
                                public void run() {
                                    launchDataCleaner(KettleVFS.getFilename(confFile), jobFilename,
                                            transMeta.getName(), writer.getFilename());
                                }
                            }.start();
                        }
                    });

                }
            }
        } catch (final Exception e) {
            new ErrorDialog(spoon.getShell(), "Error", "unexpected error occurred", e);
        } finally {
            //
        }
    }

    private String generateConfXml(String name, String stepname, String filename) {
        StringBuilder xml = new StringBuilder();

        xml.append(XMLHandler.getXMLHeader());
        xml.append("<configuration xmlns=\"http://eobjects.org/analyzerbeans/configuration/1.0\"");
        xml.append("   xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">").append(Const.CR);
        xml.append(XMLHandler.openTag("datastore-catalog"));

        /*
         * <custom-datastore
         * class-name="org.eobjects.analyzer.configuration.SampleCustomDatastore"
         * > <property name="Name" value="my_custom" /> <property
         * name="Xml file" value="pom.xml" /> <property name="Description"
         * value="custom" /> </custom-datastore>
         */

        xml.append("<custom-datastore class-name=\"" + KettleDatastore.class.getName() + "\">").append(Const.CR);
        xml.append("<property name=\"Name\" value=\"" + name + "\" />");
        xml.append("<property name=\"Filename\" value=\"" + filename + "\" />");

        xml.append(XMLHandler.closeTag("custom-datastore"));
        xml.append(XMLHandler.closeTag("datastore-catalog"));

        xml.append("<multithreaded-taskrunner max-threads=\"30\" />");
        xml.append(XMLHandler.openTag("classpath-scanner"));
        xml.append("<package recursive=\"true\">org.eobjects.analyzer.beans</package> <package>org.eobjects.analyzer.result.renderer</package> <package>org.eobjects.datacleaner.output.beans</package> <package>org.eobjects.datacleaner.panels</package> <package recursive=\"true\">org.eobjects.datacleaner.widgets.result</package> <package recursive=\"true\">com.hi</package>");
        xml.append(XMLHandler.closeTag("classpath-scanner"));

        xml.append(XMLHandler.closeTag("configuration"));

        return xml.toString();
    }

    private String[] convertArguments(Map<String, String> arguments) {
        String[] argumentNames = arguments.keySet().toArray(new String[arguments.size()]);
        Arrays.sort(argumentNames);

        String args[] = new String[argumentNames.length];
        for (int i = 0; i < args.length; i++) {
            String argumentName = argumentNames[i];
            args[i] = arguments.get(argumentName);
        }
        return args;
    }

    @Override
    public void updateMenu(Document doc) {
        // TODO Auto-generated method stub

    }
}
