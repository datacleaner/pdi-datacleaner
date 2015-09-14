package org.datacleaner.kettle.configuration;

import java.io.File;

import org.apache.metamodel.util.FileHelper;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.ui.spoon.SpoonPluginType;

public class DataCleanerSpoonConfiguration {

    public static final String CONFIGURATION_FILENAME = "datacleaner-configuration.txt";

    private final String _pluginFolderPath;
    private final String _installationFolder;

    public static final DataCleanerSpoonConfiguration load() throws DataCleanerSpoonConfigurationException {
        final String pluginFolderPath = detectPluginFolderPath();
        final String configurationFilePath = pluginFolderPath + "/" + CONFIGURATION_FILENAME;

        final File file = new File(configurationFilePath);
        if (!file.exists()) {
            throw new DataCleanerSpoonConfigurationException("No configuration file: " + configurationFilePath);
        }

        final String installationPath = FileHelper.readFileAsString(file);
        if (installationPath == null || installationPath.trim().isEmpty()) {
            throw new DataCleanerSpoonConfigurationException("Configuration file is empty: " + configurationFilePath);
        }

        final DataCleanerSpoonConfiguration configuration = new DataCleanerSpoonConfiguration(pluginFolderPath,
                installationPath);
        return configuration;
    }

    private static String detectPluginFolderPath() {
        try {
            final PluginInterface spoonPlugin = PluginRegistry.getInstance().findPluginWithId(SpoonPluginType.class,
                    "SpoonDataCleaner");
            return KettleVFS.getFilename(KettleVFS.getFileObject(spoonPlugin.getPluginDirectory().toString()));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to determine location of the spoon profile plugin. It is needed to know where DataCleaner is installed.");
        }
    }

    public static void save(String dataCleanerInstallationFolder) {
        final String pluginFolderPath = detectPluginFolderPath();
        final String configurationFilePath = pluginFolderPath + "/" + CONFIGURATION_FILENAME;

        final File file = new File(configurationFilePath);
        FileHelper.writeStringAsFile(file, dataCleanerInstallationFolder);
    }

    public DataCleanerSpoonConfiguration(String pluginFolderPath, String installationFolder) {
        _pluginFolderPath = pluginFolderPath;
        _installationFolder = installationFolder;
    }

    public String getDataCleanerInstallationFolderPath() {
        return _installationFolder;
    }

    public String getPluginFolderPath() {
        return _pluginFolderPath;
    }
}
