package org.datacleaner.kettle.configuration.utils;

import java.io.File;
import java.io.IOException;

public class SoftwareVersionHelper {

    public static final String DATACLEANER_COMMUNITY = "Community";
    public static final String DATACLEANER_ENTERPRISE = "Professional";

    public static class SoftwareVersion {

        private final String _name;
        private final String _version;

        SoftwareVersion(String name, String version) {
            _name = name;
            _version = version;
        }

        public String getName() {
            return _name;
        }

        public String getVersion() {
            return _version;
        }
    }

    public static SoftwareVersion getEditionDetails(String path) throws IOException {
        final File folder = new File(path + "/lib");
        if (!folder.exists()) {
            return null;
        }
        final String fileEnterprise = getEdition("DataCleaner-enterprise-edition-core", folder);
        if (fileEnterprise != null) {
            return new SoftwareVersion(DATACLEANER_ENTERPRISE, getVersion(fileEnterprise));
        } else {
            final String fileCommunity = getEdition("DataCleaner-engine-core", folder);
            if (fileCommunity != null) {
                return new SoftwareVersion(DATACLEANER_COMMUNITY, getVersion(fileCommunity));
            }
        }

        return null;

    }

    private static String getEdition(String file, File folder) {
        for (final File fileEntry : folder.listFiles()) {
            if (!fileEntry.isDirectory()) {
                final String fileName = fileEntry.getName();
                if (fileName.contains(file)) {
                    return fileName;
                }
            }
        }
        return null;
    }

    private static String getVersion(String fileName) {
        if (fileName == null) {
            return "Unknown";
        }
        final int lastIndexOfDash = fileName.lastIndexOf("-");
        final int lastIndexOfDot = fileName.lastIndexOf(".");
        if (lastIndexOfDash == -1 || lastIndexOfDot == -1) {
            return "Unknown";
        }
        return fileName.substring(lastIndexOfDash + 1, lastIndexOfDot);
    }
}