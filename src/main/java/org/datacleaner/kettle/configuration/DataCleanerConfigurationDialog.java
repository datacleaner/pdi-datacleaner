package org.datacleaner.kettle.configuration;

import java.io.IOException;

import org.datacleaner.kettle.configuration.utils.SoftwareVersionHelper;
import org.datacleaner.kettle.configuration.utils.SoftwareVersionHelper.SoftwareVersion;
import org.datacleaner.kettle.ui.DataCleanerBanner;
import org.datacleaner.kettle.ui.DataCleanerFooter;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.profiling.datacleaner.ModelerHelper;

public class DataCleanerConfigurationDialog extends Dialog {

    // The spaces are necessary because the label length cannot be modified at
    // runtime
    private static final String EDITION = "Edition:                       ";
    private static final String VERSION = "Version:                       ";

    protected String _result;
    protected Shell _shell;

    /**
     * Create the dialog.
     * 
     * @param parent
     * @param style
     */
    public DataCleanerConfigurationDialog(Shell parent, int style) {
        super(parent, style);
        setText("DataCleaner configuration");
    }

    /**
     * Open the dialog.
     * 
     * @return the result
     */
    public String open() {
        createContents();
        _shell.open();
        _shell.layout();
        final Display display = getParent().getDisplay();
        while (!_shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return _result;
    }

    /**
     * Create contents of the dialog.
     */

    private void createContents() {
        _shell = new Shell(getParent(), getStyle());
        _shell.setSize(500, 425);
        _shell.setText(getText());
        final GridLayout gridLayout = new GridLayout(3, false);
        gridLayout.marginLeft = -5;
        gridLayout.marginRight = -5;
        gridLayout.marginTop = -5;
        gridLayout.marginBottom = -5;

        _shell.setLayout(gridLayout);
        _shell.setOrientation(getStyle());
        final DataCleanerBanner banner = new DataCleanerBanner(_shell);
        banner.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true, 3, 1));

        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        final Label lblSelectDatacleanerApplication = new Label(_shell, SWT.NONE);
        lblSelectDatacleanerApplication.setText("Select DataCleaner application directory:");
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        final Text _text = new Text(_shell, SWT.BORDER);
        _text.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));

        final Button btnBrowse = new Button(_shell, SWT.NONE);
        btnBrowse.setText("Browse");

        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        final Label errorLabel = new Label(_shell, SWT.NONE);
        errorLabel.setText("The selected folder does not contain a DataCleaner installation");
        errorLabel.setForeground(getParent().getDisplay().getSystemColor(SWT.COLOR_RED));
        errorLabel.setVisible(false);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        final Label labelEdition = new Label(_shell, SWT.NONE);
        labelEdition.setText(EDITION);

        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        final Label labelVersion = new Label(_shell, SWT.NONE);
        labelVersion.setText(VERSION);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        final Button btnOK = new Button(_shell, SWT.NONE);
        btnOK.setText("OK");
        btnOK.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                _shell.close();
            }
        });

        final DataCleanerFooter footer = new DataCleanerFooter(_shell);
        footer.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false, 3, 1));

        btnBrowse.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent event) {
                final DirectoryDialog directoryChooser = new DirectoryDialog(_shell);
                directoryChooser.setFilterPath(_text.getText());
                directoryChooser.setText("DataCleaner instalation folder");
                directoryChooser.setMessage("Select a directory");
                final String dir = directoryChooser.open();
                if (dir != null) {
                    _text.setText(dir);
                    try {
                        final SoftwareVersion editionDetails = SoftwareVersionHelper.getEditionDetails(dir);
                        if (editionDetails != null) {
                            labelEdition.setText(EDITION.trim() + " " + editionDetails.getName());
                            labelVersion.setText(VERSION.trim() + " " + editionDetails.getVersion());
                            errorLabel.setVisible(false);
                            _result = dir;
                        } else {
                            errorLabel.setVisible(true);
                            _result = null;
                        }
                    } catch (IOException e) {
                        errorLabel.setText("Exception while reading the directory");
                        e.printStackTrace();
                    }

                }
            }
        });

        try {
            final String pluginFolderPath = ModelerHelper.getPluginFolderPath();
            final String dcInstallationFolder = ModelerHelper.getDataCleanerInstalationPath(pluginFolderPath);
            if (dcInstallationFolder != null) {
                _text.setText(dcInstallationFolder);
            }
            final SoftwareVersion editionDetails = SoftwareVersionHelper.getEditionDetails(dcInstallationFolder);
            if (editionDetails != null) {
                labelEdition.setText(EDITION.trim() + " " + editionDetails.getName());
                labelVersion.setText(VERSION.trim() + " " + editionDetails.getVersion());
            }
        } catch (Throwable e) {
            // Do nothing If the file doesn't exit 

        }

    }

    public void close() {
        this.close();
    }

    public static void main(String[] args) {

        final Display display = Display.getDefault();
        final Shell shell = new Shell(display, SWT.SHELL_TRIM);
        final DataCleanerConfigurationDialog dataCleanerSettingsDialog = new DataCleanerConfigurationDialog(shell,
                SWT.SHELL_TRIM);

        dataCleanerSettingsDialog.open();

        while (!shell.isDisposed()) {
            if (!display.readAndDispatch())
                display.sleep();
        }
        display.dispose();

    }

}
