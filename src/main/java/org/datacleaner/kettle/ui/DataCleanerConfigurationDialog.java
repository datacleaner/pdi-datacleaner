package org.datacleaner.kettle.ui;

import java.util.ArrayList;
import java.util.List;

import org.datacleaner.kettle.configuration.DataCleanerSpoonConfiguration;
import org.datacleaner.kettle.configuration.DataCleanerSpoonConfigurationException;
import org.datacleaner.kettle.configuration.utils.SoftwareVersionHelper;
import org.datacleaner.kettle.configuration.utils.SoftwareVersionHelper.SoftwareVersion;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class DataCleanerConfigurationDialog extends Dialog implements DisposeListener {

    // The spaces are necessary because the label length cannot be modified at
    // runtime
    private static final String EDITION = "Edition:                       ";
    private static final String VERSION = "Version:                       ";

    private Shell _shell;
    private Text _text;
    private Label _errorLabel;
    private Label _labelEdition;
    private Label _labelVersion;
    private Button _okButton; 
    private List<Object> _resources = new ArrayList<Object>();

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
    public void open() {
        createContents();
        
        _shell.pack();
        _shell.open();
        _shell.layout();
        
        while (!_shell.isDisposed()) {
            final Display display = _shell.getDisplay();
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
    }

    /**
     * Create contents of the dialog.
     */

    private void createContents() {
        _shell = new Shell(getParent(), getStyle());
        _shell.setSize(500, 425);
        _shell.setText(getText());
        
        //center the dialog in the middle of the screen
        final Rectangle screenSize = _shell.getDisplay().getPrimaryMonitor().getBounds();
        _shell.setLocation((screenSize.width - _shell.getBounds().width) / 2, (screenSize.height - _shell.getBounds().height) / 2);
 
        final GridLayout gridLayout = new GridLayout();
        gridLayout.numColumns = 4;
        gridLayout.makeColumnsEqualWidth=false;
        gridLayout.marginLeft = -5;
        gridLayout.marginRight = -5;
        gridLayout.marginTop = -5;
        gridLayout.marginBottom = -5;

        _shell.setLayout(gridLayout);
        _shell.setOrientation(getStyle());
        final DataCleanerBanner banner = new DataCleanerBanner(_shell);
        banner.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true, 4, 1));

        new Label(_shell, SWT.NONE);
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
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        _text = new Text(_shell, SWT.BORDER);
        _text.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));
        final Button btnBrowse = new Button(_shell, SWT.NONE);
        btnBrowse.setText("Browse");
        btnBrowse.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, true, false));
        new Label(_shell, SWT.NONE);
        
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        _errorLabel = new Label(_shell, SWT.NONE);
        _errorLabel.setText("The selected folder does not contain a DataCleaner installation");
        _errorLabel.setForeground(getParent().getDisplay().getSystemColor(SWT.COLOR_RED));
        _errorLabel.setVisible(false);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        _labelEdition = new Label(_shell, SWT.NONE);
        _labelEdition.setText(EDITION);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
       
        new Label(_shell, SWT.NONE);
        _labelVersion = new Label(_shell, SWT.NONE);
        _labelVersion.setText(VERSION);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);

        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);
        new Label(_shell, SWT.NONE);

       
        new Label(_shell, SWT.NONE);
        final Button cancelButton = new Button(_shell, SWT.NONE);
        Image cancelImage = new Image(_shell.getDisplay(), DataCleanerConfigurationDialog.class.getResourceAsStream("cancel.png"));
        _resources.add(cancelImage);
        cancelButton.setImage(cancelImage);
        cancelButton.setText("Cancel");
        cancelButton.addListener(SWT.Selection, new Listener() {
            @Override
            public void handleEvent(Event arg0) {
               _shell.close();
            }
        });
        
        cancelButton.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, true, false, 1, 1));
        _okButton = new Button(_shell, SWT.NONE);
         final Image okImage = new Image(_shell.getDisplay(), DataCleanerConfigurationDialog.class.getResourceAsStream("save.png"));
        _resources.add(okImage);
        _okButton.setImage(okImage);
        _okButton.setText("OK");
        _okButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                DataCleanerSpoonConfiguration.save(_text.getText());
                _shell.close();
            }
        });
        
        final GridData gridData = new GridData(SWT.LEFT, SWT.CENTER, false, false, 1, 1);
        _okButton.setLayoutData(gridData);
        new Label(_shell, SWT.NONE);
       
        final DataCleanerFooter footer = new DataCleanerFooter(_shell);
        footer.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false, 4, 1));

        btnBrowse.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent event) {
                final DirectoryDialog directoryChooser = new DirectoryDialog(_shell);
                directoryChooser.setFilterPath(_text.getText());
                directoryChooser.setText("DataCleaner instalation folder");
                directoryChooser.setMessage("Select a directory");
                final String dir = directoryChooser.open();
                setDataCleanerInstallationFolder(dir);
            }
        });
        
        try {
            DataCleanerSpoonConfiguration dcSpoonConfiguration = DataCleanerSpoonConfiguration.load();
            setDataCleanerInstallationFolder(dcSpoonConfiguration.getDataCleanerInstallationFolderPath());
        } catch (DataCleanerSpoonConfigurationException e) {
            // Do nothing if a previous configuration could not be loaded
        }
    }

    protected void setDataCleanerInstallationFolder(String dir) {
        if (dir == null || dir.trim().isEmpty()) {
            return;
        }
        _text.setText(dir);
        final DataCleanerSpoonConfiguration dataCleanerSpoonConfiguration = new DataCleanerSpoonConfiguration(null, dir);
        final SoftwareVersion editionDetails = SoftwareVersionHelper.getEditionDetails(dataCleanerSpoonConfiguration);
        if (editionDetails != null) {
            _labelEdition.setText(EDITION.trim() + " " + editionDetails.getName());
            _labelVersion.setText(VERSION.trim() + " " + editionDetails.getVersion());
            _errorLabel.setVisible(false);
            _okButton.setEnabled(true);
        } else {
            _errorLabel.setVisible(true);
            _okButton.setEnabled(false);
            
        }
    }

    @Override
    public void widgetDisposed(DisposeEvent arg0) {
        for (Object resource : _resources) {
            if (resource instanceof Image) {
                ((Image) resource).dispose();
            }
        }
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
