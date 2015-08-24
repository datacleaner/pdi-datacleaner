package org.datacleaner.kettle.settings;

import org.datacleaner.kettle.ui.DataCleanerBanner;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

public class DataCleanerSettingsDialog extends org.eclipse.swt.widgets.Dialog {

    public DataCleanerSettingsDialog(Shell shell) {
        super(shell, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL);
    }

    public void open() {
        Shell parent = getParent();
        Shell shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL);

        // DC banner
        final DataCleanerBanner banner = new DataCleanerBanner(shell);
        {
            final FormData bannerLayoutData = new FormData();
            bannerLayoutData.left = new FormAttachment(0, 0);
            bannerLayoutData.right = new FormAttachment(100, 0);
            bannerLayoutData.top = new FormAttachment(0, 0);
            banner.setLayoutData(bannerLayoutData);
        }

        shell.pack();
        shell.open();
    }

    public static void main(String[] args) {
        Display display = new Display();
        Shell shell = new Shell(display);

        new DataCleanerSettingsDialog(shell).open();
        ;
    }
}
