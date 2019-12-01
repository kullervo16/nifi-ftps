package org.apache.nifi.processors.standard;

import org.apache.nifi.processors.standard.util.FTPSTransfer;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * This interactive test requires a working FTPS server... it now runs against a publicly available (read-only) FTPS
 * server. You can run it manually against your server, in which case you will need to change the credentials
 */
public class FTPSTest {


    private TestRunner sot;


    @Test
    @Ignore
    public void testBasicListSelfSigned() {
        this.sot = TestRunners.newTestRunner(ListFTPS.class);
        this.sot.setProperty(FileTransfer.HOSTNAME,"192.168.1.50");
        this.sot.setProperty(FileTransfer.USERNAME,"jef");
        this.sot.setProperty(FileTransfer.PASSWORD,"ikke");
        this.sot.setProperty(FTPSTransfer.ALLOW_SELFSIGNED,"true");

        this.sot.run();
        assertEquals(18,this.sot.getFlowFilesForRelationship(ListFTPS.REL_SUCCESS).size());
    }

    @Test
    public void testBasicListPublic() {
        this.sot = TestRunners.newTestRunner(ListFTPS.class);
        this.sot.setProperty(FileTransfer.HOSTNAME,"test.rebex.net");
        this.sot.setProperty(FileTransfer.USERNAME,"demo");
        this.sot.setProperty(FileTransfer.PASSWORD,"password");
        this.sot.setProperty(FTPSTransfer.ALLOW_SELFSIGNED,"false");
        this.sot.setProperty(FTPSTransfer.IMPLICIT, "false");

        this.sot.run();
        assertEquals(1,this.sot.getFlowFilesForRelationship(ListFTPS.REL_SUCCESS).size());
    }

    @Test
    @Ignore
    public void testBasicGetSelfSigned() {
        this.sot = TestRunners.newTestRunner(GetFTPS.class);
        this.sot.setProperty(FileTransfer.HOSTNAME,"192.168.1.50");
        this.sot.setProperty(FileTransfer.USERNAME,"jef");
        this.sot.setProperty(FileTransfer.PASSWORD,"ikke");
        this.sot.setProperty(FTPSTransfer.ALLOW_SELFSIGNED,"true");
        this.sot.setProperty(FileTransfer.REMOTE_PATH,"transfer");
        this.sot.setProperty(FileTransfer.DELETE_ORIGINAL, "false");

        this.sot.run();
        assertEquals(2,this.sot.getFlowFilesForRelationship(GetFTPS.REL_SUCCESS).size());
    }

    @Test
    public void testBasicGetPublic() throws IOException {
        this.sot = TestRunners.newTestRunner(GetFTPS.class);
        this.sot.setProperty(FileTransfer.HOSTNAME,"test.rebex.net");
        this.sot.setProperty(FileTransfer.USERNAME,"demo");
        this.sot.setProperty(FileTransfer.PASSWORD,"password");
        this.sot.setProperty(FTPSTransfer.ALLOW_SELFSIGNED,"false");

        this.sot.setProperty(FileTransfer.DELETE_ORIGINAL, "false");

        this.sot.run();
        assertEquals(1,this.sot.getFlowFilesForRelationship(GetFTPS.REL_SUCCESS).size());
        this.sot.getFlowFilesForRelationship(GetFTPS.REL_SUCCESS).get(0).assertContentEquals(new File("./src/test/resources/getResult.txt"));
    }
}
