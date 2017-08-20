package de.martin70m.common.ftp;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import de.martin70m.common.sql.MySqlConnection;

public class FTPDownloader {

	FTPClient ftp = null;

    public FTPDownloader(String host, String user, String pwd) throws Exception {
        ftp = new FTPClient();
        ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out)));
        int reply;
        ftp.connect(host);
        reply = ftp.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            ftp.disconnect();
            throw new Exception("Exception in connecting to FTP Server");
        }
        ftp.login(user, pwd);
        ftp.setFileType(FTP.BINARY_FILE_TYPE);
        ftp.enterLocalPassiveMode();
    }

    // Download the FTP File from the FTP Server
    public void downloadFile(String remoteFilePath, String localFilePath) {
        try (FileOutputStream fos = new FileOutputStream(localFilePath)) {
            this.ftp.retrieveFile(remoteFilePath, fos);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // list the files in a specified directory on the FTP
    public boolean listFiles(String directory, String fileName) throws IOException {
        // lists files and directories in the current working directory
        boolean verificationFilename = false;        
        FTPFile[] files = ftp.listFiles(directory);
        for (FTPFile file : files) {
            String details = file.getName();                
            System.out.println(details);            
            if(details.equals(fileName))
            {
                System.out.println("Correct Filename");
                verificationFilename=details.equals(fileName);
                //assertTrue("Verification Failed: The filename is not updated at the CDN end.",details.equals(fileName));                
            }
        }  
        
         return verificationFilename;
    }
    public boolean downloadFiles(String remoteDirectory, String localDirectory) throws IOException {
        // lists files and directories in the current working directory
        boolean verificationFilename = false;        
        FTPFile[] files = ftp.listFiles(remoteDirectory);
        for (FTPFile file : files) { 
        	if (!file.isFile()) {
        		continue;
        	}
        	System.out.println("File is " + file.getName()+" getting Downloaded");
            //get output stream
            OutputStream output;
         
            File outfile=new File(localDirectory + "/" + file.getName());
            outfile.createNewFile();
             
            output = new FileOutputStream(outfile);
            //get the file from the remote system
            ftp.retrieveFile(remoteDirectory + file.getName(), output);
            
            //close output stream
            output.close();
        }  
        
         return verificationFilename;
    }
    public void disconnect() {
        if (this.ftp.isConnected()) {
            try {
                this.ftp.logout();
                this.ftp.disconnect();
            } catch (IOException f) {
                // do nothing as file is already downloaded from FTP server
            }
        }
    }

}