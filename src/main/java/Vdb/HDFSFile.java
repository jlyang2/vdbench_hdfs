package Vdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

class HDFSFile extends File {

    // private String pathname;
    private Path path;
    static String sep = System.getProperty("file.separator");

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public HDFSFile(String dir, String finame){
        this(dir + sep + finame);
    }

    public HDFSFile(String pathname) {
        super(pathname);
        this.path = new Path(pathname);
        // TODO Auto-generated constructor stub
    }

    public String[] list() {
        FileStatus[] files;
        try {
            files = SlaveJvm.fileSys.listStatus(path);
            String[] result = new String[files.length];
            for (int i = 0; i < files.length; ++i) {
                result[i] = files[i].getPath().getName();
            }

            return result;
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }

    public boolean exists(){
        try {
            return SlaveJvm.fileSys.exists(path);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return false;
    }

    public boolean mkdir(){
        try {
            return SlaveJvm.fileSys.mkdirs(path);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return false;
    }

    public boolean delete(){
        try {
            return SlaveJvm.fileSys.delete(path, false);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return false;
    }
}
