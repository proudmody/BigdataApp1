package com.triman.bigdata.util; /**
 * Created by proud on 2016/4/6.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class HDFSUtil {

    private HDFSUtil() {
    }

    /**
     * 判断路径是否存在
     *
     * @param conf
     * @param path
     * @return
     * @throws IOException
     */
    public static boolean exits(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(path));
    }

    /**
     * 创建文件
     *
     * @param conf
     * @param filePath
     * @param contents
     * @throws IOException
     */
    public static void createFile(Configuration conf, String filePath, byte[] contents) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
    }

    /**
     * 创建文件
     *
     * @param conf
     * @param filePath
     * @param fileContent
     * @throws IOException
     */
    public static void createFile(Configuration conf, String filePath, String fileContent) throws IOException {
        createFile(conf, filePath, fileContent.getBytes());
    }

    /**
     * @param conf
     * @param localFilePath
     * @param remoteFilePath
     * @throws IOException
     */
    public static void copyFromLocalFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        fs.copyFromLocalFile(true, true, localPath, remotePath);
        fs.close();
    }

    /**
     * 删除目录或文件
     *
     * @param conf
     * @param remoteFilePath
     * @param recursive
     * @return
     * @throws IOException
     */
    public static boolean deleteFile(Configuration conf, String remoteFilePath, boolean recursive) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        boolean result = fs.delete(new Path(remoteFilePath), recursive);
        fs.close();
        return result;
    }

    /**
     * 删除目录或文件(如果有子目录,则级联删除)
     *
     * @param conf
     * @param remoteFilePath
     * @return
     * @throws IOException
     */
    public static boolean deleteFile(Configuration conf, String remoteFilePath) throws IOException {
        return deleteFile(conf, remoteFilePath, true);
    }

    /**
     * 文件重命名
     *
     * @param conf
     * @param oldFileName
     * @param newFileName
     * @return
     * @throws IOException
     */
    public static boolean renameFile(Configuration conf, String oldFileName, String newFileName) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path oldPath = new Path(oldFileName);
        Path newPath = new Path(newFileName);
        boolean result = fs.rename(oldPath, newPath);
        fs.close();
        return result;
    }

    /**
     * 创建目录
     *
     * @param conf
     * @param dirName
     * @return
     * @throws IOException
     */
    public static boolean createDirectory(Configuration conf, String dirName) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dir = new Path(dirName);
        boolean result = fs.mkdirs(dir);
        fs.close();
        return result;
    }

    /**
     * 列出指定路径下的所有文件(不包含目录)
     *
     * @param basePath
     * @param recursive
     */
    public static RemoteIterator<LocatedFileStatus> listFiles(FileSystem fs, String basePath, boolean recursive) throws IOException {

        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(new Path(basePath), recursive);

        return fileStatusRemoteIterator;
    }

    /**
     * 列出指定路径下的文件（非递归）
     *
     * @param conf
     * @param basePath
     * @return
     * @throws IOException
     */
    public static RemoteIterator<LocatedFileStatus> listFiles(Configuration conf, String basePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path(basePath), false);
        fs.close();
        return remoteIterator;
    }

    /**
     * 列出指定目录下的文件\子目录信息（非递归）
     *
     * @param conf
     * @param dirPath
     * @return
     * @throws IOException
     */
    public static FileStatus[] listStatus(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dirPath));
        fs.close();
        return fileStatuses;
    }


    /**
     * 读取文件内容
     *
     * @param conf
     * @param filePath
     * @return
     * @throws IOException
     */
    public static String readFile(Configuration conf, String filePath) throws IOException {
        String fileContent = null;
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        InputStream inputStream = null;
        ByteArrayOutputStream outputStream = null;
        try {
            inputStream = fs.open(path);
            outputStream = new ByteArrayOutputStream(inputStream.available());
            IOUtils.copyBytes(inputStream, outputStream, conf);
            fileContent = outputStream.toString();
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
            fs.close();
        }
        return fileContent;
    }
    /**
     * 上传文件到hdfs
     *
     * @param conf
     * @param src
     * @param dst
     * @return
     * @throws IOException
     */
    public static void UploadLocalFileHDFS(Configuration conf, String src, String dst) throws IOException
    {
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
       // FileSystem fs = FileSystem.get(URI.create(dst), conf);
        Path pathDst = new Path(dst);
        Path pathSrc = new Path(src);

        fs.copyFromLocalFile(pathSrc, pathDst);
        fs.close();
    }
    /**
     * 从hdfs下载文件
     *
     * @param conf
     * @param src
     * @param dst
     * @return
     * @throws IOException
     */
    public static void DowloadFromHDFS(Configuration conf, String src, String dst) throws IOException
    {
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(src);
        File file1 = new File(dst);
        FSDataInputStream in = fs.open(path);
        FileOutputStream out = new FileOutputStream(file1);

        try {
            file1.createNewFile(); // 创建文件
            IOUtils.copyBytes(in, out, 4096, true);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            finally {
                out.close();
                in.close();
            }
    }
    //@Test
    public static void test() throws IOException {
        Configuration conf = new Configuration();
        String newDir = "/test";
        //01.检测路径是否存在 测试
        if (HDFSUtil.exits(conf, newDir)) {
            System.out.println(newDir + " 已存在!");
        } else {
            //02.创建目录测试
            boolean result = HDFSUtil.createDirectory(conf, newDir);
            if (result) {
                System.out.println(newDir + " 创建成功!");
            } else {
                System.out.println(newDir + " 创建失败!");
            }
        }
        String fileContent = "Hi,hadoop. I love you";
        String newFileName = newDir + "/myfile.txt";

        //03.创建文件测试
        HDFSUtil.createFile(conf, newFileName, fileContent);
        System.out.println(newFileName + " 创建成功");

        //04.读取文件内容 测试
        System.out.println(newFileName + " 的内容为:\n" + HDFSUtil.readFile(conf, newFileName));

        //05. 测试获取所有目录信息
        FileStatus[] dirs = HDFSUtil.listStatus(conf, "/user/hadoop/");
        System.out.println("--根目录下的所有子目录---");
        for (FileStatus s : dirs) {
            System.out.println(s);
        }

        //06. 测试获取所有文件
//        FileSystem fs = FileSystem.get(conf);
//        RemoteIterator<LocatedFileStatus> files = com.triman.bigdata.util.HDFSUtil.listFiles(fs, "/", true);
//        System.out.println("--根目录下的所有文件---");
//        while (files.hasNext()) {
//            System.out.println(files.next());
//        }
//        fs.close();

        //删除文件测试
        boolean isDeleted = HDFSUtil.deleteFile(conf, newDir);
        System.out.println(newDir + " 已被删除");

//        //download test
//        com.triman.bigdata.util.HDFSUtil.DowloadFromHDFS(conf,"/user/hadoop/b.mp4","d:\\c.mp4");
//        System.out.println("下载成功！");

//        com.triman.bigdata.util.HDFSUtil.UploadLocalFileHDFS(conf,"d:\\c.mp4","/user/hadoop/");
//        //05. 测试获取所有目录信息
//        FileStatus[] dirs2 = com.triman.bigdata.util.HDFSUtil.listStatus(conf, "/user/hadoop");
//        System.out.println("--根目录下的所有子目录---");
//        for (FileStatus s : dirs2) {
//            System.out.println(s);
//        }
    }
}