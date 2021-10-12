package org.apache.flume.channel.file;

import org.junit.Test;
import org.mapdb.DB;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class FIleBufferTest {

    @Test
    public void test() throws Exception {
        DB db = null;
        DB.HTreeMapMaker map = db.createHashMap("test");
        map.make().close();
        File file = new File("E:\\SVN\\common_service\\flume-file-channel\\file-channel\\db1");
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        MappedByteBuffer writeBuf = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 10 * 1024 * 1024);
        writeBuf.put(new byte[1000]);
        writeBuf.put(new byte[]{1, 2, 3, 4});

        MappedByteBuffer readBuf = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 1000, 10 * 1024 * 1024);
        byte b = readBuf.get();
        System.out.println();

    }

    @Test
    public void bTest() throws Exception {
        {
            long start = System.currentTimeMillis();
            File file = new File("E:\\SVN\\common_service\\flume-file-channel\\file-channel\\db1");
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel channel = raf.getChannel();
            MappedByteBuffer writeBuf = channel.map(FileChannel.MapMode.READ_WRITE, 0, 100 * 1024 * 1024);
            writeBuf.put(new byte[1024]);
            writeBuf.force();
            channel.close();
            raf.close();
            long end = System.currentTimeMillis();
            System.out.println(end - start);
        }
        {
            long start = System.currentTimeMillis();
            File file = new File("E:\\SVN\\common_service\\flume-file-channel\\file-channel\\db11");
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel channel = raf.getChannel();
            channel.write(ByteBuffer.wrap(new byte[1024]));
            channel.close();
            raf.close();
            long end = System.currentTimeMillis();
            System.out.println(end - start);
        }
        {
            long start = System.currentTimeMillis();
            File file = new File("E:\\SVN\\common_service\\flume-file-channel\\file-channel\\db2");
            FileOutputStream outputStream = new FileOutputStream(file);
            outputStream.write(new byte[1024]);
            outputStream.close();
            long end = System.currentTimeMillis();
            System.out.println(end - start);
        }


        //test
        {
            File file = new File("E:\\SVN\\common_service\\flume-file-channel\\file-channel\\db3");
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel channel = raf.getChannel();
            MappedByteBuffer writeBuf = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1000 * 1024 * 1024);
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                writeBuf.put(new byte[1024]);
            }
            long end = System.currentTimeMillis();
            System.out.println(end - start);
            writeBuf.force();
            channel.close();
            raf.close();
        }
        {
            File file = new File("E:\\SVN\\common_service\\flume-file-channel\\file-channel\\db31");
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel channel = raf.getChannel();
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                channel.write(ByteBuffer.wrap(new byte[1024]));
            }
            long end = System.currentTimeMillis();
            System.out.println(end - start);
            channel.force(false);
            channel.close();
            raf.close();
        }
        {
            File file = new File("E:\\SVN\\common_service\\flume-file-channel\\file-channel\\db4");
            FileOutputStream outputStream = new FileOutputStream(file);
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                outputStream.write(new byte[1024]);
            }
            long end = System.currentTimeMillis();
            System.out.println(end - start);
            outputStream.flush();
            outputStream.close();
        }
        System.out.println();

//        count 100
//        194
//        319
//        187
//        count 1000
//        1501
//        909
//        1128
    }

}
