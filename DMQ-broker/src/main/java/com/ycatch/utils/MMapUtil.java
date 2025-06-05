package com.ycatch.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * 提供基于 Java 的 MMap Api 访问文件的能力
 * 包括: 文件读写，支持指定的 offset 的文件映射
 * 支持指定的 offset 的文件映射（结束映射的 offset - 开始写入的 offset = 映射的内存体积）
 * 文件从指定的 offset 开始读取
 * 文件从指定的 offset 开始写入
 * 文件映射后的内存释放
 * @author zhongyujie
 */
public class MMapUtil {

    private File file;
    private MappedByteBuffer mappedByteBuffer;
    private int mappedSize;
    private FileChannel fileChannel;

    /**
     * 指定 offset 做文件的映射
     *
     * @param filePath 文件路径
     * @param startOffset 开始映射 offset
     * @param mappedSize 映射的体积
     */
    public void loadFileInMMap(String filePath, int startOffset, int mappedSize) throws IOException {
        file = new File(filePath);
        if (!file.exists()) {
            throw new FileNotFoundException("filePath is " + filePath
            + " inValid");
        }
        // 高性能文件 I/O，并且提供了内存映射文件
        fileChannel = new RandomAccessFile(file, "rw").getChannel();
        // 此时此刻，文件已经被映射到指定的内存空间
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, mappedSize);
    }

    /**
     * 支持从文件的指定 offset 开始读取
     *
     * @param readOffset
     * @param size
     * @return
     */
    public byte[] readContent(int readOffset, int size) {
        mappedByteBuffer.position(readOffset);
        byte[] content = new byte[size];
        int j = 0;
        for (int i = 0; i < size; i++) {
            //  内存访问，效率非常快
            byte b = mappedByteBuffer.get(readOffset + i);
            content[j++] = b;
        }
        return content;
    }

    /**
     * 支持从文件的指定 offset 处写入
     *
     * @param content
     */
    public void writeContent(byte[] content) {
        writeContent(content, false);
    }

    /**
     * 支持从文件的指定 offset 处写入, 可选是否刷盘
     *
     * @param content
     * @param force
     */
    public void writeContent(byte[] content, boolean force) {
        // 默认刷到 page cache 中，
        // 如果需要强制刷盘，需要兼容
        mappedByteBuffer.put(content);
        if (force) {
            // 强制刷盘
            mappedByteBuffer.force();
        }
    }

    public void clear() {
        if (mappedByteBuffer == null || !mappedByteBuffer.isDirect() ||
        mappedByteBuffer.capacity() == 0) {
            return ;
        }
        invoke(invoke(viewed(mappedByteBuffer), "cleaner"), "clean");
        viewed(mappedByteBuffer);
    }

    private Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private ByteBuffer viewed(ByteBuffer byteBuffer) {
        String methodName = "viewedBuffer";
        Method[] methods = byteBuffer.getClass().getMethods();
        for (Method method : methods) {
            if ("attachment".equals(method.getName())) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(byteBuffer, methodName);
        if (viewedBuffer == null) {
            return byteBuffer;
        } else {
            return viewed(viewedBuffer);
        }
    }

}
