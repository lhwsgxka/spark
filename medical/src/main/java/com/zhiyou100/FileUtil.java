package com.zhiyou100;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.List;

public class FileUtil {
    public static List<String> readFile(String s) throws IOException {
        return IOUtils.readLines(new FileReader(s));
    }


    public static void writeFile(String s, String s1, boolean b) throws IOException {

        PrintWriter printWriter = new PrintWriter(new FileWriter(s, b), true);
        printWriter.println(s1);
        printWriter.close();
    }
    public static void writeFile(String s, List<String> reimburseList, boolean b) throws IOException {
        for (String s1:
             reimburseList) {
            writeFile(s,s1,true);
        }
    }

}
