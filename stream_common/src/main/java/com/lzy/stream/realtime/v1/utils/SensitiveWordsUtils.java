package com.lzy.stream.realtime.v1.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Package com.lzy.stream.realtime.v1.utils.SensitiveWordsUtils
 * @Author zheyuan.liu
 * @Date 2025/5/7 10:12
 * @description: 工具类
 */

public class SensitiveWordsUtils {
    public static ArrayList<String> getSensitiveWordsLists(){
        ArrayList<String> res = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("D:\\project\\stream-dev\\realtime_common\\src\\main\\resources\\Identify-sensitive-words.txt"))){
            String line ;
            while ((line = reader.readLine()) != null){
                res.add(line);
            }
        }catch (IOException ioException){
            ioException.printStackTrace();
        }
        return res;
    }

    public static <T> T getRandomElement(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        Random random = new Random();
        int randomIndex = random.nextInt(list.size());
        return list.get(randomIndex);
    }

    public static void main(String[] args) {
        System.err.println(getRandomElement(getSensitiveWordsLists()));
    }
}
