package old;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class test {
    public static void main(String[] args) throws Exception {
        System.out.println(randomString());
        System.out.println(randomString());
        System.out.println(randomString());
        System.out.println(randomString());
        System.out.println(randomString());
        System.out.println(randomString());


    }
    public static String randomString() {
        byte[] array = new byte[32]; // length is bounded by 32
        new Random().nextBytes(array);
        return new String(array, StandardCharsets.UTF_8);
    }
}
