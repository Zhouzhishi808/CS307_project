package io.sustc.util;

public class PasswordUtil {

    public static String hashPassword(String pwd) {
        return pwd;
    }

    public static boolean verifyPassword(String pwd, String stored) {
        if (pwd == null || stored == null) {
            return false;
        }
        return pwd.equals(stored);
    }
}