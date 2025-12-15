package io.sustc.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

public class PasswordUtil {

    private static final int SALT_LEN = 16;

    public static String hashPassword(String pwd) {
        if (pwd == null || pwd.isEmpty()) {
            return null;
        }

        try {
            SecureRandom rand = new SecureRandom();
            byte[] salt = new byte[SALT_LEN];
            rand.nextBytes(salt);

            byte[] hash = hash(pwd, salt);

            String s1 = Base64.getEncoder().encodeToString(salt);
            String s2 = Base64.getEncoder().encodeToString(hash);

            return s1 + "$" + s2;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Password hashing failed", e);
        }
    }

    public static boolean verifyPassword(String pwd, String stored) {
        if (pwd == null || stored == null) {
            return false;
        }

        try {
            String[] parts = stored.split("\\$");
            if (parts.length != 2) {
                return false;
            }

            byte[] salt = Base64.getDecoder().decode(parts[0]);
            byte[] expected = Base64.getDecoder().decode(parts[1]);

            byte[] actual = hash(pwd, salt);

            return MessageDigest.isEqual(expected, actual);
        } catch (Exception e) {
            return false;
        }
    }

    private static byte[] hash(String pwd, byte[] salt) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(salt);
        return md.digest(pwd.getBytes());
    }
}