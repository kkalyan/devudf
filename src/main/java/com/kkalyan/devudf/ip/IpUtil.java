package com.kkalyan.devudf.ip;

public class IpUtil {

    public static String convertToStrIPFromLong(long ip) {
        short subD = (short) (ip & 0xFF);
        short subC = (short) ((ip & 0xFF00) >> 8);
        short subB = (short) ((ip & 0xFF0000) >> 16);
        short subA = (short) ((ip & 0xFF000000) >> 24);
        String IP = subA + "." + subB + "." + subC + "." + subD;
        return IP;
    }

    public static long convertToLongFromStrIP(String address) {
        if (address == null) {
            return 0;
        }
        address = address.trim();

        String[] parts = address.split("\\.");
        if (parts == null || parts.length != 4) {
            return 0;
        }

        byte[] addr = new byte[4];
        for (int i = 0; i < 4; i++) {
            addr[i] = Integer.valueOf(parts[i]).byteValue();
        }
        long ipnum = 0L;
        for (int i = 0; i < 4; i++) {
            long y = addr[i];
            if (y < 0L) {
                y += 256L;
            }
            ipnum += y << (3 - i) * 8;
        }
        return ipnum;
    }

}
