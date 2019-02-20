package com.cwk.springbootkafkahbase.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TimeUtils {

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        String scn_time = "2017-12-24 17:55:23";

        try {
            Long scnTimeMs = TimeUtils.sdf.parse(scn_time).getTime();
            System.out.println("scnTimeMs = " + scnTimeMs);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

}
