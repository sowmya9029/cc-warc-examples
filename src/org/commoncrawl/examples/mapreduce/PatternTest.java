/**
 * Copyright 2018 Expedia, Inc. All rights reserved.
 * EXPEDIA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.commoncrawl.examples.mapreduce;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by srkonduri on 2/11/18.
 */
public class PatternTest {
    private static String BODY = "asdfdshfhudhudsfhs shd@sdfsf.com zcxdvhjs";
    private static final String EMAIL_PATTERN = "[a-zA-Z0-9]+[._a-zA-Z0-9!#$%&'*+-/=?^_`{|}~]*[a-zA-Z]*@[a-zA-Z0-9]{2,8}.[a-zA-Z.]{2,6}";
    private static Pattern patternTag;
    private static Matcher matcherTag;
    public static void main(String []args) {
        patternTag = Pattern.compile(EMAIL_PATTERN,Pattern.CASE_INSENSITIVE);
        matcherTag = patternTag.matcher(BODY);
        while(matcherTag.find()) {
            System.out.println(matcherTag.group());
        }

    }
}
