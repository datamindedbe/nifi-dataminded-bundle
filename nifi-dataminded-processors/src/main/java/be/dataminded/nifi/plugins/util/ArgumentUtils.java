/*
 * Copyright 2017 Data Minded
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package be.dataminded.nifi.plugins.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class ArgumentUtils {
    private final static char QUOTE = '"';
    private final static List<Character> DELIMITING_CHARACTERS = new ArrayList<>(3);

    static {
        DELIMITING_CHARACTERS.add('\t');
        DELIMITING_CHARACTERS.add('\r');
        DELIMITING_CHARACTERS.add('\n');
    }

    public static List<String> splitArgs(final String input, final char definedDelimiter) {
        if (input == null) {
            return Collections.emptyList();
        }

        final List<String> args = new ArrayList<>();

        boolean inQuotes = false;
        final StringBuilder sb = new StringBuilder();

        for (int i = 0; i < input.length(); i++) {
            final char c = input.charAt(i);

            if (DELIMITING_CHARACTERS.contains(c) || c == definedDelimiter) {
                if (inQuotes) {
                    sb.append(c);
                } else {
                    final String arg = sb.toString();
                    args.add(arg);
                    sb.setLength(0);
                }
                continue;
            }

            if (c == QUOTE) {
                inQuotes = !inQuotes;
                continue;
            }

            sb.append(c);
        }

        args.add(sb.toString());

        return args;
    }




    public static Timestamp convertStringToTimestamp(String str_date) {
        try {
            DateFormat formatter;
            formatter = new SimpleDateFormat("yyyy-MM-dd");  //ISO-8601
            Date date = formatter.parse(str_date);
            java.sql.Timestamp timeStampDate = new Timestamp(date.getTime());

            return timeStampDate;
        } catch (ParseException e) {
            logger.error(
                    "Something is very wrong here, one row (even if count is zero) should have been returned: {}",
                    new Object[]{selectQuery});
            System.out.println("Exception :" + e);
            return null;
        }
    }



}