package ru.yandex.clickhouse.response;


import com.google.common.primitives.Primitives;
import ru.yandex.clickhouse.ClickHouseArray;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

final class ByteFragmentUtils {

    private static final char ARRAY_ELEMENTS_SEPARATOR = ',';
    private static final char STRING_QUOTATION = '\'';

    private ByteFragmentUtils() {
    }

    static int parseInt(ByteFragment s) throws NumberFormatException {
        if (s == null) {
            throw new NumberFormatException("null");
        }

        if (s.isNull()) {
            return 0; //jdbc spec
        }

        int result = 0;
        boolean negative = false;
        int i = 0, max = s.length();
        int limit;
        int multmin;
        int digit;

        if (max > 0) {
            if (s.charAt(0) == '-') {
                negative = true;
                limit = Integer.MIN_VALUE;
                i++;
            } else {
                limit = -Integer.MAX_VALUE;
            }
            multmin = limit / 10;
            if (i < max) {
                digit = s.charAt(i++) - 0x30; //Character.digit(s.charAt(i++), 10);
                if (digit < 0) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                } else {
                    result = -digit;
                }
            }
            while (i < max) {
                // Accumulating negatively avoids surprises near MAX_VALUE
                digit = s.charAt(i++) - 0x30; // Character.digit(s.charAt(i++), 10);
                if (digit < 0 || digit > 9) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                if (result < multmin) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                result *= 10;
                if (result < limit + digit) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                result -= digit;
            }
        } else {
            throw new NumberFormatException("For input string: \"" + s.asString() + '"');
        }
        if (negative) {
            if (i > 1) {
                return result;
            } else {    /* Only got "-" */
                throw new NumberFormatException("For input string: \"" + s.asString() + '"');
            }
        } else {
            return -result;
        }
    }


    static long parseLong(ByteFragment s) throws NumberFormatException {
        if (s == null) {
            throw new NumberFormatException("null");
        }

        if (s.isNull()) {
            return 0; //jdbc spec
        }

        long result = 0;
        boolean negative = false;
        int i = 0, max = s.length();
        long limit;
        long multmin;
        int digit;

        if (max > 0) {
            if (s.charAt(0) == '-') {
                negative = true;
                limit = Long.MIN_VALUE;
                i++;
            } else {
                limit = -Long.MAX_VALUE;
            }
            multmin = limit / 10;
            if (i < max) {
                digit = s.charAt(i++) - 0x30; // Character.digit(s.charAt(i++), 10);
                if (digit < 0 || digit > 9) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                } else {
                    result = -digit;
                }
            }
            while (i < max) {
                // Accumulating negatively avoids surprises near MAX_VALUE
                digit = s.charAt(i++) - 0x30; // Character.digit(s.charAt(i++), 10);
                if (digit < 0 || digit > 9) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                if (result < multmin) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                result *= 10;
                if (result < limit + digit) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                result -= digit;
            }
        } else {
            throw new NumberFormatException("For input string: \"" + s.asString() + '"');
        }
        if (negative) {
            if (i > 1) {
                return result;
            } else {    /* Only got "-" */
                throw new NumberFormatException("For input string: \"" + s.asString() + '"');
            }
        } else {
            return -result;
        }
    }

    static Object parseArray(ByteFragment value, Class elementClass) {
        return parseArray(value, elementClass, false, null);
    }

    static Object parseArray(ByteFragment value, Class elementClass, SimpleDateFormat dateFormat) {
        return parseArray(value, elementClass, false, dateFormat);
    }

    static Object parseArray(ByteFragment value, Class elementClass, boolean useObjects) {
        return parseArray(value, elementClass, useObjects, null);
    }

    static Object parseArray(ByteFragment value, Class elementClass, boolean useObjects, SimpleDateFormat dateFormat) {
        if (value.isNull()) {
            return null;
        }

        if (value.charAt(0) != '[' || value.charAt(value.length() - 1) != ']') {
            throw new IllegalArgumentException("not an array: " + value);
        }

        if ((elementClass == Date.class || elementClass == Timestamp.class) && dateFormat == null) {
            throw new IllegalArgumentException("DateFormat must be provided for date/dateTime array");
        }

        ByteFragment trim = value.subseq(1, value.length() - 2);

        int index = 0;
        Object array = java.lang.reflect.Array.newInstance(
            useObjects ? elementClass : Primitives.unwrap(elementClass),
            getArrayLength(trim)
        );
        int fieldStart = 0;
        boolean inQuotation = false;
        for (int chIdx = 0; chIdx < trim.length(); chIdx++) {
            int ch = trim.charAt(chIdx);

            if (ch == '\\') {
                chIdx++;
            }
            inQuotation = ch == STRING_QUOTATION ^ inQuotation;

            if (!inQuotation && ch == ARRAY_ELEMENTS_SEPARATOR || chIdx == trim.length() - 1) {
                int fieldEnd = chIdx == trim.length() - 1 ? chIdx + 1 : chIdx;
                if (trim.charAt(fieldStart) == '\'') {
                    fieldStart++;
                    fieldEnd--;
                }
                ArrayByteFragment fragment = ArrayByteFragment.wrap(trim.subseq(fieldStart, fieldEnd - fieldStart));

                if (elementClass == String.class) {
                    String stringValue = fragment.asString(true);
                    java.lang.reflect.Array.set(array, index++, stringValue);
                } else if (elementClass == Long.class) {
                    Long longValue;
                    if (fragment.isNull()) {
                        longValue = useObjects ? null : 0L;
                    } else {
                        longValue = parseLong(fragment);
                    }
                    java.lang.reflect.Array.set(array, index++, longValue);
                } else if (elementClass == Integer.class) {
                    Integer intValue;
                    if (fragment.isNull()) {
                        intValue = useObjects ? null : 0;
                    } else {
                        intValue = parseInt(fragment);
                    }
                    java.lang.reflect.Array.set(array, index++, intValue);
                } else if (elementClass == BigInteger.class) {
                    BigInteger bigIntegerValue;
                    if (fragment.isNull()) {
                        bigIntegerValue = null;
                    } else {
                        bigIntegerValue = new BigInteger(fragment.asString(true));
                    }
                    java.lang.reflect.Array.set(array, index++, bigIntegerValue);
                } else if (elementClass == Float.class) {
                    Float floatValue;
                    if (fragment.isNull()) {
                        floatValue = useObjects ? null : 0.0F;
                    } else if (fragment.isNaN()) {
                        floatValue = Float.NaN;
                    } else {
                        floatValue = Float.parseFloat(fragment.asString());
                    }
                    java.lang.reflect.Array.set(array, index++, floatValue);
                } else if (elementClass == Double.class) {
                    Double doubleValue;
                    if (fragment.isNull()) {
                        doubleValue = useObjects ? null : 0.0;
                    } else if (fragment.isNaN()) {
                        doubleValue = Double.NaN;
                    } else {
                        doubleValue = Double.parseDouble(fragment.asString());
                    }
                    java.lang.reflect.Array.set(array, index++, doubleValue);
                } else if (elementClass == Date.class) {
                    Date dateValue;
                    if (fragment.isNull()) {
                        dateValue = null;
                    } else {
                        try {
                            dateValue = new Date(dateFormat.parse(fragment.asString()).getTime());
                        } catch (ParseException e) {
                            throw new IllegalArgumentException(e);
                        }
                    }
                    java.lang.reflect.Array.set(array, index++, dateValue);
                } else  if (elementClass == Timestamp.class) {
                    Timestamp dateTimeValue;
                    if (fragment.isNull()) {
                        dateTimeValue = null;
                    } else {
                        try {
                            dateTimeValue = new Timestamp(dateFormat.parse(fragment.asString()).getTime());
                        } catch (ParseException e) {
                            throw new IllegalArgumentException(e);
                        }
                    }
                    java.lang.reflect.Array.set(array, index++, dateTimeValue);
                } else  {
                    throw new IllegalStateException();
                }

                fieldStart = chIdx + 1;
            }
        }

        return array;
    }

    private static int getArrayLength(ByteFragment value) {
        if (value.length() == 0) {
            return 0;
        }

        int length = 1;
        boolean inQuotation = false;
        for (int i = 0; i < value.length(); i++) {
            int ch = value.charAt(i);

            if (ch == '\\') {
                i++;
            }

            inQuotation = ch == STRING_QUOTATION ^ inQuotation;

            if (!inQuotation && ch == ARRAY_ELEMENTS_SEPARATOR) {
                length++;
            }
        }
        return length;
    }

    static Object parseArrayOfArrays(ByteFragment value, Class elementClass, boolean useObjects,
                                     int elementType, boolean isUnsigned, SimpleDateFormat dateFormat) {
        value = value.subseq(1, value.length() - 2);
        List<ByteFragment> byteFragments = nestedByteFragments(value);

        Object array = java.lang.reflect.Array.newInstance(ClickHouseArray.class, byteFragments.size());

        int index = 0;

        for (ByteFragment byteFragment : byteFragments) {
            Object parsedArray = parseArray(byteFragment, elementClass, useObjects, dateFormat);
            ClickHouseArray clickHouseArray = new ClickHouseArray(elementType, isUnsigned, parsedArray);
            java.lang.reflect.Array.set(array, index, clickHouseArray);
            index++;
        }
        return array;
    }

    private static List<ByteFragment> nestedByteFragments(ByteFragment value) {

        List<ByteFragment> result = new ArrayList<ByteFragment>();
        List<Integer> indices = getArrayByteFragmentsIndices(value);
        for (int i = 0; i < indices.size() - 1; i ++) {
            int start = indices.get(i) + 2;
            int length = indices.get(i + 1) - indices.get(i) - 1;
            result.add(value.subseq(start, length));
        }
        return result;
    }

    private static List<Integer> getArrayByteFragmentsIndices(ByteFragment value) {
        int index = -2;

        List<Integer> resultList = new ArrayList<Integer>();

        while(index != -1) {
            resultList.add(index);
            index = getNextIndex(index + 2, value);
        }

        return  resultList;
    }

    private static int getNextIndex(int startValue, ByteFragment value) {
        if (startValue > value.getLen()) {
            return -1;
        }
        int chInd = 0;
        int nestedLevel = 1;
        boolean inQuotation = false;
        while (nestedLevel != 0) {
            int ch = value.charAt(chInd + startValue + 1);
            inQuotation = ch == STRING_QUOTATION ^ inQuotation;
            if (!inQuotation ) {
                if (ch == '[') {
                    nestedLevel++;
                } else if (ch == ']') {
                    nestedLevel--;
                }
            } else if (ch == '\\') {
                chInd++;
            }
            chInd++;
        }
        return chInd + startValue;
    }
}
