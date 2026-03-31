package utils;
import java.nio.charset.StandardCharsets;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public final class Parse {

    private Parse() {}

    public static String[] parseMovieLine(String text) {
        if (text == null || text.trim().isEmpty()) {
            return null;
        }
        String[] tokens = text.trim().split(",", 3);
        if (tokens.length < 3) {
            return null;
        }
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].trim();
        }
        return tokens;
    }

    public static String[] parseRatingLine(String text) {
        if (text == null || text.trim().isEmpty()) {
            return null;
        }
        String[] tokens = text.trim().split(",", 4);
        if (tokens.length < 3) {
            return null;
        }
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].trim();
        }
        try {
            Double.parseDouble(tokens[2]);
        } catch (NumberFormatException ex) {
            return null;
        }
        return new String[]{tokens[0], tokens[1], tokens[2]};
    }

    public static String[] parseUserLine(String text) {
        if (text == null || text.trim().isEmpty()) {
            return null;
        }
        String[] tokens = text.trim().split(",", 5);
        if (tokens.length < 3) {
            return null;
        }
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].trim();
        }
        try {
            Integer.parseInt(tokens[2]);
        } catch (NumberFormatException ex) {
            return null;
        }
        return new String[]{tokens[0], tokens[1].toUpperCase(Locale.ROOT), tokens[2]};
    }

    public static String ageBucket(int age) {
        return (age <= 18) ? "0-18" : (age <= 35) ? "18-35" : (age <= 50) ? "35-50" : "50+";
    }

    public static String fmtRating(double val) {
        String formatted = String.format(Locale.ROOT, "%.4f", val);
        while (formatted.endsWith("0") && formatted.contains(".")) {
            formatted = formatted.substring(0, formatted.length() - 1);
        }
        if (formatted.endsWith(".")) {
            formatted = formatted.substring(0, formatted.length() - 1);
        }
        return formatted.isEmpty() ? "0" : formatted;
    }

    public static List<String> readAllLines(InputStream is, Charset charset) throws IOException {
        List<String> list = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset))) {
            String lineData;
            while ((lineData = reader.readLine()) != null) {
                list.add(lineData);
            }
        }
        return list;
    }

    public static Charset sideFileCharset() {
        return StandardCharsets.ISO_8859_1;
    }
}
