package utils;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class SideTables {

    public static final class UserRow {
        public final String gender;
        public final int age;

        public UserRow(String gender, int age) {
            this.gender = gender;
            this.age = age;
        }
    }

    private SideTables() {}

    private static boolean matchFileName(String rawPath, String targetSuffix) {
        if (rawPath != null) {
            String normalized = rawPath.replace('\\', '/');
            return normalized.endsWith(targetSuffix);
        }
        return false;
    }

    public static Map<String, String> loadMovieTitles(Configuration cfg, URI[] cacheUris, String targetFile) throws IOException {
        Map<String, String> titlesDict = new HashMap<>();
        if (cacheUris != null) {
            Charset charset = Parse.sideFileCharset();
            for (URI uri : cacheUris) {
                if (matchFileName(uri.getPath(), targetFile)) {
                    try (InputStream stream = setupStream(cfg, uri)) {
                        for (String rowData : Parse.readAllLines(stream, charset)) {
                            String[] parsed = Parse.parseMovieLine(rowData);
                            if (parsed != null) {
                                titlesDict.put(parsed[0], parsed[1]);
                            }
                        }
                    }
                    break;
                }
            }
        }
        return titlesDict;
    }

    public static Map<String, List<String>> loadGenresByMovie(Configuration cfg, URI[] cacheUris, String targetFile) throws IOException {
        Map<String, List<String>> genresDict = new HashMap<>();
        if (cacheUris != null) {
            Charset charset = Parse.sideFileCharset();
            for (URI uri : cacheUris) {
                if (matchFileName(uri.getPath(), targetFile)) {
                    try (InputStream stream = setupStream(cfg, uri)) {
                        for (String rowData : Parse.readAllLines(stream, charset)) {
                            String[] parsed = Parse.parseMovieLine(rowData);
                            if (parsed != null) {
                                String mId = parsed[0];
                                String rawGenres = parsed[2];
                                List<String> list = new ArrayList<>();
                                String[] splitted = rawGenres.split("\\|");
                                for (String genre : splitted) {
                                    String trimmed = genre.trim();
                                    if (!trimmed.isEmpty()) {
                                        list.add(trimmed);
                                    }
                                }
                                genresDict.put(mId, list);
                            }
                        }
                    }
                    break;
                }
            }
        }
        return genresDict;
    }

    public static Map<String, UserRow> loadUsers(Configuration cfg, URI[] cacheUris, String targetFile) throws IOException {
        Map<String, UserRow> usersDict = new HashMap<>();
        if (cacheUris != null) {
            Charset charset = Parse.sideFileCharset();
            for (URI uri : cacheUris) {
                if (matchFileName(uri.getPath(), targetFile)) {
                    try (InputStream stream = setupStream(cfg, uri)) {
                        for (String rowData : Parse.readAllLines(stream, charset)) {
                            String[] parsed = Parse.parseUserLine(rowData);
                            if (parsed != null) {
                                usersDict.put(parsed[0], new UserRow(parsed[1], Integer.parseInt(parsed[2])));
                            }
                        }
                    }
                    break;
                }
            }
        }
        return usersDict;
    }

    private static InputStream setupStream(Configuration cfg, URI uri) throws IOException {
        Path filePath = new Path(uri);
        FileSystem system = FileSystem.get(uri, cfg);
        return system.open(filePath);
    }
}
