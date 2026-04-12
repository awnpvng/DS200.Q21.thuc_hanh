reviews_raw = LOAD '$INPUT_REVIEW' USING PigStorage(';') AS (
    review_id: int,
    comment: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

comment_norm = FOREACH reviews_raw GENERATE
    review_id,
    category,
    aspect,
    sentiment,
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(LOWER(TRIM(comment)), ',', ' '),
            '.', ' '),
          '!', ' '),
        '?', ' '),
      ';', ' ') AS comment_lc;

words = FOREACH comment_norm GENERATE
    review_id,
    category,
    aspect,
    sentiment,
    FLATTEN(TOKENIZE(comment_lc)) AS word;

words = FILTER words BY word IS NOT NULL AND TRIM(word) != '';

stopwords_raw = LOAD '$INPUT_STOP' USING TextLoader() AS (line:chararray);
stop_trim = FOREACH (FILTER stopwords_raw BY TRIM(line) != '') GENERATE TRIM(LOWER(line)) AS stop;

cog = COGROUP words BY word, stop_trim BY stop;
filtered = FILTER cog BY IsEmpty(stop_trim);
words_clean = FOREACH filtered GENERATE FLATTEN(words);

STORE words_clean INTO '$OUT_TASK1' USING PigStorage(',');
