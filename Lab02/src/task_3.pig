reviews_raw = LOAD '$INPUT_REVIEW' USING PigStorage(';') AS (
    review_id: int,
    comment: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- Negative counts per aspect
neg_only = FILTER reviews_raw BY sentiment == 'negative';
by_aspect_neg = GROUP neg_only BY aspect;
neg_counts = FOREACH by_aspect_neg GENERATE group AS aspect, COUNT(neg_only) AS cnt;
neg_ordered = ORDER neg_counts BY cnt DESC;
STORE neg_ordered INTO '$OUT_TASK3A' USING PigStorage(',');

-- Positive counts per aspect
pos_only = FILTER reviews_raw BY sentiment == 'positive';
by_aspect_pos = GROUP pos_only BY aspect;
pos_counts = FOREACH by_aspect_pos GENERATE group AS aspect, COUNT(pos_only) AS cnt;
pos_ordered = ORDER pos_counts BY cnt DESC;
STORE pos_ordered INTO '$OUT_TASK3B' USING PigStorage(',');
