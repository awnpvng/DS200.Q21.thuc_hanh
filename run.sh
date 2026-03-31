#!/usr/bin/env bash
# Run Lab 01 Tasks 1-4 as Java MapReduce
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/Lab01" && pwd)"
JAVA_PROJ="$ROOT"
DATA="$ROOT/data"
OUT="$ROOT/output"
WORK="$OUT/.mr_work_java"

for cmd in mvn java; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing '$cmd' on PATH. Install Maven and Java, then retry." >&2
    exit 1
  fi
done

# Compile classes
( cd "$JAVA_PROJ" && mvn compile )

echo "Building Classpath for Java execution..."
( cd "$JAVA_PROJ" && mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt )
CP="$(cat "$JAVA_PROJ/cp.txt"):$JAVA_PROJ/target/classes"

MERGED="$(mktemp)"
trap 'rm -f "$MERGED" "$JAVA_PROJ/cp.txt"' EXIT
{
  cat "$DATA/ratings_1.txt"
  echo
  cat "$DATA/ratings_2.txt"
} > "$MERGED"

mkdir -p "$OUT" "$WORK"

ABS_MERGED="$(cd "$(dirname "$MERGED")" && pwd)/$(basename "$MERGED")"
ABS_MOVIES="$DATA/movies.txt"
ABS_USERS="$DATA/users.txt"

# MapReduce local mode requires Java 17 module opens to prevent initialization crashes
export HADOOP_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"

echo "Task 1 (Java MapReduce, two stages)..."
java $HADOOP_OPTS -cp "$CP" task1.Driver "$ABS_MERGED" "$ABS_MOVIES" "$WORK" "$OUT/output_task_1.txt"

echo "Task 2..."
java $HADOOP_OPTS -cp "$CP" task2.Driver "$ABS_MERGED" "$ABS_MOVIES" "$WORK" "$OUT/output_task_2.txt"

echo "Task 3..."
java $HADOOP_OPTS -cp "$CP" task3.Driver "$ABS_MERGED" "$ABS_USERS" "$ABS_MOVIES" "$WORK" "$OUT/output_task_3.txt"

echo "Task 4..."
java $HADOOP_OPTS -cp "$CP" task4.Driver "$ABS_MERGED" "$ABS_USERS" "$ABS_MOVIES" "$WORK" "$OUT/output_task_4.txt"

sort -o "$OUT/output_task_3.txt" "$OUT/output_task_3.txt"
sort -o "$OUT/output_task_4.txt" "$OUT/output_task_4.txt"

echo "Done. Reports: $OUT/output_task_1.txt ... output_task_4.txt"
