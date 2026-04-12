set -e

export JAVA_HOME="/d/Java/jdk-17"
export HADOOP_HOME="/d/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_USER_NAME="$(whoami)"
export PIG_HOME="/d/pig-0.18.0"
export PATH="$PIG_HOME/bin:$HADOOP_HOME/bin:$PATH"

mkdir -p /tmp/hadoop-${HADOOP_USER_NAME}

ROOT="$(cd "$(dirname "$0")" && pwd)"
PIG_DIR="$ROOT/src"
PARAMS="$PIG_DIR/params.properties"

if ! command -v pig >/dev/null 2>&1; then
  echo "Apache Pig not found."
  exit 1
fi

cat >"$PARAMS" <<EOF
INPUT_REVIEW=$ROOT/dataset/hotel-review.csv
INPUT_STOP=$ROOT/dataset/stopwords.txt
OUT_TASK1=$ROOT/output/task_1_out
OUT_TASK2A=$ROOT/output/task_2a_out
OUT_TASK2B=$ROOT/output/task_2b_out
OUT_TASK2C=$ROOT/output/task_2c_out
OUT_TASK3A=$ROOT/output/task_3a_out
OUT_TASK3B=$ROOT/output/task_3b_out
OUT_TASK4A=$ROOT/output/task_4a_out
OUT_TASK4B=$ROOT/output/task_4b_out
OUT_TASK5=$ROOT/output/task_5_out
EOF

echo "Running as user: $(whoami)"
echo "Lab root: $ROOT"

rm -rf "$ROOT/output/task_"*

pig -x local -param_file "$PARAMS" -f "$PIG_DIR/task_1.pig"
pig -x local -param_file "$PARAMS" -f "$PIG_DIR/task_2.pig"
pig -x local -param_file "$PARAMS" -f "$PIG_DIR/task_3.pig"
pig -x local -param_file "$PARAMS" -f "$PIG_DIR/task_4.pig"
pig -x local -param_file "$PARAMS" -f "$PIG_DIR/task_5.pig"

for d in "$ROOT"/output/*_out; do
  if [ -d "$d" ]; then
    base=$(basename "$d" "_out")
    if [ -f "$d/part-r-00000" ]; then
      mv "$d/part-r-00000" "$ROOT/output/$base.csv"
    fi
    rm -rf "$d" 
  fi
done

echo "Pig outputs have been converted to $ROOT/output/*.csv files."
