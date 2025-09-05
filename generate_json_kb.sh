#!/bin/bash

# Usage: ./generate_json_kb.sh <target_kb>
# Example: ./generate_json_kb.sh 1

TARGET_KB=$1
TARGET_BYTES=$((TARGET_KB * 1024))
OUTPUT_FILE="json_test_output.json"

if [ -z "$TARGET_KB" ]; then
  echo "Usage: $0 <target_kilobytes>"
  exit 1
fi

echo "{" > "$OUTPUT_FILE"
i=1
current_size=$(wc -c < "$OUTPUT_FILE")

while [ "$current_size" -lt "$TARGET_BYTES" ]; do
  field_name=$(printf "\"field%04d\"" "$i")
  entry="$field_name:\"value\""

  # Add comma if not the first field
  if [ "$i" -gt 1 ]; then
    echo -n "," >> "$OUTPUT_FILE"
  fi

  echo -n "$entry" >> "$OUTPUT_FILE"
  current_size=$(wc -c < "$OUTPUT_FILE")
  ((i++))
done

echo "}" >> "$OUTPUT_FILE"

final_size=$(wc -c < "$OUTPUT_FILE")
echo "✅ JSON file '$OUTPUT_FILE' generated with size: $final_size bytes (~$((final_size / 1024)) KB)"
