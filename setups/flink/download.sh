#!/bin/bash

# Daftar URL untuk file JAR Hadoop tambahan
urls=(
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/3.3.4/hadoop-client-3.3.4.jar"
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
)

# Loop untuk mengunduh setiap file
for url in "${urls[@]}"; do
    echo "Mengunduh: $url"
    wget "$url"
    if [ $? -eq 0 ]; then
        echo "Berhasil mengunduh $(basename "$url")"
    else
        echo "Gagal mengunduh $(basename "$url")"
    fi
done