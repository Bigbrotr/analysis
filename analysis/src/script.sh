#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Uso: $0 <file.csv>"
  exit 1
fi

FILE="$1"

if [ ! -f "$FILE" ]; then
  echo "Errore: file '$FILE' non trovato."
  exit 1
fi

# Estrai valori unici della seconda colonna (cioè i "tipi" come x, y, wss://...)
cut -d',' -f2 "$FILE" | tail -n +2 | sort -u > values.txt
mapfile -t values < values.txt

# Genera tutte le coppie
> pairs.txt
for ((i = 0; i < ${#values[@]}; i++)); do
  for ((j = i + 1; j < ${#values[@]}; j++)); do
    echo "${values[i]},${values[j]}" >> pairs.txt
  done
done

# Funzione da eseguire in parallelo (senza file temporanei)
compute_common() {
  IFS=',' read -r v1 v2 <<< "$1"

  count=$(comm -12 \
    <(awk -F',' -v a="$v1" '$2 == a { print $1 }' "$FILE" | sort -u) \
    <(awk -F',' -v b="$v2" '$2 == b { print $1 }' "$FILE" | sort -u) \
    | wc -l)

  if [ "$count" -gt 0 ]; then
    echo "$v1,$v2,$count"
  fi
}

export -f compute_common
export FILE

# Esegui tutto in parallelo, evitando stampa se count = 0
parallel --jobs 0 compute_common :::: pairs.txt

# Cleanup
rm -f values.txt pairs.txt

