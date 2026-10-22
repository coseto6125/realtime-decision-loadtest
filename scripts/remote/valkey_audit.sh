#!/usr/bin/env bash
# Key and encoding audit on lt-redis: encodings and bytes per key type on a sample of users.
set -u
C="docker exec lt-redis valkey-cli"
echo "## config"
for k in maxmemory maxmemory-policy appendonly appendfsync io-threads lazyfree-lazy-user-del hash-max-listpack-entries hash-max-listpack-value list-max-listpack-size zset-max-listpack-entries zset-max-listpack-value; do
  echo "$k=$($C CONFIG GET $k | tail -1)"
done
echo "## keyspace"; $C INFO keyspace | tail -n +2
echo "## memory"; $C INFO memory | grep -E '^(used_memory|used_memory_rss|used_memory_dataset|mem_fragmentation_ratio):'
for pat in 'ctx:*' 'ev:*' 'dd:*' 'cap:*'; do
  # one valkey-cli session per pattern: alternate ENCODING / MEMORY USAGE replies, aggregated by awk
  $C --scan --pattern "$pat" --count 1000 | head -200 \
    | while read -r k; do printf 'OBJECT ENCODING %s\nMEMORY USAGE %s SAMPLES 0\n' "$k" "$k"; done \
    | docker exec -i lt-redis valkey-cli \
    | awk -v pat="$pat" 'NR % 2 == 1 { enc[$0]++; n++ } NR % 2 == 0 { b += $0 }
        END { printf "## %s sample=%d avg_bytes=%d encodings:", pat, n, n ? b / n : 0; for (e in enc) printf " %s=%d", e, enc[e]; print "" }'
done
echo "## sample ctx hash"; k=$($C --scan --pattern 'ctx:*' --count 1000 | head -1); echo "$k fields=$($C HLEN "$k")"; $C HGETALL "$k" | paste - - | head -30
echo "## streams"; for s in events events:quarantine; do echo "$s len=$($C XLEN $s) mem=$($C MEMORY USAGE $s)"; done
$C XINFO STREAM events | paste - - | head -8
echo "## SLOWLOG GET 10"; $C SLOWLOG GET 10
echo "## LATENCY DOCTOR"; $C LATENCY DOCTOR
