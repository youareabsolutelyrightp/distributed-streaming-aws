function to_kib(raw, suffix, number) {
  suffix = substr(raw, length(raw), 1)
  number = substr(raw, 1, length(raw) - 1) + 0
  if (suffix == "G") return number * 1024 * 1024
  if (suffix == "M") return number * 1024
  return number
}

{
  for (field_index = 1; field_index < NF; field_index++) {
    if ($field_index !~ /^[0-9]+[KMG]->[0-9]+[KMG]\([0-9]+[KMG]\)$/) continue
    if ($(field_index + 1) !~ /^[0-9.]+ms$/) continue
    split($field_index, transition, "->")
    split(transition[2], after_and_capacity, "(")
    before = to_kib(transition[1])
    after = to_kib(after_and_capacity[1])
    capacity_raw = after_and_capacity[2]
    sub(/\)$/, "", capacity_raw)
    capacity = to_kib(capacity_raw)
    pause_raw = $(field_index + 1)
    sub(/ms$/, "", pause_raw)
    if (!found || before > maximum_before) maximum_before = before
    latest_after = after
    latest_capacity = capacity
    pause_seconds += (pause_raw + 0) / 1000
    found = 1
  }
}

END {
  if (found) {
    printf "%d %d %.9f %d\n", maximum_before, latest_capacity, pause_seconds, latest_after
  } else {
    print "null null null null"
  }
}
