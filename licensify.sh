#!/bin/bash

DIR="$1"
BP="${2-license-boilerplate}"
exec 3<&0

function get_boilerplate() {
  FILE="$1"
  cat "$BP" | case "$FILE" in
  *.c|*.h|*.cpp|*.hpp)
    awk 'BEGIN{print "/*"} {print " * " $0} END{print " */\n"}'
    ;;
  *.am|*.ac|*.m4|*.pc.in)
    awk 'BEGIN{print "#"} {print "# " $0} END{print "#\n"}'
    ;;
  *.lua)
    awk 'BEGIN{print "--"} {print "-- " $0} END{print "--\n"}'
    ;;
  esac
}

TMPFILE=$(mktemp)
function add_boilerplate() {
  truncate -s0 $TMPFILE
  cat <(get_boilerplate "$1") "$1" >> "$TMPFILE"
  cp "$TMPFILE" "$1"
}

find "$DIR" \
  -name "*.cpp" -or -name "*.hpp" -or \
  -name "*.c" -or -name "*.h" -or \
  -name "*.am" -or -name "*.ac" -or -name "*.m4" -or -name "*.pc.in" -or \
  -name "*.lua" \
| while read SRCFILE; do
  if ! grep -q 'Apache' $SRCFILE; then
    echo -n "$SRCFILE: Add license? "
    while read -u3 ans; do
      if [[ $ans == 'y' || $ans == '' ]]; then
        echo "Licensifying..."
        add_boilerplate "$SRCFILE"
        break
      elif [[ $ans != 'n' ]]; then
        echo "Skipping..."
        break
      else
        echo "Try again"
      fi
    done
  fi
done

rm -f "$TMPFILE"
