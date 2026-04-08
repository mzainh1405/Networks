 #!/bin/bash

HOSTS=("10.200.51.18" "10.200.51.19")
PORT=4022
ATTEMPT=0

while true; do
    ATTEMPT=$((ATTEMPT + 1))
    echo "=== Attempt $ATTEMPT ==="

    for HOST in "${HOSTS[@]}"; do
        echo "Trying $HOST:$PORT..."

        RESPONSE=$(nc -w 3 "$HOST" "$PORT")

        FLAG=$(echo "$RESPONSE" | grep -o 'IN2011:[^ ]*')
        EXPECTED=$(echo "$RESPONSE" | grep -o '[a-f0-9]\{64\}' | tail -1)

        if [ -z "$FLAG" ] || [ -z "$EXPECTED" ]; then
            echo "No valid response from $HOST, skipping..."
            continue
        fi

        ACTUAL=$(echo -n "$FLAG" | sha256sum | cut -d' ' -f1)

        echo "Expected: $EXPECTED"
        echo "Actual:   $ACTUAL"

        if [ "$ACTUAL" = "$EXPECTED" ]; then
            echo ""
            echo ":white_check_mark: VALID FLAG FOUND on $HOST!"
            echo "$FLAG"
            exit 0
        else
            echo ":x: Tampered on $HOST, trying again..."
        fi
    done

    echo ""
    sleep 1  # wait a second before retrying
done 