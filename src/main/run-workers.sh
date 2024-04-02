# Run N number of workers on given .so
# Inject crash_percentage if provided

# Usage: ./run-workers.sh <.so> <N>

# Check if the number of arguments is correct
if [ "$#" -ne 2 ]; then
    echo "Usage: ./run-workers.sh <.so> <N>"
    exit 1
fi

# Check if the first argument is a .so file
if [ ! -f "$1" ]; then
    echo "Error: $1 is not a file"
    exit 1
fi

# Check if the second argument is a number
if ! [[ "$2" =~ ^[0-9]+$ ]]; then
    echo "Error: $2 is not a number"
    exit 1
fi

# Run N number of workers
for i in $(seq 1 $2); do
    # Run worker ($1 is the .so file) &
    go run mrworker.go $1 &
done


# Wait for all workers to finish
wait