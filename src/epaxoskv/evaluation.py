def calculate_avg_latencies(filename):
    latency_sum = {"PUT": 0, "GET": 0, "APPEND": 0}
    operation_count = {"PUT": 0, "GET": 0, "APPEND": 0}

    # Parse the log and calculate latency sum and count for each operation type
    with open(filename, "r") as file:
        for line in file:
            parts = line.strip().split(": ")
            operation = parts[0]
            latency = int(parts[4].split()[0])
            latency_sum[operation] += latency
            operation_count[operation] += 1

    # Calculate average latency for each operation type
    average_latency = {}
    for operation in latency_sum:
        if operation_count[operation] > 0:
            average_latency[operation] = latency_sum[operation] / operation_count[operation]
        else:
            average_latency[operation] = 0

    # Print average latency for each operation type
    for operation, latency in average_latency.items():
        print(f"{filename} -- Average latency for {operation}: {latency} µs")

for filename in ["epaxosTestSpeed4A_latency.log", "../kvraft/raftTestSpeed4A_latency.log"]:
    calculate_avg_latencies(filename)