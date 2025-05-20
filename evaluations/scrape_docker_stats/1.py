import docker
import pandas as pd
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

client = docker.from_env()

def bytes_to_mb(b):
    return round(b / (1024 * 1024), 2)

def get_container_stats(container):
    try:
        stats = container.stats(stream=False)

        # CPU usage
        cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - stats["precpu_stats"]["cpu_usage"]["total_usage"]
        system_delta = stats["cpu_stats"]["system_cpu_usage"] - stats["precpu_stats"]["system_cpu_usage"]
        cpu_percent = (cpu_delta / system_delta) * len(stats["cpu_stats"]["cpu_usage"].get("percpu_usage", [])) * 100.0 if system_delta > 0.0 else 0.0

        # Memory usage
        memory_usage = stats["memory_stats"]["usage"] / (1024 ** 2)
        memory_limit = stats["memory_stats"].get("limit", 1)
        memory_percent = (memory_usage / (memory_limit / (1024 ** 2))) * 100.0

        # Network I/O
        net_stats = stats.get("networks", {})
        total_rx = sum(iface.get("rx_bytes", 0) for iface in net_stats.values())
        total_tx = sum(iface.get("tx_bytes", 0) for iface in net_stats.values())
        net_input_mb = bytes_to_mb(total_rx)
        net_output_mb = bytes_to_mb(total_tx)

        return {
            "timestamp": datetime.now().isoformat(),
            "container_id": container.short_id,
            "name": container.name,
            "cpu_percent": round(cpu_percent, 2),
            "memory_usage_mib": round(memory_usage, 2),
            "memory_percent": round(memory_percent, 2),
            "net_input_mb": net_input_mb,
            "net_output_mb": net_output_mb
        }

    except Exception as e:
        print(f"Error collecting stats from {container.name}: {e}")
        return None

def collect_all_stats():
    containers = client.containers.list()
    stats_data = []

    with ThreadPoolExecutor(max_workers=len(containers)) as executor:
        future_to_container = {executor.submit(get_container_stats, c): c for c in containers}
        for future in as_completed(future_to_container):
            result = future.result()
            if result:
                stats_data.append(result)

    return stats_data

if __name__ == "__main__":
    try:
        while True:
            data = collect_all_stats()

            # # Tampilkan ke terminal
            # for row in data:
            #     print(row)

            # Simpan ke CSV (append)
            df = pd.DataFrame(data)
            df.to_csv("docker_stats.csv", mode='a', header=not pd.io.common.file_exists("docker_stats.csv"), index=False)

            time.sleep(1)

    except KeyboardInterrupt:
        print("Scraping dihentikan.")