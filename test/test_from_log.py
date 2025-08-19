def count_cancel_success(log_file_path):
    keyword = "cancel query success..."
    count = 0

    with open(log_file_path, "r", encoding="utf-8") as f:
        for line in f:
            if keyword in line:
                count += 1

    print(f"Found {count} occurrences of '{keyword}' in {log_file_path}")

if __name__ == "__main__":
    log_path = "/home/hyh/Sqms-On-Postgresql/log/20250419_114931_comming.log"
    count_cancel_success(log_path)
