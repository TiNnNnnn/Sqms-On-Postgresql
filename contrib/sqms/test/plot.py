import math
import os
import time
import psycopg2
import random
import math
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def plot_query_times(query_time_map):
    """
    绘制查询执行时间图，x轴为batch索引，y轴为执行时间。
    参数：
        query_time_map (dict): key为query名, value为每个batch的执行时间列表, -1表示被取消。
    """
    print("paint query times...")
    data = []

    for query_name, time_list in query_time_map.items():
        for batch_index, exec_time in enumerate(time_list):
            data.append({
                "query": query_name,
                "batch": batch_index,
                "time": exec_time,
                "cancelled": exec_time == -1
            })

    df = pd.DataFrame(data)

    plt.figure(figsize=(18, 10))

    # 正常执行的 query 点
    sns.scatterplot(
        data=df[~df["cancelled"]],
        x="batch", y="time", hue="query", style="query", s=100,
        legend='full'
    )

    # 被取消的 query 点（红色X）
    sns.scatterplot(
        data=df[df["cancelled"]],
        x="batch", y="time", hue="query", style="query",
        marker="X", s=150, color="red", legend=False, label="cancelled"
    )

    plt.title("Query Execution Time per Batch (Including Cancelled)")
    plt.xlabel("Batch Index")
    plt.ylabel("Execution Time (s)")

    # 自动计算 legend 的列数（每行显示个数）与行数
    query_num = len(query_time_map)
    ncol = 8 if query_num >= 80 else 6 if query_num >= 60 else 4
    plt.legend(
        bbox_to_anchor=(0.5, -0.25),
        loc='upper center',
        ncol=ncol,
        title="Query",
        frameon=False
    )

    plt.tight_layout()
    plt.savefig("time.png", dpi=300, bbox_inches='tight')  # 保证完整保存
    plt.show()
    plt.close()

def write_query_times_to_excel(query_time_map, filename="query_times.xlsx"):
    # 将数据转换为 DataFrame 格式
    queries = list(query_time_map.keys())
    num_batches = len(next(iter(query_time_map.values())))

    # 构造数据
    data = {
        "query": queries
    }

    for i in range(num_batches):
        data[f"batch_{i}"] = [query_time_map[q][i] for q in queries]

    df = pd.DataFrame(data)
    df.to_excel(filename, index=False)
    print(f"Query times saved to {filename}")