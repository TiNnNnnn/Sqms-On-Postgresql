import math
import os
import time
import psycopg2
import random
import math
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import xlsxwriter
from scipy.interpolate import make_interp_spline

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

def write_query_times_to_excel(query_time_map, filename="output/query_times.xlsx"):
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

def write_average_query_times_to_excel(query_time_map, filename="output/query_average_time_URwLoad.xlsx"):
    # 将数据转换为 DataFrame 格式
    queries = list(query_time_map.keys())

    # 构造数据
    data = {
        "query": queries
    }

    data[f"avg_time"] = [query_time_map[q] for q in queries]

    df = pd.DataFrame(data)
    df.to_excel(filename, index=False)
    print(f"Query times saved to {filename}")

def plot_query_time(query_time_map, title, output_path):
    x = np.array(list(query_time_map.keys()))
    y = np.array(list(query_time_map.values()))

    if len(x) < 4:
        # 不足以平滑时，退回原始折线图
        print("Too few points for smoothing; drawing raw line.")
        plt.figure(figsize=(10, 6))
        plt.plot(x, y, marker='o')
    else:
        # 插值生成更多平滑点
        x_new = np.linspace(x.min(), x.max(), 300)
        spl = make_interp_spline(x, y, k=3)  # k=3 表示三次样条
        y_smooth = spl(x_new)

        plt.figure(figsize=(10, 6))
        plt.plot(x_new, y_smooth, label="Smoothed", color='blue')

    plt.xlabel("Number of successful queries")
    plt.ylabel("Total time (s)")
    plt.title(title)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

def write_batch_query_time_to_excel(query_time_map, output_path):
    workbook = xlsxwriter.Workbook(output_path)
    worksheet = workbook.add_worksheet()

    worksheet.write(0, 0, "Query Count")
    worksheet.write(0, 1, "Total Time (s)")

    for i, (k, v) in enumerate(query_time_map.items(), start=1):
        worksheet.write(i, 0, k)
        worksheet.write(i, 1, v)

    workbook.close()


def write_query_time_to_excel(query_run_map, output_path):
    workbook = xlsxwriter.Workbook(output_path)
    worksheet = workbook.add_worksheet()

    max_len = max(len(v) for v in query_run_map.values())
    worksheet.write(0, 0, "Query Count")
    for col in range(max_len):
        worksheet.write(0, col + 1, f"Value {col+1}")

    for row, (k, v) in enumerate(query_run_map.items(), start=1):
        worksheet.write(row, 0, k)
        for col, item in enumerate(v):
            worksheet.write(row, col + 1, item)

    workbook.close()

def plot_from_excel(input_excel_path, output_image_path):
    df = pd.read_excel(input_excel_path, engine='openpyxl') 
    df.columns = df.columns.astype(str).str.strip()

    def clean_column(series):
        return series.astype(str).str.replace(r'_x000d_', '', regex=True).str.strip().astype(float)

    print(df.columns)
    query_counts = clean_column(df["Query Count"])
    with_index = clean_column(df["1"])
    without_index = clean_column(df["2"])

    plt.figure(figsize=(10, 6))
    plt.rcParams.update({
        'font.size': 24,            # 所有字体大小
        'axes.titlesize': 24,       # 标题字体大小
        'axes.labelsize': 24,       # 坐标轴标签字体大小
        'xtick.labelsize': 20,      # x轴刻度字体
        'ytick.labelsize': 20,      # y轴刻度字体
        'legend.fontsize': 18,      # 图例字体大小
        'axes.linewidth': 2,   
    })

    if len(query_counts) >= 3:
        # 插值平滑曲线
        x_new = np.linspace(query_counts.min(), query_counts.max(), 300)

        spl_with = make_interp_spline(query_counts, with_index, k=2)
        spl_without = make_interp_spline(query_counts, without_index, k=2)

        y_with_smooth = spl_with(x_new)
        y_without_smooth = spl_without(x_new)

        plt.plot(x_new, y_with_smooth, label="Without Core Subquery Excavating", color='green',linewidth=3)
        plt.plot(x_new, y_without_smooth, label="With Core Subquery Excavating", color='blue',linewidth=3)

        # 原始点
        #plt.scatter(query_counts, with_index, color='blue')
        #plt.scatter(query_counts, without_index, color='red')
    else:
        # 数据点太少，不进行插值
        plt.plot(query_counts, with_index, marker='o', label="With Index", color='blue',linewidth=3)
        plt.plot(query_counts, without_index, marker='o', label="Without Index", color='red',linewidth=3)

    plt.xlabel("Query Count")
    plt.ylabel("Total Time (s)")
    plt.title("Workload runtime ")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    # 创建输出目录
    os.makedirs(os.path.dirname(output_image_path), exist_ok=True)
    plt.savefig(output_image_path, bbox_inches='tight', dpi=300)
    plt.show()

def plot_grouped_bar_chart(output_path):
    methods = ['QPPNet', 'SlowViews', 'SlowViews(Warm)', 'PlanBase',]
    colors = ['#8FBC8F', 'skyblue', 'orange', 'pink']

    plt.rcParams.update({
        'font.size': 18,            # 所有字体大小
        'axes.titlesize': 20,       # 标题字体大小
        'axes.labelsize': 20,       # 坐标轴标签字体大小
        'xtick.labelsize': 18,      # x轴刻度字体
        'ytick.labelsize': 18,      # y轴刻度字体
        'legend.fontsize': 12,      # 图例字体大小
        'axes.linewidth': 2,   
    })

    workloads = ['RW-Load', 'URW-Load']
    relative_error = {
        'RW-Load': [97, 77, 91, 30],
        'URW-Load':  [98, 71, 85, 29]
    }
    mean_absolute_error = {
        'RW-Load': [67, 96, 95, 97],
        'URW-Load':  [65, 88, 89, 98]
    }

    def plot_single_chart(ax, data_dict, ylabel, title):
        bar_width = 0.1
        group_spacing = 0.4
        method_count = len(methods)
        workload_count = len(workloads)

        total_group_width = method_count * bar_width
        indices = np.arange(workload_count) * (total_group_width + group_spacing)

        bars = []
        for i, method in enumerate(methods):
            values = [data_dict[workload][i] for workload in workloads]
            bar = ax.bar(indices + i * bar_width, values, width=bar_width,
             label=method, color=colors[i],
             edgecolor='black', linewidth=1)
            bars.append(bar)

        ax.set_xticks(indices + (method_count / 2 - 0.5) * bar_width)
        ax.set_xticklabels(workloads)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        # 不显示横向网格线
        ax.grid(False)
        return bars

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    bars1 = plot_single_chart(ax1, relative_error, 'Recall (%)', 'Recall per Workload')
    bars2 = plot_single_chart(ax2, mean_absolute_error, 'Precision (%)', 'Precision per Workload')

    # 设置统一图例位置：放在图两张子图之间上方中央
    handles = [bar[0] for bar in bars1]  # 取每种方法的 bar 对象（一个即可）
    labels = methods
    fig.legend(handles, labels, loc='upper center', ncol=len(methods), fontsize='medium')

    plt.tight_layout(rect=[0, 0, 1, 0.92])  # 留出上方空间放 legend
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.show()


if __name__ == "__main__":
    plot_grouped_bar_chart("output/compare_accuracy")
    #plot_from_excel("/home/hyh/Sqms-On-Postgresql/contrib/sqms/test2/output/20250616_105812_without_excavate/query_batch_time.xlsx","output/core_subquery_compare.png")