import pandas as pd
import matplotlib.pyplot as plt
import os

# === CONFIG ===
MYSQL_DIR = "results_with_indexes/mySQL"
NEO4J_DIR = "results_with_indexes/neo4j"
MYSQL_SUMMARY = "results_with_indexes/mySQL/mysql_summary.csv"
NEO4J_SUMMARY = "results_with_indexes/neo4j/neo4j_summary.csv"
OUTPUT_DIR = "results_with_indexes/plots"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_results(file_path):
    """Carica i risultati CSV di una query."""
    return pd.read_csv(file_path)

def plot_comparison(mysql_df, neo4j_df, query_name):
    """Genera grafici comparativi per una singola query."""
    plt.figure()
    plt.plot(mysql_df.index, mysql_df["time_ms"], marker="o", label="MySQL")
    plt.plot(neo4j_df.index, neo4j_df["time_ms"], marker="o", label="Neo4j")
    plt.title(f"Execution Times - {query_name}")
    plt.xlabel("Execution")
    plt.ylabel("Time (ms)")
    plt.legend()
    plt.savefig(f"{OUTPUT_DIR}/{query_name}_lineplot.png")
    plt.close()

    # Boxplot
    # plt.figure()
    # plt.boxplot(
    #     [mysql_df["time_ms"], neo4j_df["time_ms"]],
    #     tick_labels=["MySQL", "Neo4j"]
    # )
    # plt.title(f"Distribuzione tempi - {query_name}")
    # plt.ylabel("Tempo (ms)")
    # plt.savefig(f"{OUTPUT_DIR}/{query_name}_boxplot.png")
    # plt.close()

def plot_summary(mysql_summary, neo4j_summary):
    """Grafico comparativo tempi medi su tutte le query."""
    plt.figure(figsize=(12, 6))  # figura più larga
    x = range(len(mysql_summary))

    # barre MySQL e Neo4j
    plt.bar([i-0.2 for i in x], mysql_summary["avg_ms"], width=0.4, label="MySQL")
    plt.bar([i+0.2 for i in x], neo4j_summary["avg_ms"], width=0.4, label="Neo4j")

    # etichette più leggibili (rotazione + allineamento)
    plt.xticks(x, mysql_summary["query_name"], rotation=45, ha="right")

    plt.title("Average Execution Time Comparison (log scale)")
    plt.ylabel("Average Time (ms)")
    plt.yscale("log")  # manteniamo scala log per confronti migliori
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/summary_comparison_log.png")
    plt.close()

def plot_graphs():
    # Carica i summary
    mysql_summary = pd.read_csv(MYSQL_SUMMARY)
    neo4j_summary = pd.read_csv(NEO4J_SUMMARY)

    # Grafico riassuntivo generale
    plot_summary(mysql_summary, neo4j_summary)

    # Cicla sulle query (assumendo stesso ordine nei summary)
    for query in mysql_summary["query_name"]:
        mysql_file = os.path.join(MYSQL_DIR, f"mysql_{query}.csv")
        neo4j_file = os.path.join(NEO4J_DIR, f"neo4j_{query}.csv")

        if os.path.exists(mysql_file) and os.path.exists(neo4j_file):
            mysql_df = load_results(mysql_file)
            neo4j_df = load_results(neo4j_file)
            plot_comparison(mysql_df, neo4j_df, query)

if __name__ == "__main__":
    # Carica i summary
    plot_graphs()