import argparse,shutil
import csv
import math
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Tuple, Set, Any
from GeneraGrafici import plot_graphs
from MySql import mainMySql
from Neo4j import mainNeo4j

def _to_number_or_str(v: str) -> Any:
    """Prova a convertire in int/float; altrimenti stringa invariata."""
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return ""
    # int?
    try:
        i = int(s)
        return i
    except ValueError:
        pass
    # float?
    try:
        f = float(s)
        return f
    except ValueError:
        return s


def load_table(path: Path) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Legge CSV (header + righe come dict)."""
    if not path.exists():
        return [], []
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        try:
            header = next(r)
        except StopIteration:
            return [], []
        rows = []
        for row in r:
            row = row + [""] * (len(header) - len(row))  # pad se mancano colonne
            obj = {header[i]: _to_number_or_str(row[i]) for i in range(len(header))}
            rows.append(obj)
        return header, rows


def _norm_value(v: Any) -> Any:
    """Normalizza i valori per il confronto insiemistico."""
    if isinstance(v, float):
        # arrotonda per stabilizzare il confronto
        if math.isnan(v):
            return "NaN"
        return round(v, 1)
    return v


def rows_to_keyset(rows: List[Dict[str, Any]], columns: List[str]) -> Set[Tuple]:
    """Converte lista di dict in set di tuple ordinate secondo columns."""
    keyset = set()
    for row in rows:
        key = tuple(_norm_value(row.get(col)) for col in columns)
        keyset.add(key)
    return keyset


def compare_two_csv(mysql_csv: Path, neo4j_csv: Path,REPORTS_DIR) -> Dict[str, Any]:
    """Confronta due risultati (stesse colonne in comune)."""
    mh, mr = load_table(mysql_csv)
    nh, nr = load_table(neo4j_csv)

    # intersezione colonne; se vuota, confronto impossibile
    common_cols = [c for c in mh if c in nh]
    if not common_cols:
        return {
            "query": mysql_csv.stem,
            "status": "no_common_columns",
            "mysql_rows": len(mr),
            "neo4j_rows": len(nr),
            "only_mysql": 0,
            "only_neo4j": 0,
            "details": f"No common columns between {mh} and {nh}",
        }

    mset = rows_to_keyset(mr, common_cols)
    nset = rows_to_keyset(nr, common_cols)

    only_m = mset - nset
    only_n = nset - mset
    equal = len(only_m) == 0 and len(only_n) == 0

    # salva un diff dettagliato per la query
    diff_path = REPORTS_DIR / f"diff_{mysql_csv.stem}.csv"
    with open(diff_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["side", *common_cols])
        for t in sorted(only_m):
            w.writerow(["only_mysql", *t])
        for t in sorted(only_n):
            w.writerow(["only_neo4j", *t])

    return {
        "query": mysql_csv.stem,
        "status": "equal" if equal else "different",
        "mysql_rows": len(mr),
        "neo4j_rows": len(nr),
        "only_mysql": len(only_m),
        "only_neo4j": len(only_n),
        "details": f"Diff saved to {diff_path.name}",
    }


def find_common_query_files(MYSQL_DIR,NEO4J_DIR) -> List[Tuple[Path, Path]]:
    """Trova le coppie di file risultato con lo stesso nome in mysql/ e neo4j/."""
    mysql_files = {p.stem: p for p in MYSQL_DIR.glob("*.csv")}
    neo4j_files = {p.stem: p for p in NEO4J_DIR.glob("*.csv")}
    common = sorted(set(mysql_files.keys()) & set(neo4j_files.keys()))
    return [(mysql_files[name], neo4j_files[name]) for name in common]


def write_summary_report(results: List[Dict[str, Any]],REPORTS_DIR) -> Path:
    out = REPORTS_DIR / "comparison_summary.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "query",
                "status",
                "mysql_rows",
                "neo4j_rows",
                "only_in_mysql",
                "only_in_neo4j",
                "details",
            ]
        )
        for r in results:
            w.writerow(
                [
                    r["query"],
                    r["status"],
                    r["mysql_rows"],
                    r["neo4j_rows"],
                    r["only_mysql"],
                    r["only_neo4j"],
                    r.get("details", ""),
                ]
            )
    return out


def main():
    parser = argparse.ArgumentParser(
    description="Run benchmarks and compare last-run query results between MySQL and Neo4j."
    )
    parser.add_argument("--run", action="store_true", help="Run mysql.py and neo4j.py before comparing.")
    parser.add_argument("--use_index", action="store_true", help="Usa gli indici e salva in result_with_indexes/")
    args = parser.parse_args()

    # Root dinamico
    use_indexes = args.use_index
    RESULTS_ROOT = Path("results_with_indexes") if args.use_index else Path("results")
    MYSQL_DIR = RESULTS_ROOT / "MySql"
    NEO4J_DIR = RESULTS_ROOT / "Neo4j"
    REPORTS_DIR = RESULTS_ROOT / "reports"
    PLOTS_DIR = RESULTS_ROOT / "plots"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    for folder in [MYSQL_DIR, NEO4J_DIR, REPORTS_DIR,PLOTS_DIR]:
        folder.mkdir(parents=True, exist_ok=True)
        # Elimina tutti i file all'interno
        for f in folder.iterdir():
            if f.is_file():
                f.unlink()
            elif f.is_dir():
                shutil.rmtree(f)

    if args.run:
        mainMySql(RESULTS_ROOT,use_indexes)
        mainNeo4j(RESULTS_ROOT,use_indexes)

    pairs = find_common_query_files(MYSQL_DIR,NEO4J_DIR)
    if not pairs:
        sys.exit(
            f"No common result files found in {MYSQL_DIR} and {NEO4J_DIR}. Make sure both scripts saved CSVs with the same base names."
        )

    results = []
    print("▶️ Comparing results…")
    for mfile, nfile in pairs:
        r = compare_two_csv(mfile, nfile,REPORTS_DIR)
        results.append(r)
        status_icon = "✅" if r["status"] == "equal" else "❌"
        print(
            f"{status_icon} {r['query']}: {r['status']} "
            f"(rows mysql={r['mysql_rows']}, neo4j={r['neo4j_rows']}, "
            f"only_mysql={r['only_mysql']}, only_neo4j={r['only_neo4j']})"
        )

    summary_path = write_summary_report(results,REPORTS_DIR)
    print(f"\n📄 Summary written to {summary_path}")
    print(f"📄 Per-query diffs written to {REPORTS_DIR}/diff_*.csv")
    plot_graphs(RESULTS_ROOT)

if __name__ == "__main__":
    main()