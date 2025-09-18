from pathlib import Path
import mysql.connector
import time
import statistics
import csv
import traceback
from datetime import datetime


# Connection config
CONFIG = {
    "user": "root",
    "password": "root",
    "host": "127.0.0.1",  
    "port": 3306,          
    "database": "movielens"
}

# ------------------------------
# Parametri benchmark
# ------------------------------
REPEATS = 10 # numero di misure per query
WARMUP_RUNS = 1        # esecuzioni di warm-up per query (scartate)
OUTPUT_PREFIX = "mysql"  # prefisso file csv
RESULTS_DIR = Path("results_with_indexes/mysql")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------
# Definizione delle query
# - name: nome breve usato nei file
# - sql: stringa SQL (usa %s per i parametri)
# - params: tuple di parametri in ordine
# ------------------------------
QUERIES = [
    {
        "name": "top_movies_avg_min50",
        "sql": """
            SELECT m.movieId, m.title, ROUND(AVG(r.rating),2) AS avg_rating, COUNT(*) AS num_votes
            FROM MOVIE m
            JOIN RATINGS r ON m.movieId = r.movieId
            GROUP BY m.movieId, m.title
            HAVING COUNT(*) >= 50
            ORDER BY avg_rating DESC, num_votes DESC
        """,
        "params": (),
    },
    {
        "name": "recs_by_similar_users_uid42_mincommon10",
        "sql": """
            WITH my_movies AS (
            SELECT DISTINCT movieId
            FROM RATINGS
            WHERE userId = %s
            ),
            similar_users AS (
                SELECT r2.userId,
                    COUNT(DISTINCT r2.movieId) AS common
                FROM my_movies m
                JOIN RATINGS r2 ON r2.movieId = m.movieId
                WHERE r2.userId <> %s
                GROUP BY r2.userId
                HAVING COUNT(DISTINCT r2.movieId) >= %s
            ),
            candidate AS (
                SELECT  r.movieId,
                        ROUND(AVG(r.rating), 2) AS avg_sim_rating,
                        COUNT(*)                AS votes
                FROM RATINGS r
                JOIN similar_users s ON s.userId = r.userId
                LEFT JOIN my_movies m ON m.movieId = r.movieId
                WHERE m.movieId IS NULL
                AND r.rating >= 4
                GROUP BY r.movieId
            )
            SELECT   c.movieId,
                    mo.title,
                    c.avg_sim_rating,
                    c.votes
            FROM     candidate c
            JOIN     MOVIE mo ON mo.movieId = c.movieId
            ORDER BY c.avg_sim_rating DESC, c.votes DESC;
        """,
        "params": (42, 42, 10),
    },
    {
        "name": "fof_recs_uid42_depth3_scifi",
        "sql": """
            WITH
            /* Utenti raggiungibili con un cammino di 4 archi
            User(id) -> Movie -> User -> Movie -> User(l4)
            Evitiamo i backtrack immediati (stesso movie / stesso user) e il ritorno al seed.
            */
            l4 AS (
            SELECT DISTINCT r4.userId AS l4UserId
            FROM RATINGS r1                      -- hop 1: seed -> movie1
            JOIN RATINGS r2                      -- hop 2: movie1 -> user2
                ON r2.movieId = r1.movieId
            AND r2.userId <> %s
            JOIN RATINGS r3                      -- hop 3: user2 -> movie3 (diverso da movie1)
                ON r3.userId  = r2.userId
            AND r3.movieId <> r1.movieId
            JOIN RATINGS r4                      -- hop 4: movie3 -> user4 (diverso da user2)
                ON r4.movieId = r3.movieId
            AND r4.userId  <> r2.userId
            WHERE r1.userId = %s
                AND r4.userId <> %s
            ),

            -- Film già visti dal seed (da escludere)
            seen_by_seed AS (
            SELECT movieId FROM RATINGS WHERE userId = %s
            ),

            -- Candidati: film Sci-Fi valutati >=4 dagli utenti l4, non visti dal seed
            candidates AS (
            SELECT r.movieId
            FROM l4
            JOIN RATINGS r   ON r.userId  = l4.l4UserId AND r.rating >= 4.0
            JOIN HAS h       ON h.movieId = r.movieId
            JOIN GENRE g     ON g.name    = h.name AND g.name = 'Sci-Fi'
            LEFT JOIN seen_by_seed sb ON sb.movieId = r.movieId
            WHERE sb.movieId IS NULL
            )

            SELECT c.movieId, m.title, COUNT(*) AS freq
            FROM candidates c
            JOIN MOVIE m ON m.movieId = c.movieId
            GROUP BY c.movieId, m.title
            ORDER BY freq DESC, m.title
            LIMIT 50;

        """,
        "params": (42, 42, 42, 42),
    },
    {
        "name": "count_how_many_users_vote_>=4_a_couple_of_film",
        "sql": """
            WITH params AS (
            SELECT 828124615 AS sinceSec, 1537799250 AS untilSec
            ),
            filtered AS (
            SELECT userId, movieId
            FROM RATINGS r JOIN params p
            WHERE r.rating >= 4.0
                AND r.timestamp BETWEEN p.sinceSec AND p.untilSec
            )
            SELECT
            LEAST(f1.movieId, f2.movieId) AS m1,
            GREATEST(f1.movieId, f2.movieId) AS m2,
            COUNT(DISTINCT f1.userId) AS common_users
            FROM filtered f1
            JOIN filtered f2
            ON f1.userId = f2.userId
            AND f1.movieId < f2.movieId
            GROUP BY m1, m2
            HAVING COUNT(DISTINCT f1.userId) >= 50
            ORDER BY common_users DESC
        """,
        "params": (),
    },
    {
		"name": "movie_pairs_common_raters",
		"sql": """
			SELECT
				LEAST(r1.movieId, r2.movieId)    AS m1,
				GREATEST(r1.movieId, r2.movieId) AS m2,
				COUNT(*)                         AS co_raters
			FROM RATINGS r1
			JOIN RATINGS r2
			  ON r1.userId  = r2.userId
			 AND r1.movieId < r2.movieId
			GROUP BY m1, m2
			HAVING COUNT(*) >= 5
			ORDER BY co_raters DESC;       
		""",
		"params": (),
	}
]

indexes_mysql = {
    "MOVIE": [
        "CREATE INDEX idx_title ON MOVIE(title)"
    ],
    "RATINGS": [
        "CREATE INDEX idx_movieId ON RATINGS(movieId)",    # join su MOVIE
        "CREATE INDEX idx_userId ON RATINGS(userId)",      # join su USER
        "CREATE INDEX idx_rating ON RATINGS(rating)",      # filtri rating >= 4
        "CREATE INDEX idx_timestamp ON RATINGS(timestamp)" # filtri su intervallo temporale
    ],
    "HAS": [
        "CREATE INDEX idx_name ON HAS(name)"
    ]
}

# ------------------------------
# Aux
# ------------------------------
def run_query_times_and_last(cursor, sql, params, repeats, warmups):
    for _ in range(warmups):
        cursor.execute(sql, params)
        cursor.fetchall()

    times_ms, rows_last, header = [], [], []
    for _ in range(repeats):
        t0 = time.perf_counter()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        t1 = time.perf_counter()
        times_ms.append((t1 - t0) * 1000.0)
        rows_last = rows  # keep only the last run
        header = [col[0] for col in cursor.description] if cursor.description else []
    return times_ms, rows_last, header


def save_runs_csv(filename, times_ms):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["run", "time_ms"])
        for i, t in enumerate(times_ms, 1):
            w.writerow([i, t])

def save_last_result_csv(path: Path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if header:
            w.writerow(header)
        for r in rows:
            w.writerow(r)


def append_summary_row(filename, row):
    header = ["timestamp", "query_name", "runs", "avg_ms", "stdev_ms", "min_ms", "max_ms", "rows_last"]
    write_header = False
    try:
        with open(filename, "r", encoding="utf-8"):
            pass
    except FileNotFoundError:
        write_header = True

    with open(filename, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(header)
        w.writerow(row)

# ------------------------------
# Main benchmark
# ------------------------------
def mainMySql():
    try:
        conn = mysql.connector.connect(**CONFIG)
        # buffered evita problemi se in futuro iteri sui risultati
        cursor = conn.cursor(buffered=False)

        summary_file = f"results_with_indexes/MySql/{OUTPUT_PREFIX}_summary.csv"
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for q in QUERIES:
            name = q["name"]
            sql = q["sql"]
            params = q.get("params", ())

            print(f"\n=== {name} ===")
            times_ms, rows_last, header = run_query_times_and_last(cursor, sql, params, REPEATS, WARMUP_RUNS)

            avg = statistics.mean(times_ms)
            stdev = statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0
            print("Execution times (ms):", [round(t, 2) for t in times_ms])
            #print(f"Rows (last run): {rows_last}") # togliere commento per stampare l'ultima riga
            print(f"Average: {avg:.2f} ms | StdDev: {stdev:.2f} ms | Min: {min(times_ms):.2f} ms | Max: {max(times_ms):.2f} ms")

            # CSV per-run
            runs_file = f"results_with_indexes/MySql/{OUTPUT_PREFIX}_{name}.csv"
            save_runs_csv(runs_file, times_ms)
            save_last_result_csv(RESULTS_DIR / f"{name}.csv", header, rows_last)

            # CSV summary cumulativo
            append_summary_row(
                summary_file,
                [ts, name, len(times_ms), f"{avg:.4f}", f"{stdev:.4f}", f"{min(times_ms):.4f}", f"{max(times_ms):.4f}", len(rows_last)],
            )
            if rows_last:
                print("Sample rows (up to 5):")
                for r in rows_last[:5]:
                    print(r)


        cursor.close()
        conn.close()
        print("\n✅ Benchmark completato. CSV generati nella cartella corrente.")
    except Exception as e:
        print("Errore:", e)
        traceback.print_exc()

if __name__ == "__main__":   
    mainMySql()
