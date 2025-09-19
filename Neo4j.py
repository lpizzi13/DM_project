from pathlib import Path
from neo4j import GraphDatabase
import time
import statistics
import csv
from datetime import datetime
import os
import sys
# Connection config (adatta user/password/uri e nome database)
neo4j_config = {
    "uri": "bolt://localhost:7687",
    "auth": ("user", "Password1!"),
    "database": "project",  # cambia se usi un database diverso
}

REPEATS = 10
WARMUP_RUNS = 1
OUTPUT_PREFIX = "neo4j"

QUERIES = [
    {
        "name": "top_movies_avg_min50",
        "cypher": """
            MATCH (m:Movie)<-[r:RATED]-(:User)
            WITH m, round(avg(r.rating),2) AS avg_rating, count(r) AS num_votes
            WHERE num_votes >= 50
            RETURN m.movieId AS movieId, m.title AS title, avg_rating, num_votes
            ORDER BY avg_rating DESC, num_votes DESC
        """,
        "params": {},
    },
    {
        "name": "recs_by_similar_users_uid42_mincommon10",
        "cypher": """
            MATCH (u:User {userId:$userId})-[:RATED]->(m:Movie)
            WITH u, COLLECT(m) AS myMovies
            MATCH (u)-[:RATED]->(comm:Movie)<-[:RATED]-(other:User)
            WITH other, COUNT(DISTINCT comm) AS common, myMovies
            WHERE common >= $minCommon
            MATCH (other)-[r:RATED]->(rec:Movie)
            WHERE r.rating >= 4 AND NOT rec IN myMovies
            WITH rec, round(AVG(r.rating),2) AS avg_sim_rating, COUNT(r) AS votes
            RETURN rec.movieId AS movieId,
                   rec.title   AS title,
                   avg_sim_rating,
                   votes
            ORDER BY avg_sim_rating DESC, votes DESC
        """,
        "params": {"userId": 42, "minCommon": 10},
    },
    {
        "name": "fof_recs_uid42_depth3_scifi",
        "cypher": """
            MATCH (u:User {userId:$uid})
            MATCH p = (u)-[:RATED*4]-(l4:User)            
            WITH DISTINCT l4, u
            MATCH (l4)-[r:RATED]->(m:Movie)-[:HAS_GENRE]->(:Genre {name:'Sci-Fi'})
            WHERE r.rating >= 4 AND NOT (u)-[:RATED]->(m)
            RETURN m.movieId AS movieId, m.title AS title, count(*) AS freq
            ORDER BY freq DESC, title
            LIMIT 50;
        """,
        "params": {"uid": 42},
    },
    {
        "name": "count_how_many_users_vote_greather_than_4_a_couple_of_film",
        "cypher": """
            WITH 828124615 AS sinceSec, 1537799250 AS untilSec
            MATCH (m1:Movie)<-[r1:RATED]-(u:User)-[r2:RATED]->(m2:Movie)
            WHERE r1.rating >= 4.0 AND r2.rating >= 4.0
            AND r1.timestamp >= sinceSec AND r1.timestamp <= untilSec
            AND r2.timestamp >= sinceSec AND r2.timestamp <= untilSec
            WITH toInteger(m1.movieId) AS id1, toInteger(m2.movieId) AS id2, u
            // normalizza l'ordine come in MySQL: (min, max)
            WITH (CASE WHEN id1 < id2 THEN id1 ELSE id2 END) AS m1,
                (CASE WHEN id1 < id2 THEN id2 ELSE id1 END) AS m2,
                u
            WITH m1, m2, count(DISTINCT u) AS common_users
            WHERE common_users >= 50
            RETURN m1, m2, common_users
            ORDER BY common_users DESC
                    """,
        "params": {},
    },
   {
		"name": "movie_pairs_common_raters",
		"cypher": """
			MATCH (u:User)-[:RATED]->(m1:Movie),
			      (u)-[:RATED]->(m2:Movie)
			WHERE m1.movieId < m2.movieId
			WITH m1, m2, count(*) AS co_raters
			WHERE co_raters >= 5   // facoltativo, come sopra
			RETURN m1.movieId AS m1, m2.movieId AS m2, co_raters
			ORDER BY co_raters DESC
		""",
		"params": (),
	}
]

indexes_neo4j = {
    "Movie": [
        "CREATE INDEX movie_id_index FOR (m:Movie) ON (m.movieId)",
        "CREATE INDEX movie_title_index FOR (m:Movie) ON (m.title)"
    ],
    "User": [
        "CREATE INDEX user_id_index FOR (u:User) ON (u.userId)"
    ],
    "Genre": [
        "CREATE INDEX genre_name_index FOR (g:Genre) ON (g.name)"
    ],
    "RATED": [
        "CREATE INDEX rated_rating_index FOR ()-[r:RATED]-() ON (r.rating)",
        "CREATE INDEX rated_timestamp_index FOR ()-[r:RATED]-() ON (r.timestamp)"
    ]
}

def apply_neo4j_indexes(session,use_indexes):
    for _, stmts in indexes_neo4j.items():
        for stmt in stmts:
            if use_indexes:
                # Eseguo CREATE INDEX
                session.run(stmt)
            else:
                # Estraggo il nome dell’indice per generare DROP
                parts = stmt.split()
                # Sintassi tipica: CREATE INDEX index_name ...
                if len(parts) >= 3 and parts[0].upper() == "CREATE" and parts[1].upper() == "INDEX":
                    index_name = parts[2]
                    drop_stmt = f"DROP INDEX {index_name} IF EXISTS"
                    session.run(drop_stmt)

def run_query_times_and_last(session, cypher, params, repeats, warmups):
    for _ in range(warmups):
        session.run(cypher, params).consume()

    times_ms, rows_last, header = [], [], []
    for _ in range(repeats):
        t0 = time.perf_counter()
        res = session.run(cypher, params)
        data = list(res)
        t1 = time.perf_counter()
        times_ms.append((t1 - t0) * 1000.0)
        rows_last = [tuple(r.values()) for r in data]
        header = list(data[0].keys()) if data else []
    return times_ms, rows_last, header


def save_runs_csv(filename, times_ms):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["run", "time_ms"])
        for i, t in enumerate(times_ms, 1):
            w.writerow([i, t])

def save_last_result_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if header:
            w.writerow(header)
        for r in rows:
            w.writerow(r)

def append_summary_row(filename, row):
    header = ["timestamp", "query_name", "runs", "avg_ms", "stdev_ms", "min_ms", "max_ms", "rows_last"]
    write_header = not os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(header)
        w.writerow(row)

def mainNeo4j(RESULTS_ROOT,use_indexes):
    try:
        driver = GraphDatabase.driver(neo4j_config["uri"], auth=neo4j_config["auth"])
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        RESULTS_DIR = RESULTS_ROOT / "neo4j"
        summary_file = RESULTS_DIR/ f"{OUTPUT_PREFIX}_summary.csv"

        with driver.session(database=neo4j_config["database"]) as session:
            apply_neo4j_indexes(session,use_indexes)
            for q in QUERIES:
                name = q["name"]
                cypher = q["cypher"]
                params = q.get("params", {})

                times_ms, rows_last, header = run_query_times_and_last(session, cypher, params, REPEATS, WARMUP_RUNS)
                avg = statistics.mean(times_ms)
                stdev = statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0

                print(f"\n=== {name} ===")
                print("Times (ms):", [round(t, 2) for t in times_ms])
                #print(f"Rows (last run): {rows_last}")
                print(f"Average: {avg:.2f} ms | StdDev: {stdev:.2f} ms | Min: {min(times_ms):.2f} ms | Max: {max(times_ms):.2f} ms")

                save_runs_csv(RESULTS_ROOT / "neo4j" / f"{OUTPUT_PREFIX}_{name}.csv", times_ms)
                save_last_result_csv(RESULTS_DIR / f"{name}.csv", header, rows_last)
                append_summary_row(
                    summary_file,
                    [ts, name, len(times_ms), f"{avg:.4f}", f"{stdev:.4f}", f"{min(times_ms):.4f}", f"{max(times_ms):.4f}", len(rows_last)],
                )
                if rows_last:
                    print("Sample rows (up to 5):")
                    for r in rows_last[:5]:
                        print(r)

        driver.close()
        print("\nBenchmark completed. CSV files written to the current directory.")
    except Exception as e:
        print("Neo4j error:", e)
        sys.exit(1)

if __name__ == "__main__":
    mainNeo4j()
