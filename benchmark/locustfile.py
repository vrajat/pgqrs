from locust import User, task, between, events
import os, time, json, psycopg2
try:
    from psycogreen.gevent import patch_psycopg
    patch_psycopg()
except Exception:
    # psycogreen not installed or patch failed; continue without gevent-cooperative psycopg2
    pass

DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = int(os.getenv("PG_PORT", 5432))
DB_USER = os.getenv("PG_USER", "pgbench")
DB_PASS = os.getenv("PG_PASS", "pgbench")
DB_NAME = os.getenv("PG_DB", "pgbench")
SCENARIO = os.getenv("SCENARIO", "queue_logged_skiplocked")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
PRODUCER_PAYLOAD_SIZE = int(os.getenv("PRODUCER_PAYLOAD_SIZE", 200))
VISIBILITY_SECONDS = int(os.getenv("VISIBILITY_SECONDS", 30))
POLL_SLEEP = float(os.getenv("POLL_SLEEP", 0.01))

def connect():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME)

import time
from locust import events

def consume_skiplocked(conn, batch_size):
    start = time.time()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""WITH cte AS (
                SELECT msg_id
                FROM {SCENARIO}
                WHERE vt <= clock_timestamp()
                ORDER BY msg_id ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM {SCENARIO} t
            USING cte
            WHERE t.msg_id = cte.msg_id
            RETURNING t.msg_id;
            """, (batch_size,))
            rows = cur.fetchall()
            conn.commit()
            elapsed_ms = int((time.time() - start) * 1000)
            events.request.fire(
                request_type="db",
                name="consume_skiplocked",
                response_time=elapsed_ms,
                response_length=len(rows),
                response=None,
                context={},
                exception=None,
                start_time=None
            )
            return len(rows)
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        elapsed_ms = int((time.time() - start) * 1000)
        events.request.fire(
            request_type="db",
            name="consume_skiplocked",
            response_time=elapsed_ms,
            response_length=0,
            response=None,
            context={},
            exception=e,
            start_time=None
        )
        return 0

def consume_naive(conn, batch_size):
    start = time.time()
    claimed = 0
    try:
        with conn.cursor() as cur:
            cur.execute(f"""SELECT msg_id
                FROM {SCENARIO}
                WHERE vt <= clock_timestamp()
                ORDER BY msg_id ASC
                LIMIT %s
            """, (batch_size,))
            rows = cur.fetchall()
            for (msg_id,) in rows:
                try:
                    cur.execute(f"""UPDATE {SCENARIO}
                        SET vt = clock_timestamp() + interval '%s seconds',
                            read_ct = read_ct + 1
                        WHERE msg_id = %s AND vt <= clock_timestamp()
                        RETURNING msg_id
                    """, (VISIBILITY_SECONDS, msg_id))
                    if cur.rowcount:
                        claimed += 1
                except Exception as inner_e:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    elapsed_inner_ms = int((time.time() - start) * 1000)
                    events.request.fire(
                        request_type="db",
                        name="consume_naive_per_row_error",
                        response_time=elapsed_inner_ms,
                        response_length=claimed,
                        response=None,
                        context={},
                        exception=inner_e,
                        start_time=None
                    )
                    # Return early; caller will continue loop later
                    return claimed
            conn.commit()
            elapsed_ms = int((time.time() - start) * 1000)
            events.request.fire(
                request_type="db",
                name="consume_naive",
                response_time=elapsed_ms,
                response_length=claimed,
                response=None,
                context={},
                exception=None,
                start_time=None
            )
            return claimed
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        elapsed_ms = int((time.time() - start) * 1000)
        events.request.fire(
            request_type="db",
            name="consume_naive",
            response_time=elapsed_ms,
            response_length=claimed,
            response=None,
            context={},
            exception=e,
            start_time=None
        )
        return claimed

class Producer(User):
    wait_time = between(0.001, 0.01)
    def on_start(self):
        self.conn = connect()
        self.cur = self.conn.cursor()
    @task
    def produce(self):
        payload = json.dumps({"data":"x" * max(1, PRODUCER_PAYLOAD_SIZE - 20)})
        for _ in range(5):
            self.cur.execute(f"INSERT INTO {SCENARIO} (payload, vt) VALUES (%s, clock_timestamp())", (payload,))
        self.conn.commit()


    def on_stop(self):
        try:
            self.cur.close()
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass

class Consumer(User):
    wait_time = between(0, POLL_SLEEP)
    def on_start(self):
        self.conn = connect()
    @task
    def consume(self):
        if "naive" in SCENARIO:
            claimed = consume_naive(self.conn, BATCH_SIZE)
        else:
            claimed = consume_skiplocked(self.conn, BATCH_SIZE)
        if claimed == 0:
            time.sleep(POLL_SLEEP)

if __name__ == "__main__":
    conn = connect()
    cur = conn.cursor()
    for i in range(10000):
        cur.execute(f"INSERT INTO {SCENARIO} (payload, vt) VALUES (%s, clock_timestamp())", (json.dumps({"i": i}),))
    conn.commit()
    print("seeded")


    def on_stop(self):
        try:
            self.conn.close()
        except Exception:
            pass
