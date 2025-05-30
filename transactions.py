import psycopg
import random
import time

class Transactions:

    def __init__(self, args: dict):
        # args is a dict of string passed with the --args flag
        # user passed a yaml/json, in python that's a dict object
        self.schedule_freq: int = int(args.get("schedule_freq", 10))
        self.status_freq: int = int(args.get("status_freq", 90))
        self.inventory_freq: int = int(args.get("inventory_freq", 75))
        self.price_freq: int = int(args.get("price_freq", 25))
        self.contention_freq: int = int(args.get("contention_freq", 20))
        self.batch_size: int = int(args.get("batch_size", 16))
        self.delay: int = int(args.get("delay", 100))

        # you can arbitrarely add any variables you want
        self.counter: int = 0



    # the setup() function is executed only once
    # when a new executing thread is started.
    # Also, the function is a vector to receive the excuting threads's unique id and the total thread count
    def setup(self, conn: psycopg.Connection, id: int, total_thread_count: int):
        self.id = id
        with conn.cursor() as cur:
            print(
                f"My thread ID is {id}. The total count of threads is {total_thread_count}"
            )
            print(cur.execute(f"select version()").fetchone()[0])



    # the run() function returns a list of functions
    # that dbworkload will execute, sequentially.
    # Once every func has been executed, run() is re-evaluated.
    # This process continues until dbworkload exits.
    def loop(self):   
        return [self.schedule, self.status, self.inventory, self.price, self.contention]



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def flights(self, conn: psycopg.Connection):
        query = f"""
SELECT flight_id
FROM flights
AS OF SYSTEM TIME follower_read_timestamp()
ORDER BY random()
LIMIT {self.batch_size};
"""
        with conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur]




    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def schedule(self, conn: psycopg.Connection):
        if (random.randint(1, 100) <= self.schedule_freq):
            flight_ids = self.flights(conn)
            values = ','.join(f"%s" for i in range(len(flight_ids)))
            query = f"""
UPDATE flights
SET scheduled_departure = scheduled_departure
        + (
            (CASE WHEN random() < 0.5 THEN 1 ELSE -1 END)
            * (floor(random() * 56) + 5)::INT
        ) * INTERVAL '1 minute',
    scheduled_arrival   = scheduled_arrival
        + (
            (CASE WHEN random() < 0.5 THEN 1 ELSE -1 END)
            * (floor(random() * 56) + 5)::INT
        ) * INTERVAL '1 minute',
    updated_at = now()
WHERE flight_id IN ({values});
"""
            with conn.cursor() as cur:
                cur.execute(query, tuple(flight_ids))
            time.sleep(self.delay / 1000)



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def status(self, conn: psycopg.Connection):
        if (random.randint(1, 100) <= self.status_freq):
            flight_ids = self.flights(conn)
            values = ','.join(f"%s" for i in range(len(flight_ids)))
            query = f"""
UPDATE flight_status
SET status = (
        ARRAY['on_time','delayed','cancelled']
    )[1 + floor(random() * 3)::INT],
    updated_at = now()
WHERE flight_id IN ({values});
"""
            with conn.cursor() as cur:
                cur.execute(query, tuple(flight_ids))
            time.sleep(self.delay / 1000)



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def inventory(self, conn: psycopg.Connection):
        if (random.randint(1, 100) <= self.inventory_freq):
            flight_ids = self.flights(conn)
            values = ','.join(f"%s" for i in range(len(flight_ids)))
            query = f"""
UPDATE seat_inventory
SET seats_available = (seats_available::FLOAT * (1 + (random()-0.25)/10))::INT,
    updated_at = now()
WHERE flight_id IN ({values});
"""
            with conn.cursor() as cur:
                cur.execute(query, tuple(flight_ids))
            time.sleep(self.delay / 1000)



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def price(self, conn: psycopg.Connection):
        if (random.randint(1, 100) <= self.price_freq):
            flight_ids = self.flights(conn)
            values = ','.join(f"%s" for i in range(len(flight_ids)))
            query = f"""
UPDATE flight_prices
SET price_usd = price_usd * (1 + (random()-0.5)/10)::DECIMAL,
    updated_at = now()
WHERE flight_id IN ({values});
"""
            with conn.cursor() as cur:
                cur.execute(query, tuple(flight_ids))
            time.sleep(self.delay / 1000)




    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def contention(self, conn: psycopg.Connection):
        if (random.randint(1, 100) <= self.contention_freq):
            original_autocommit = conn.autocommit
            try:
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM airports WHERE city = 'City_1';")
                    time.sleep(random.uniform(0.01, 0.05))
                    cur.execute("UPDATE airports SET country = 'CRDB' WHERE city = 'City_1';")
                conn.commit()
            except Exception as e:
                print(f"Error occurred: {e}")
                conn.rollback()
            finally:
                conn.autocommit = original_autocommit
            time.sleep(self.delay / 1000)
