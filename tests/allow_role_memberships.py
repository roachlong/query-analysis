#!/usr/bin/env python3
"""
Verifies behavior of allow_role_memberships_to_change_during_transaction.

Key finding: the setting must be on READER/APP sessions (not just the
PAM/grantor) for GRANTs to bypass the schema lease wait.

Three things matter for this test to measure anything real — see the comments
at each site, all verified against CockroachDB v26.2.1:

  1. The reader must connect AS the app user.  A per-user setting applied with
     ALTER USER only takes effect on sessions that user actually opens, so a
     reader connected as root measures nothing.  (As root the block does not
     reproduce at all.)

  2. The reader must resolve role memberships IMPLICITLY, via an ordinary
     privilege-checked query.  Reading system.role_members or
     pg_catalog.pg_auth_members instead takes a catalog descriptor lease, which
     this setting does not govern -- those readers block whether the setting is
     on or off, which would "prove" the opposite conclusion.

  3. At least two membership changes must run while the reader txn is held.
     The first always succeeds (a lease may trail by one version); the second
     is the one that has to drain the reader's lease and blocks.

This script does NOT change any cluster-wide or pre-existing configuration.  It
only ever touches the objects it creates itself (see SAFETY below), so it is
safe to point at a shared dev cluster.

Usage:
  pip install psycopg2-binary

  # insecure local cluster (the default)
  python3 allow_role_memberships.py [--host localhost] [--port 26257]

  # secure cluster, admin authenticating by client certificate
  python3 allow_role_memberships.py --host my-host --certs-dir ~/certs

  # secure cluster, admin authenticating by password
  python3 allow_role_memberships.py --host my-host --password "$PGPASSWORD" \
      --sslmode verify-full --sslrootcert ~/certs/ca.crt

On a secure cluster the two test users are created with a freshly generated
random password, held only in memory for the life of the run, because a
passwordless user cannot authenticate when the cluster is not in insecure mode.

SAFETY: creates and drops demo_app_role, demo_pam_user, demo_app_user and table
demo_app_data -- anything pre-existing under those names WILL be dropped.
demo_pam_user is temporarily granted admin.  Nothing else is modified: in
particular this script never issues ALTER ROLE ALL, so a cluster-wide
`ALTER ROLE ALL SET allow_role_memberships_to_change_during_transaction = true`
(the remediation this test recommends) is left untouched.
"""

import psycopg2
import threading
import time
import argparse
import os
import secrets
import sys

# ── Config ────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--host",          default="localhost")
parser.add_argument("--port",          type=int, default=26257)
parser.add_argument("--user",          default="root",
                    help="admin user used for setup/teardown (default root)")
parser.add_argument("--database",      default="defaultdb")
parser.add_argument("--sslmode",       default=None,
                    help="libpq sslmode; defaults to verify-full when "
                         "--certs-dir is given, otherwise disable")
parser.add_argument("--certs-dir",     default=None,
                    help="CockroachDB certs dir; uses ca.crt plus "
                         "client.<user>.{crt,key} for admin cert auth")
parser.add_argument("--sslrootcert",   default=None,
                    help="CA certificate (alternative to --certs-dir)")
parser.add_argument("--password",      default=None,
                    help="password for --user (alternative to cert auth)")
parser.add_argument("--grant-timeout", type=int, default=8,
                    help="seconds to wait before calling a GRANT blocked "
                         "(default 8)")
args = parser.parse_args()

SETTING = "allow_role_memberships_to_change_during_transaction"

# sslmode drives everything else: anything but "disable" means the cluster is
# running securely, which in turn means the test users need real passwords.
SSLMODE = args.sslmode or ("verify-full" if args.certs_dir else "disable")
SECURE  = SSLMODE != "disable"

# Password for the two test users. Generated per run and never persisted; the
# alphabet is urlsafe-base64, so it needs no SQL quoting beyond the quotes.
TEST_PW = secrets.token_urlsafe(18) if SECURE else None

def _ssl_params() -> dict:
    p = {"sslmode": SSLMODE}
    root = args.sslrootcert
    if not root and args.certs_dir:
        candidate = os.path.join(args.certs_dir, "ca.crt")
        if os.path.exists(candidate):
            root = candidate
    if root:
        p["sslrootcert"] = root
    return p

BASE = dict(host=args.host, port=args.port, database=args.database)

# ── Helpers ───────────────────────────────────────────────────────────────────
def connect_admin():
    """Connect as the admin user, by client certificate or password."""
    p = dict(BASE, user=args.user, **_ssl_params())
    if args.certs_dir:
        crt = os.path.join(args.certs_dir, f"client.{args.user}.crt")
        key = os.path.join(args.certs_dir, f"client.{args.user}.key")
        if os.path.exists(crt) and os.path.exists(key):
            p["sslcert"], p["sslkey"] = crt, key
    if args.password:
        p["password"] = args.password
    return psycopg2.connect(**p)

def connect_as(user: str):
    """
    Connect as one of the test users.  These authenticate by password, not by
    certificate: issuing client certs for them would mean running the cockroach
    CLI mid-test, and the whole point is that these are ordinary app logins.
    """
    p = dict(BASE, user=user, **_ssl_params())
    if TEST_PW:
        p["password"] = TEST_PW
    return psycopg2.connect(**p)

def connect(user: str = None):
    return connect_as(user) if user else connect_admin()

def exec_sql(sql, user: str = None):
    conn = connect(user)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
                cur.execute(stmt)
    finally:
        conn.close()

def show_setting(cur) -> str:
    """Report the session's effective value, so results are verifiable."""
    cur.execute(f"SHOW {SETTING}")
    return cur.fetchone()[0]

def run_timed(fn, limit):
    """Run fn in a thread, give it `limit` seconds. Returns (res, thread)."""
    res = {}
    def go():
        start = time.perf_counter()
        try:
            fn()
            res["ok"] = True
        except Exception as exc:
            res["error"] = str(exc).splitlines()[0]
        res["elapsed"] = time.perf_counter() - start
        res["done"] = True
    th = threading.Thread(target=go, daemon=True)
    th.start()
    th.join(timeout=limit)
    return res, th

# ── Setup / Teardown ──────────────────────────────────────────────────────────
def setup():
    print("\n📋  Setup: creating test users, role and table ...")
    # WITH PASSWORD only in secure mode: insecure clusters reject it outright
    # ("setting or updating a password is not supported in insecure mode"),
    # and there they do not need one to authenticate.
    pw = f" WITH PASSWORD '{TEST_PW}'" if SECURE else ""
    # demo_app_data is granted to BOTH the role and the app user directly, so
    # the reader can always read it -- the point is that reading it forces a
    # privilege check, which resolves role memberships.
    exec_sql(f"""
        DROP TABLE IF EXISTS demo_app_data;
        DROP ROLE IF EXISTS demo_app_role;
        DROP USER IF EXISTS demo_pam_user;
        DROP USER IF EXISTS demo_app_user;
        CREATE ROLE  demo_app_role;
        CREATE USER  demo_pam_user{pw};
        CREATE USER  demo_app_user{pw};
        GRANT admin TO demo_pam_user WITH ADMIN OPTION;
        CREATE TABLE demo_app_data (id INT PRIMARY KEY);
        INSERT INTO demo_app_data VALUES (1), (2);
        GRANT SELECT ON TABLE demo_app_data TO demo_app_role;
        GRANT SELECT ON TABLE demo_app_data TO demo_app_user
    """)

def teardown():
    # Dropping the users discards their per-user settings with them, so there
    # is nothing cluster-wide to reset -- and deliberately so: an
    # ALTER ROLE ALL RESET here would silently wipe a cluster-wide
    # `ALTER ROLE ALL SET <setting> = true`, i.e. the very remediation this
    # test recommends.
    print("\n🧹  Cleanup ...")
    exec_sql("""
        DROP TABLE IF EXISTS demo_app_data;
        DROP USER IF EXISTS demo_pam_user;
        DROP USER IF EXISTS demo_app_user;
        DROP ROLE IF EXISTS demo_app_role
    """)

# ── Test Mechanics ────────────────────────────────────────────────────────────
def hold_open_txn(ready: threading.Event, stop: threading.Event, state: dict):
    """
    Simulate a production app session: open a transaction that resolves role
    memberships and hold it open until told to stop.
    Sessions without the setting will block any concurrent GRANT.

    Connects as demo_app_user, not root (point 1 in the module docstring), and
    reads an ordinary table rather than a catalog (point 2).
    """
    conn = None
    try:
        conn = connect(user="demo_app_user")
        conn.autocommit = True
        with conn.cursor() as cur:
            state["setting"] = show_setting(cur)
            conn.autocommit = False          # open the txn
            cur.execute("SELECT count(*) FROM demo_app_data")
            cur.fetchall()
            cur.execute("SHOW transaction_status")
            state["txn_status"] = cur.fetchone()[0]
        ready.set()
        stop.wait()      # hold the txn open
    except Exception as exc:
        state["error"] = str(exc).splitlines()[0]
    finally:
        # ready MUST be set even on failure, or the main thread waits forever
        ready.set()
        if conn is not None:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass

def measure_grants(stop: threading.Event):
    """
    Run successive membership changes as demo_pam_user while the reader txn is
    held open.  Returns (timings, blocked_at, effective_setting).

    No timeout is set on this session deliberately: a blocked GRANT here is
    waiting on a schema-lease/descriptor-version drain, not a lock, and that
    wait is interruptible by neither lock_timeout nor statement_timeout
    (verified: a GRANT with statement_timeout=8s, confirmed applied as 8000ms,
    stayed ACTIVE for over 5 minutes).  So we race it against a wall clock.
    """
    conn = connect(user="demo_pam_user")
    conn.autocommit = True
    timings, blocked_at, setting = [], None, "?"
    try:
        with conn.cursor() as cur:
            setting = show_setting(cur)

        for stmt in ("GRANT demo_app_role TO demo_app_user",
                     "REVOKE demo_app_role FROM demo_app_user",
                     "GRANT demo_app_role TO demo_app_user"):
            def issue(stmt=stmt):
                with conn.cursor() as cur:
                    cur.execute(stmt)
            res, th = run_timed(issue, args.grant_timeout)
            if res.get("done"):
                timings.append(f"{res['elapsed']:.2f}s")
            else:
                # Releasing the reader is what lets it through -- which is
                # itself the proof that the reader txn was the blocker.
                blocked_at = stmt
                stop.set()
                th.join(timeout=60)
                timings.append(f"BLOCKED>{args.grant_timeout}s")
                break
    finally:
        conn.close()
    return timings, blocked_at, setting

def run_scenario(label: str, pam_setting: bool, reader_setting: bool):
    # Set both users EXPLICITLY every scenario, rather than RESETting the ones
    # that should be off.  Two reasons:
    #   - RESET would fall back to the all-roles default, so on a cluster with
    #     `ALTER ROLE ALL SET <setting> = true` the "off" scenarios would
    #     silently run as on, not block, and the test would report a false pass.
    #   - An explicit per-user value overrides that default in both directions
    #     (verified), so the test is immune to whatever the cluster default is
    #     and never has to modify it.
    exec_sql(f"ALTER USER demo_pam_user SET {SETTING} = {str(pam_setting).lower()}")
    exec_sql(f"ALTER USER demo_app_user SET {SETTING} = {str(reader_setting).lower()}")

    # Start from a known state, before any reader txn exists: a REVOKE is the
    # same flavour of schema change as the GRANT and would block identically.
    try:
        exec_sql("REVOKE demo_app_role FROM demo_app_user", user="demo_pam_user")
    except Exception:
        pass

    # Open a background production transaction
    ready, stop = threading.Event(), threading.Event()
    state = {}
    reader = threading.Thread(target=hold_open_txn,
                              args=(ready, stop, state), daemon=True)
    reader.start()
    ready.wait()
    time.sleep(0.2)   # let it settle

    if "error" in state:
        stop.set(); reader.join(timeout=5)
        print(f"  ⚠️   {label:<42}  reader failed: {state['error']}")
        return

    timings, blocked_at, pam_eff = measure_grants(stop)

    stop.set()
    reader.join(timeout=5)

    mark   = "❌" if blocked_at else "✅"
    result = "  ".join(timings)
    # Echo the effective per-session values: proof the settings really applied.
    print(f"  {mark}  {label:<42}  {result:<26}"
          f"[reader={state.get('setting','?')}/{state.get('txn_status','?')} "
          f"grantor={pam_eff}]")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 78)
    print(f"   {SETTING}")
    print("   Grant timing verification — with production txn held open")
    print("=" * 78)
    if args.certs_dir:
        auth = f"client cert ({args.certs_dir})"
    elif args.password:
        auth = "password"
    else:
        auth = "none (insecure)"
    print(f"   Host: {args.host}:{args.port}   sslmode: {SSLMODE}   admin auth: {auth}")
    print(f"   Test users: {'generated password' if SECURE else 'passwordless'}"
          f"   block threshold: {args.grant_timeout}s")

    # Fail fast with an actionable message rather than a raw traceback from
    # inside a worker thread.
    try:
        exec_sql("SELECT 1")
    except Exception as exc:
        print(f"\n❌  Cannot connect as admin user {args.user!r}: "
              f"{str(exc).strip().splitlines()[0]}")
        if not SECURE:
            print("    If the cluster is secure, pass --certs-dir or "
                  "--password (and --sslmode).")
        sys.exit(2)

    setup()

    print("\n⏱   Running 4 scenarios (production reader txn held open throughout).")
    print("    Each shows timings for 3 successive membership changes:\n")

    run_scenario("1. Neither has the setting      (baseline)",
                 pam_setting=False, reader_setting=False)
    run_scenario("2. Only PAM/grantor has setting (original advice)",
                 pam_setting=True,  reader_setting=False)
    run_scenario("3. Only reader/app has setting",
                 pam_setting=False, reader_setting=True)
    run_scenario("4. Both have the setting",
                 pam_setting=True,  reader_setting=True)

    print("""
📌  Interpretation:
    ❌ Scenarios 1 & 2 block  →  setting on the GRANTOR alone does NOT help.
    ✅ Scenarios 3 & 4 are fast  →  setting on READER sessions is what matters.

    Note the shape of the blocking: the FIRST change always succeeds, the
    SECOND is the one that stalls. A lease may trail by one version, so it is
    the second change that must drain the reader's lease. A test that issues a
    single GRANT will look fine and prove nothing.

    The setting tells a session: "I accept that role memberships may change
    during my transaction." Sessions without it hold a blocker on any GRANT
    until they commit or rollback — regardless of what the grantor has set.

    ➡  Correct approach for PAM provisioning:
       ALTER ROLE ALL SET allow_role_memberships_to_change_during_transaction = true
       — or at minimum, set it on all application user roles.
       Scoping it to the PAM service account only does NOT bypass the wait.

    ⚠  The wait is NOT bounded by lock_timeout or statement_timeout: it is a
       schema-lease drain, not a lock. Do not rely on a timeout to cap it.
""")

    teardown()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
