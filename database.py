import sqlite3
import os
from datetime import datetime

DB_PATH = "/var/data/metrica.db" if os.path.isdir("/var/data") else "metrica.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS keywords_permanentes (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword          TEXT NOT NULL,
            contexto         TEXT NOT NULL DEFAULT '',
            modo             TEXT NOT NULL DEFAULT 'diario',
            frecuencia_horas INTEGER NOT NULL DEFAULT 24,
            hora_envio       INTEGER NOT NULL DEFAULT 12,
            activa           INTEGER NOT NULL DEFAULT 1,
            creada_en        TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS destinatarios (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            email      TEXT NOT NULL UNIQUE,
            creado_en  TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS historial (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword_id       INTEGER,
            keyword          TEXT NOT NULL,
            modo             TEXT NOT NULL DEFAULT 'manual',
            total_resultados INTEGER NOT NULL DEFAULT 0,
            email_enviado    INTEGER NOT NULL DEFAULT 0,
            html_content     TEXT,
            ejecutado_en     TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS urls_enviadas (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword_id  INTEGER NOT NULL,
            url         TEXT NOT NULL,
            enviado_en  TEXT NOT NULL,
            UNIQUE(keyword_id, url)
        );
    """)
    # Migración segura: agrega columna contexto si no existe en BD ya creada.
    # ALTER TABLE IF NOT EXISTS no existe en SQLite — usamos try/except.
    try:
        c.execute("ALTER TABLE keywords_permanentes ADD COLUMN contexto TEXT NOT NULL DEFAULT ''")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE keywords_permanentes ADD COLUMN hora_envio INTEGER NOT NULL DEFAULT 12")
        conn.commit()
        print("[db] Columna 'hora_envio' agregada a keywords_permanentes")
    except sqlite3.OperationalError:
        pass  # Ya existía — normal en deploys posteriores al primero
    conn.close()


def get_keywords_permanentes():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM keywords_permanentes ORDER BY creada_en DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_keyword_permanente(keyword: str, contexto: str, modo: str, frecuencia_horas: int, hora_envio: int = 12):
    conn = get_conn()
    conn.execute(
        """INSERT INTO keywords_permanentes
           (keyword, contexto, modo, frecuencia_horas, hora_envio, activa, creada_en)
           VALUES (?,?,?,?,?,1,?)""",
        (keyword, contexto, modo, frecuencia_horas, hora_envio, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def delete_keyword_permanente(keyword_id):
    conn = get_conn()
    conn.execute("DELETE FROM keywords_permanentes WHERE id=?", (keyword_id,))
    conn.execute("DELETE FROM urls_enviadas WHERE keyword_id=?", (keyword_id,))
    conn.commit()
    conn.close()


def get_destinatarios():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM destinatarios ORDER BY creado_en DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_destinatario(email):
    conn = get_conn()
    try:
        conn.execute("INSERT INTO destinatarios (email, creado_en) VALUES (?,?)",
                     (email, datetime.now().isoformat()))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()


def delete_destinatario(dest_id):
    conn = get_conn()
    conn.execute("DELETE FROM destinatarios WHERE id=?", (dest_id,))
    conn.commit()
    conn.close()


def save_historial(keyword_id, keyword, modo, total, enviado, html_content):
    conn = get_conn()
    conn.execute(
        """INSERT INTO historial
           (keyword_id,keyword,modo,total_resultados,email_enviado,html_content,ejecutado_en)
           VALUES (?,?,?,?,?,?,?)""",
        (keyword_id, keyword, modo, total, int(enviado), html_content, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def get_historial(limit=30):
    conn = get_conn()
    rows = conn.execute(
        """SELECT id,keyword,modo,total_resultados,email_enviado,ejecutado_en
           FROM historial ORDER BY ejecutado_en DESC LIMIT ?""",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_historial_by_id(historial_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM historial WHERE id=?", (historial_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def url_ya_enviada(keyword_id, url):
    conn = get_conn()
    row = conn.execute("SELECT 1 FROM urls_enviadas WHERE keyword_id=? AND url=?",
                       (keyword_id, url)).fetchone()
    conn.close()
    return row is not None


def marcar_url_enviada(keyword_id, url):
    conn = get_conn()
    try:
        conn.execute("INSERT INTO urls_enviadas (keyword_id,url,enviado_en) VALUES (?,?,?)",
                     (keyword_id, url, datetime.now().isoformat()))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()


def limpiar_urls_antiguas(dias=30):
    conn = get_conn()
    conn.execute("DELETE FROM urls_enviadas WHERE enviado_en < datetime('now', ?)",
                 (f'-{dias} days',))
    conn.commit()
    conn.close()
