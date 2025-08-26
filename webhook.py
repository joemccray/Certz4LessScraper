from fastapi import FastAPI, HTTPException, Query
import psycopg2
import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection():
    return psycopg2.connect(DATABASE_URL)


@app.get("/")
def root():
    return {"message": "Certz4Less Webhook API is online 🚀"}


@app.get("/questions")
def list_questions(vendor: Optional[str] = Query(None)):
    try:
        conn = get_connection()
        cur = conn.cursor()

        if vendor:
            cur.execute("SELECT * FROM questions WHERE vendor = %s", (vendor,))
        else:
            cur.execute("SELECT * FROM questions")

        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        results = [dict(zip(colnames, row)) for row in rows]

        cur.close()
        conn.close()
        return {"counts": len(results), "questions": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/questions/{question_id}")
def get_question(question_id: str):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM questions WHERE question_id = %s", (question_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Question not found")

        colnames = [desc[0] for desc in cur.description]
        result = dict(zip(colnames, row))
        cur.close()
        conn.close()
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/answers/{question_id}")
def get_answers(question_id: str):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT * FROM answers WHERE question_id = %s", (question_id,))
        rows = cur.fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="No answers found")

        colnames = [desc[0] for desc in cur.description]
        results = [dict(zip(colnames, row)) for row in rows]

        cur.close()
        conn.close()

        return {"answers": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
