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

        # 1) Fetch questions
        with conn.cursor() as cur:
            if vendor:
                cur.execute("SELECT * FROM questions WHERE vendor = %s", (vendor,))
            else:
                cur.execute("SELECT * FROM questions")

            q_rows = cur.fetchall()
            q_colnames = [desc[0] for desc in cur.description]

        questions = [dict(zip(q_colnames, row)) for row in q_rows]

        # 2) For each question, fetch answers and attach (dedup + order)
        with conn.cursor() as cur_ans:
            for q in questions:
                qid = q["question_id"]
                cur_ans.execute(
                    """
                    SELECT DISTINCT option, text, is_correct
                    FROM answers
                    WHERE question_id = %s
                    ORDER BY option
                    """,
                    (qid,),
                )
                ans_rows = cur_ans.fetchall()
                q["answers"] = [
                    {"option": opt, "text": txt, "is_correct": is_c}
                    for (opt, txt, is_c) in ans_rows
                ]

        conn.close()
        return {"counts": len(questions), "questions": questions}

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

        cur.execute(
            "SELECT option, text, is_correct FROM answers WHERE question_id = %s ORDER BY option",
            (question_id,)
        )
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
