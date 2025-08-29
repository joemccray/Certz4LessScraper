from fastapi import FastAPI, HTTPException, Query
import psycopg2
import os
import math
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

@app.get("/health")
def health_check():
    """Quick health check with basic stats."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Get basic stats
        cur.execute("SELECT COUNT(*) FROM questions")
        question_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM answers")
        answer_count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "stats": {
                "questions": question_count,
                "answers": answer_count
            },
            "pagination": "enabled"
        }
        
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

@app.get("/vendors")
def list_vendors():
    """Get all available vendors with question counts."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT vendor, COUNT(*) as question_count 
            FROM questions 
            GROUP BY vendor 
            ORDER BY vendor
        """)
        
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        vendors = [dict(zip(colnames, row)) for row in rows]
        
        cur.close()
        conn.close()
        
        return {
            "success": True,
            "total_vendors": len(vendors),
            "vendors": vendors
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/questions")
def list_questions(
    vendor: Optional[str] = Query(None, description="Filter by vendor"),
    page: int = Query(1, ge=1, description="Page number (starting from 1)"),
    page_size: int = Query(50, ge=1, le=500, description="Number of records per page (max 500)")
):
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Calculate offset for pagination
        offset = (page - 1) * page_size

        # First, get the total count
        if vendor:
            count_query = "SELECT COUNT(*) FROM questions WHERE vendor = %s"
            cur.execute(count_query, (vendor,))
        else:
            count_query = "SELECT COUNT(*) FROM questions"
            cur.execute(count_query)
        
        total_count = cur.fetchone()[0]
        total_pages = math.ceil(total_count / page_size)

        # Then get the paginated questions
        if vendor:
            data_query = "SELECT * FROM questions WHERE vendor = %s ORDER BY question_id LIMIT %s OFFSET %s"
            cur.execute(data_query, (vendor, page_size, offset))
        else:
            data_query = "SELECT * FROM questions ORDER BY question_id LIMIT %s OFFSET %s"
            cur.execute(data_query, (page_size, offset))

        q_rows = cur.fetchall()
        q_colnames = [desc[0] for desc in cur.description]
        questions = [dict(zip(q_colnames, row)) for row in q_rows]

        # Get answers for the current page questions only
        if questions:
            question_ids = [q["question_id"] for q in questions]
            placeholders = ','.join(['%s'] * len(question_ids))
            
            cur.execute(f"""
                SELECT DISTINCT question_id, option, text, is_correct
                FROM answers
                WHERE question_id IN ({placeholders})
                ORDER BY question_id, option
            """, question_ids)
            
            ans_rows = cur.fetchall()
            
            # Group answers by question_id
            answers_dict = {}
            for qid, opt, txt, is_c in ans_rows:
                if qid not in answers_dict:
                    answers_dict[qid] = []
                answers_dict[qid].append({
                    "option": opt,
                    "text": txt,
                    "is_correct": is_c
                })
            
            # Attach answers to questions
            for q in questions:
                qid = q["question_id"]
                q["answers"] = answers_dict.get(qid, [])

        # Build pagination metadata
        pagination_info = {
            "current_page": page,
            "page_size": page_size,
            "total_records": total_count,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_previous": page > 1,
            "next_page": page + 1 if page < total_pages else None,
            "previous_page": page - 1 if page > 1 else None
        }

        cur.close()
        conn.close()
        
        return {
            "success": True,
            "pagination": pagination_info,
            "questions": questions,
            "vendor_filter": vendor
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/questions/{question_id}")
def get_question(question_id: str):
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Get question data
        cur.execute("SELECT * FROM questions WHERE question_id = %s", (question_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Question not found")

        colnames = [desc[0] for desc in cur.description]
        result = dict(zip(colnames, row))
        
        # Get answers
        cur.execute(
            "SELECT option, text, is_correct FROM answers WHERE question_id = %s ORDER BY option",
            (question_id,)
        )
        ans_rows = cur.fetchall()
        
        answers = []
        for opt, txt, is_c in ans_rows:
            answers.append({
                "option": opt,
                "text": txt,
                "is_correct": is_c
            })
        
        result["answers"] = answers
        
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