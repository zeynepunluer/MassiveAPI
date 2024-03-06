
from flask import Flask, request, jsonify
import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASS


app = Flask(__name__)


def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST
        )
        return conn
    except psycopg2.Error as e:
        print("Database connection is failed:", e)


@app.route("/assignment/query", methods=["POST"])
def handle_query():
    request_data = request.json
    filters = request_data.get("filters", {})
    ordering = request_data.get("ordering", [])
    page = request.args.get("page", default=1, type=int)
    page_size = request.args.get("page_size", default=10, type=int)


    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Cannot connect to the database"}), 500

    try:

        query = "SELECT * FROM report_output WHERE TRUE"
        params = []


        for column, value in filters.items():
            query += f" AND {column} = %s"
            params.append(value)


        for order_item in ordering:
            for column, direction in order_item.items():
                query += f" ORDER BY {column} {direction}"


        query += f" LIMIT {page_size} OFFSET {(page - 1) * page_size}"


        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()


        cursor.close()
        conn.close()


        return jsonify({"page": page, "page_size": page_size, "count": len(results), "results": [dict(row) for row in results]})
    except psycopg2.Error as e:
        print("SQL query error:", e)
        return jsonify({"error": "SQL query error"}), 500

if __name__ == "__main__":
    app.run(debug=True)
