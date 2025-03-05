import csv
import os
import psycopg2



table_names = { 
    "open_orders_by_delivery_status": """
        SELECT 
            (timestamp 'epoch' + "delivery_date"::double precision * interval '1 day')::date AS "delivery_date",
            "status" AS delivery_status,
            "open_count" AS "open_items_count"
        FROM public."open_orders_by_delivery_status";
    """,
    "pending_items_by_product": """
        SELECT *
        FROM public."pending_items_by_product"
        ORDER BY "pending_count" DESC;
    """,
    "top3_customers_pending_orders": """
        SELECT *
        FROM public."top3_customers_pending_orders"
        ORDER BY "rank_num" ASC;
    """,    
    "top3_delivery_dates": """
        SELECT 
        rank_num,
        (timestamp 'epoch' + delivery_date::double precision * interval '1 day')::date AS delivery_date,
        open_count as open_items_count
        FROM public."top3_delivery_dates" 
        ORDER BY "rank_num" ASC;
    """
}

def fetch_data_from_db(query, db_config):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    return colnames, rows

def write_to_csv(file_path, colnames, rows):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(colnames)
        writer.writerows(rows)

if __name__ == "__main__":
    db_config = {
        'dbname': 'sink_db',
        'user': 'sink_user',
        'password': 'sink_password',
        'host': 'localhost',
        'port': '5433'
    }

    for table_name, query in table_names.items():
        colnames, rows = fetch_data_from_db(query, db_config)
        output_path = f'./csv_result/{table_name}.csv'
        write_to_csv(output_path, colnames, rows)