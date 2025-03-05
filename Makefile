.PHONY: all down up wait alter connector sink stream csv clean

all: up wait alter wait connector sink stream

down: 
	docker compose down -v

up:
	docker compose up -d

wait:
	@echo "Waiting for services"
	sleep 10

alter:
	@echo "ALTER ROLE for CDC in source DB"
	docker compose exec transactions-db psql -U finance_db_user -d finance_db -c "ALTER ROLE cdc_user WITH REPLICATION;"

connector:
	@echo "Connector configuration with CURL"
	curl -X POST -H "Content-Type: application/json" --data @connector.json http://localhost:8083/connectors

sink:
	@echo "Creating sink tables in sink"
	docker compose exec db-sink psql -U sink_user -d sink_db -c "CREATE TABLE IF NOT EXISTS open_orders_by_delivery_status (delivery_date VARCHAR, status VARCHAR, open_count BIGINT, PRIMARY KEY (delivery_date, status));"
	docker compose exec db-sink psql -U sink_user -d sink_db -c "CREATE TABLE IF NOT EXISTS top3_delivery_dates (rank_num BIGINT, delivery_date VARCHAR, open_count BIGINT, PRIMARY KEY (rank_num, delivery_date));"
	docker compose exec db-sink psql -U sink_user -d sink_db -c "CREATE TABLE IF NOT EXISTS pending_items_by_product (product_id BIGINT, pending_count BIGINT, PRIMARY KEY (product_id));"
	docker compose exec db-sink psql -U sink_user -d sink_db -c "CREATE TABLE IF NOT EXISTS top3_customers_pending_orders (rank_num BIGINT, customer_id BIGINT, pending_orders BIGINT, PRIMARY KEY (rank_num, customer_id));"

stream:
	@echo "Running Flink job"
	docker compose exec flink-jobmanager flink run -py /jobs/flink_stream.py	

.requirements_installed: requirements.txt
	pip install -r requirements.txt
	@touch .requirements_installed

csv: .requirements_installed
	@echo "Writing CSV files, look at csv_result folder later"
	python3 csv_writer.py

clean:
	rm -f .requirements_installed
