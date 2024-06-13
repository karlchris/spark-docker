up:
	docker compose up -d --build

down:
	docker compose down --rmi all

dev:
	docker exec -it spark-master bash
