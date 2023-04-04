init_database:
	docker exec -it stocks python init_database.py
build:
	docker compose build --no-cache
install:
	docker compose build
up:
	docker compose up -d
ps:
	docker compose ps
version:
	docker exec -it stocks python --version
down:
	docker compose down
bash:
	docker compose exec stocks bash
ls:
	docker container ls
