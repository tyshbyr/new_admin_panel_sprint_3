Поднять проект:
```
docker compose up
```
Залить дамп в пустой постгрес:
```
docker exec -it postgres_db psql -d movies_database -h 127.0.0.1 -U app -f db_dump.sql
```

# Заключительное задание первого модуля

Ваша задача в этом уроке — загрузить данные в Elasticsearch из PostgreSQL. Подробности задания в папке `etl`.
