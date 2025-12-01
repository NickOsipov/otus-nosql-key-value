exec:
	docker exec -it redis bash

pipe:
	python3 src/pipeline.py

insert:
	python3 infra/insert.py