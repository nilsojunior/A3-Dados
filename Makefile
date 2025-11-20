DB = imdb_db

all:
	@psql -U postgres -c "CREATE DATABASE $(DB)"
	@psql -U postgres -d $(DB) -f db.sql

	@mongoimport --db=$(DB) \
            --collection=titles \
            --file=imdb.json \
			--jsonArray
test:
	@node index.js

clean:
	@psql -U postgres -c "drop database $(DB)"
	@mongosh --eval "db.getSiblingDB('$(DB)').dropDatabase()"
