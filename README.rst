
mongo-galvani
=============

MongoDB Create/Update/Delete activity simulator based on asyncio, aiohttp and python-eve.

+ Runs an Eve/Flask HTTP server as a REST interface to the database.
+ Runs separate green threads that loop continuously, randomly creating,
  updating and deleting via concurrent HTTP requests.

