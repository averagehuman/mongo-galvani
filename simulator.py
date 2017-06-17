"""
MongoDB Create/Update/Delete activity simulator.

+ Runs a python-eve/flask HTTP server as a REST interface to the database.
+ Runs separate green threads that loop continuously, randomly creating,
  updating and deleting.

Lowering ACTIVITY_MAX_WAIT will increase the request rate.

Requires:  python-eve and fake-factory.
Tested on: Python 3.6
"""
import sys
import json
import random
import logging
import threading
import asyncio
from time import sleep
from datetime import datetime

import pymongo
import aiohttp
import eve
import faker
from faker.providers.color import Provider as FakeColorProvider

logger = logging.getLogger(__name__)

TAGS = [colour.lower() for colour in FakeColorProvider.all_colors.keys()]

PERSON = {
    'fname': {
        'type': 'string',
        'minlength': 1,
        'maxlength': 20,
        'required': True,
    },
    'lname': {
        'type': 'string',
        'minlength': 1,
        'maxlength': 25,
        'required': True,
    },
    'username': {
        'type': 'string',
        'minlength': 1,
        'maxlength': 30,
        'required': True,
        'unique': True,
    },
    'email': {
        'type': 'string',
        'maxlength': 50,
        'required': True,
        'unique': True,
    },
    'dob': {
        'type': 'datetime',
    },
    # 'tags' is a list, and can only contain values from 'allowed'.
    'tags': {
        'type': 'list',
        'allowed': TAGS
    },
    # An embedded 'strongly-typed' dictionary.
    'location': {
        'type': 'dict',
        'schema': {
            'address': {'type': 'string'},
            'city': {'type': 'string'}
        },
    },
}


class WSGIServerThread(threading.Thread):
    """
    Run the Eve/Flask server in its own thread.
    """

    def __init__(self, application, host, port):
        self._app = application
        self._host = host
        self._port = port
        super().__init__()

    def run(self):
        self._app.run(host=self._host, port=self._port, debug=False)


class ActivitySimulatorBase:
    """
    Base functionality. Override the 'start' method.
    """

    MONGO_HOST = '127.0.0.1'
    MONGO_PORT = 27017
    MONGO_DBNAME = 'test'
    LOCALE = 'en_GB'
    DATE_FORMAT = '%a, %d %b %Y %H:%M:%S'

    def __init__(self, host='127.0.0.1', port=5000, options=None):
        self.host = host
        self.port = port
        if options:
            self.__dict__.update(options)
        self.fake = faker.Faker(self.LOCALE)
        self.started = False
        self._client = None
        self._db = None
        self._loop = None
        self._session = None
        self._app = None
        self._server = None
        self._eve_settings = None

    @property
    def client(self):
        if self._client is None:
            self._client = pymongo.MongoClient(self.MONGO_HOST, self.MONGO_PORT)
        return self._client

    @property
    def db(self):
        if self._db is None:
            self._db = self.client[self.MONGO_DBNAME]
        return self._db

    @property
    def loop(self):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        return self._loop

    @property
    def session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(loop=self.loop)
        return self._session

    @property
    def app(self):
        if self._app is None:
            self._app = eve.Eve(settings=self.eve_settings)
        return self._app

    @property
    def eve_settings(self):
        if not self._eve_settings:
            self._eve_settings = dict(
                MONGO_HOST=self.MONGO_HOST,
                MONGO_PORT=self.MONGO_PORT,
                MONGO_DBNAME=self.MONGO_DBNAME,
                DOMAIN=self.DOMAIN,
                DATE_FORMAT=self.DATE_FORMAT,
                OPLOG=True,
            )
        return self._eve_settings

    async def random_wait(self):
        """
        Sleep for a random number of seconds.
        """
        await asyncio.sleep(random.randint(0, self.ACTIVITY_MAX_WAIT))

    def _start_server_thread(self):
        self._server = WSGIServerThread(self.app, self.host, self.port)
        self._server.daemon = True
        self._server.start()

    def start(self):
        self._start_server_thread()

    def terminate(self):
        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()


class UserSimulator(ActivitySimulatorBase):
    HEADERS = {'Content-Type': 'application/json'}
    MAX_CONCURRENCY = 50

    # activity parameters
    ACTIVITY_MAX_WAIT = 3        # upper limit to waits between creates/updates/deletes
    ACTIVITY_STATUS_WAIT = 5     # wait between status messages
    CREATE_LIKELIHOOD = 0.4
    DELETE_LIKELIHOOD = 0.3
    UPDATE_LIKELIHOOD = 0.4

    # user api
    USER_INITIAL_POPULATION = 100

    DOMAIN = {
        'users': {
            'schema': PERSON,
            'resource_methods': ['GET', 'POST'],
            'item_methods': ['GET', 'PATCH', 'PUT', 'DELETE'],
            'additional_lookup': {
                'url': 'regex("[-\w]+")',
                'field': 'username'
            },
        }
    }

    # cache of (userid, etag) pairs
    USER_SET = set()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_create_url = 'http://%s:%s/users/' % (self.host, self.port)
        self.user_endpoint_pattern = self.user_create_url + '%s'

    def start(self):
        super().start()
        self._create_indicies()
        self._find_existing_dataset()
        if not self.USER_SET:
            self.loop.run_until_complete(asyncio.Task(self._populate()))
        self.loop.run_until_complete(asyncio.gather(
            self._activity_logger(),
            self._create_user(),
            self._update_user(),
            self._delete_user(),
        ))

    def _create_indicies(self):
        """
        Create indexes on mongodb collections for better query performance.
        """
        # username is unique in users collection
        self.db.users.create_index([('username', pymongo.ASCENDING)], unique=True)

    def _find_existing_dataset(self):
        """
        If the users collection is non-empty populate the USER_SET cache.
        """
        for user in self.db.users.find():
            userid = str(user.get('_id', ''))
            if userid:
                etag = user.get('_etag', '')
                self.USER_SET.add((userid, etag))
        existing = len(self.USER_SET)
        if existing > 0:
            logger.info('Found %d existing users' % existing)

    def _fake_user_tags(self):
        """
        Generate a list of HTML colour names to represent a list of tags.
        """
        return [self.fake.color_name().lower() for _ in range(0, random.randint(0, 10))]

    def _fake_user_data(self):
        """
        Use fake-factory to create real-looking user data matching the mongo schema.
        """
        fname = self.fake.first_name()
        lname = self.fake.last_name()
        email = '%s.%s@mail.localhost' % (fname.lower(), lname.lower())
        return {
            'fname': fname,
            'lname': lname,
            'email': email,
            'username': self.fake.user_name(),
            'dob': self.fake.date_time().strftime(self.DATE_FORMAT),
            'tags': self._fake_user_tags(),
            'location': {
                'address': self.fake.street_address(),
                'city': self.fake.city(),
            },
        }

    def _fake_user_update(self):
        """
        Generate fake update for an existing user.
        """
        probability = self.UPDATE_LIKELIHOOD / 3.0
        payload = {}
        if random.random() < probability:
            payload['email'] = '%s.%s@mail.localhost' % (self.fake.word(), self.fake.word())
        if random.random() < probability:
            payload['tags'] = self._fake_user_tags()
        if random.random() < probability:
            payload['location'] = {'address': self.fake.street_address()}
        return payload

    #-------------------------------------------------------------------------------------
    # Coroutines
    #-------------------------------------------------------------------------------------
    async def _create_users(self, count):
        """
        Bulk create users via async HTTP requests to the Eve server.
        """
        session = self.session
        url = self.user_create_url
        for _ in range(count):
            async with session.post(url, json=self._fake_user_data()) as response:
                data = await response.json()
                status = data.get('_status')
                userid = data.get('_id')
                etag = data.get('_etag', '')
                if status != 'OK' or not userid:
                    logger.error("Unexpected response - %s" % data)
                else:
                    self.USER_SET.add((userid, etag))
                    logger.info("Created user '%s'" % userid)

    async def _populate(self):
        """
        If no initial data set is found, create one here.

        Users are created via batches of async POSTs to the Eve server with 'MAX_CONCURRENCY'
        requests in each batch.
        """
        factor, remainder = divmod(self.USER_INITIAL_POPULATION, self.MAX_CONCURRENCY)
        for _ in range(factor):
            await self._create_users(self.MAX_CONCURRENCY)
            asyncio.sleep(2)
        if remainder:
            await self._create_users(remainder)

    async def _activity_logger(self):
        """
        Print out an activity summary.
        """
        while True:
            usercount = self.db.users.count()
            timestamp = datetime.now().strftime(self.DATE_FORMAT)
            msg = 'There are %s (%s) users on %s' % (usercount, len(self.USER_SET), timestamp)
            logger.debug(msg)
            await asyncio.sleep(self.ACTIVITY_STATUS_WAIT)

    async def _create_user(self):
        """
        Continuously create new users.
        """
        while True:
            if random.random() < self.CREATE_LIKELIHOOD:
                await self._create_users(1)
            await self.random_wait()

    async def _update_user(self):
        """
        Continuously update existing users at random.
        """
        while True:
            if self.USER_SET:
                # select an existing user at random and send a PATCH request with a fake update.
                userid, etag = random.choice(list(self.USER_SET))
                url = self.user_endpoint_pattern % userid
                headers = dict(self.HEADERS)
                if etag:
                    headers['If-Match'] = etag
                update = self._fake_user_update()
                # Update data is generated at random and may be empty.
                # An empty payload is a no-op but ignore in any case.
                if update:
                    async with self.session.patch(url, json=update, headers=headers) as response:
                        if response.status >= 400:
                            logger.error(await response.text())
                        else:
                            # Need to update cached etag
                            data = await response.json()
                            newtag = data['_etag']
                            try:
                                self.USER_SET.remove((userid, etag))
                            except KeyError:
                                pass
                            self.USER_SET.add((userid, newtag))
                            logger.info("Updated user '%s'. Payload -> %s" % (userid, update))
            await self.random_wait()

    async def _delete_user(self):
        """
        Continuously delete existing users at random.
        """
        while True:
            if self.USER_SET and random.random() < self.DELETE_LIKELIHOOD:
                # select an existing user at random and send a DELETE request
                userid, etag = random.choice(list(self.USER_SET))
                url = self.user_endpoint_pattern % userid
                headers = dict(self.HEADERS)
                if etag:
                    headers['If-Match'] = etag
                async with self.session.delete(url, headers=headers) as response:
                    if response.status >= 400:
                        logger.error(await response.text())
                    else:
                        try:
                            self.USER_SET.remove((userid, etag))
                        except KeyError:
                            pass
                        logger.info("Deleted user '%s'." % userid)
            await self.random_wait()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(threadName)10s [%(levelname)s] %(name)18s: %(message)s',
        stream=sys.stderr,
    )
    simulator = UserSimulator()
    try:
        simulator.start()
    except KeyboardInterrupt:
        print('\nBye.')
    finally:
        simulator.terminate()


