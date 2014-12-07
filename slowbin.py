
from time import time, sleep
import socket
from urlparse import urlparse

from flask import Flask, Response, abort, jsonify
from redis import StrictRedis
import requests

from config import (
    LENGTH_LIMIT,
    LENGTH_LIMIT_HUMAN,
    CHUNK_SIZE,
    RATE_LIMIT,
    DEFAULT_MIMETYPE,
    CACHE_TIME,
)

from messages import (
    ERROR_CONTENT_HEADER,
    ERROR_CONTENT_HEADER_NOT_INT,
    ERROR_CONTENT_LENGTH,
    ERROR_LOCAL_URL,
    ERROR_BAD_HOSTNAME,
    ERROR_BAD_URL,
    ERROR_SCHEME_NOT_SPECIFIED,
)

slowbin = Flask(__name__)
redis = StrictRedis()


def validate_url(url):
    """Return a tuple (False, "error message") for invalid or local URLs.
    Return (True, '') for all other URLs.

    :param url: String. The URL to validate
    :returns: (Boolean, String). Tuple: valid, error message.
    """
    try:
        o = urlparse(url)
    except:
        return False, ERROR_BAD_URL

    if not o.netloc:
        return False, ERROR_BAD_URL

    if not o.scheme:
        return False, ERROR_SCHEME_NOT_SPECIFIED

    try:
        ip = socket.gethostbyname(o.netloc)
    except:
        return False, ERROR_BAD_HOSTNAME

    if ip == '127.0.0.1':
        return False, ERROR_LOCAL_URL

    return True, ''


def cache_iterator(url, length):
    """Read chunks from Redis key `url`, sized CHUNK_SIZE from 0 to `length`
    :param url: String. The URL to stream
    :param length: Integer. The length of the response
    """
    chunk_sizes = range(0, length, CHUNK_SIZE)

    if length % CHUNK_SIZE:
        chunk_sizes.append(length)

    for chunk in zip(chunk_sizes, chunk_sizes[1:]):
        yield redis.getrange(url, chunk[0], chunk[1] - 1)


def requests_iterator(url):
    """Read chunks from Requests, sized CHUNK_SIZE until response is exhausted.
    :param url: String. The URL to stream
    :returns: Iterator of response chunks
    """
    res = requests.get(url, stream=True)

    for chunk in res.iter_content(chunk_size=CHUNK_SIZE):
        if chunk:
            yield chunk


def stream(url, length, rate, cached):
    """Stream response from Requests or cache. Cache response if not cached.

    :param url: String. The URL to stream
    :param length: Integer. Content-Length of the URL
    :param rate: Float. Time in seconds to complete the stream
    :param cached: Boolean. Whether the response is cached or now
    :returns: Iterator of response chunks
    """
    chunks = length / CHUNK_SIZE
    chunk_start = time()
    total_size = 0

    if cached:
        iterator = cache_iterator(url, length)
    else:
        iterator = requests_iterator(url)

    for chunk in iterator:
        yield chunk

        if not cached:
            redis.append(url, chunk)

        chunks -= 1
        rate -= time() - chunk_start
        total_size += CHUNK_SIZE

        if total_size > LENGTH_LIMIT:
            return

        chunk_start = time()

        if rate > 0 and chunks > 0:
            sleep(rate / chunks)

    if not cached:
        redis.expire(url, CACHE_TIME)
        redis.expire(url + ':mimetype', CACHE_TIME)


@slowbin.errorhandler(400)
def custom400(error):
    """Return JSON response with error message on 400 error.

    :param error: werkzeug.exceptions.HTTPException passed by abort()
    :returns: Flask.Response. JSON error message
    """
    response = jsonify({'message': error.description})
    response.status_code = 400

    return response


@slowbin.route('/<int:rate>/<path:url>')
def fetch(rate, url):
    """Return response `url` in `rate` seconds for valid URLs with valid
    Content-Length headers

    :param rate: Integer. Seconds to return the response in
    :param url: String. URL to stream
    :returns: Flask.Response. The response stream
    """
    start = time()

    if rate > RATE_LIMIT:
        rate = RATE_LIMIT

    if redis.exists(url):
        redis.expire(url, CACHE_TIME)
        redis.expire(url + ':mimetype', CACHE_TIME)

        mimetype = redis.get(url + ':mimetype')
        length = redis.strlen(url)

        # Subtract time used until now
        rate = rate - (time() - start)

        return Response(stream(url, length, rate, True), mimetype=mimetype)

    # Validate initial parameters, preventing local URLs
    valid, error = validate_url(url)

    if not valid:
        return abort(400, error)

    # Get the content-length and content-type from HEAD
    head = requests.head(url)

    # Only respond to status code 200
    if not head.ok:
        return abort(head.status_code)

    mimetype = head.headers.get('content-type', DEFAULT_MIMETYPE)
    length = head.headers.get('content-length')

    # Validate content-length content
    if not length:
        return abort(400, ERROR_CONTENT_HEADER)

    try:
        length = int(length)
    except ValueError:
        return abort(400, ERROR_CONTENT_HEADER_NOT_INT)

    if length > LENGTH_LIMIT:
        return abort(400, ERROR_CONTENT_LENGTH.format(LENGTH_LIMIT_HUMAN))

    redis.set(url + ':mimetype', mimetype)

    # Subtract time used until now
    rate = rate - (time() - start)

    return Response(stream(url, length, rate, False), mimetype=mimetype)
