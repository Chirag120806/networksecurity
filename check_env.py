import ssl
import sys
import pymongo

print("Python executable:", sys.executable)
print("Python version:", sys.version)
print("OpenSSL version:", ssl.OPENSSL_VERSION)
print("pymongo version:", pymongo.__version__)
