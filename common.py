#!/usr/bin/env python2

import json, socket, os, random, string, struct
from Crypto.PublicKey import RSA

def receiveJson(sock):
    message = ""
    while True:
        part = sock.recv(4096)
        if part:
            message += part
        else:
            break
    return json.loads(message)

def sendJson(sock, message):
    sock.sendall(json.dumps(message))
    sock.shutdown(socket.SHUT_WR)

def sendAndGet(address, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)
    sendJson(sock, message)
    ret = receiveJson(sock)
    sock.close()
    return ret

def sendInt32(sock, n):
    sock.sendall(struct.pack("i", n))

def recvInt32(sock):
    return struct.unpack("i", recvTotal(sock, 4))[0]

def str2addr(s):
    spl = s.split(":")
    return (spl[0], int(spl[1]))

def copySocketToFile(sock, out, total):
    got = 0
    while got < total:
        toGet = total - got
        if toGet > 4096:
            toGet = 4096
        part = sock.recv(toGet)
        if not part:
            raise RuntimeError
        out.write(part)
        got += len(part)

def recvTotal(sock, total):
    buf = ""
    got = 0
    while got < total:
        toGet = total - got
        if toGet > 4096:
            toGet = 4096
        part = sock.recv(toGet)
        if not part:
            raise RuntimeError
        buf += part
        got += len(part)
    return buf

def newKeyPair():
    def removeExtra(s):
        return "".join(s.split("\n")[1:-1])
    key = RSA.generate(1024)
    private = removeExtra(key.exportKey())
    public = removeExtra(key.publickey().exportKey())
    return private, public

def randomFileName(directory, pre="", post=""):
    if not os.path.exists(directory):
        raise RuntimeError
    while True:
        code = ''.join(random.choice(string.ascii_uppercase + string.digits) \
                for x in range(12))
        path = os.path.join(directory, pre + code + post)
        if not os.path.exists(path):
            return code

        
