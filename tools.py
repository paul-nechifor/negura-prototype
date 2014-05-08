#!/usr/bin/env python2

import json, sys
from Crypto.PublicKey import RSA

import common

def createServer(configFile, keyFile, name=None, port=None, bSize=None, mBlocks=None):
    private, public = common.newKeyPair()
    j = {}
    j["private-key"] = private
    j["public-key"] = public
    private, public = common.newKeyPair()
    j["admin-private-key"] = private
    j["admin-public-key"] = public

    out = open(keyFile, "w")
    out.write(json.dumps(j, indent=4))
    out.close()

    c = {}
    if name == None:
        c["name"] = raw_input("Name: ")
    else:
        c["name"] = name
    if port == None:
        c["port"] = int(raw_input("Port: "))
    else:
        c["port"] = int(port)
    if bSize == None:
        c["block-size"] = int(raw_input("Block size (in KiB): ")) * 1024
    else:
        c["block-size"] = int(bSize) * 1024
    if mBlocks == None:
        c["minimum-blocks"] = int(raw_input("Minimum blocks: "))
    else:
        c["minimum-blocks"] = int(mBlocks)

    c["check-in-time"] = 3600
    c["listen"] = 100
    c["users"] = {}
    c["operations"] = []
    c["blocks"] = []
    c["next-id"] = "1"

    keys = json.loads(open(keyFile).read())
    c["public-key"] = keys["public-key"]
    c["admin-public-key"] = keys["admin-public-key"]

    out = open(configFile, "w")
    out.write(json.dumps(c, indent=4))
    out.close()

if __name__ == "__main__":
    v = sys.argv
    if v[1] == "createserver":
        if len(v) > 3:
            createServer(v[2], v[3], v[4], v[5], v[6], v[7])
        else:
            createServer(v[2], v[3])
