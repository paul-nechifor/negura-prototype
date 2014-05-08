#!/usr/bin/env python2

import sys, json, os, time
from subprocess import Popen as run
import common

def addFile(configFile, filePath, storePath):
    config = json.loads(open(configFile).read())

    req = {}
    req["request"] = "local-add"
    req["file-path"] = filePath
    req["store-path"] = storePath

    clientAddress = ("", config["port"])
    discard = common.sendAndGet(clientAddress, req)

def addDir(configFile, dirPath, storePath):
    for f in os.listdir(dirPath):
        p1 = os.path.join(dirPath, f)
        p2 = os.path.join(storePath, f)
        print p1, p2
        addFile(configFile, p1, p2)

def getFile(configFile, storedPath, savePath):
    config = json.loads(open(configFile).read())

    req = {}
    req["request"] = "local-pull-file"
    req["stored-path"] = storedPath
    req["save-path"] = savePath

    clientAddress = ("", config["port"])
    discard = common.sendAndGet(clientAddress, req)

def startFtp(configFile, port):
    config = json.loads(open(configFile).read())

    req = {}
    req["request"] = "local-start-ftp"
    req["port"] = int(port)

    clientAddress = ("", config["port"])
    discard = common.sendAndGet(clientAddress, req)


def register6():
    params = ['gnome-terminal', '--geometry', '60x24', '--command']
    s = ["tools.py", "createserver", "server.json", "keys.json", "Server Muzica", "5000", "256", "128"]
    run(s)
    time.sleep(2)
    run(params + ["server.py server.json"])
    for i, u in enumerate(["a", "b", "c", "d", "e", "f"]):
        time.sleep(1)
        out = open("out.sh", "w")
        out.write(r'echo -e "127.0.0.1\\n5000\\n128\\n%s_blocks\\n%d\\n" | client.py register %s.json' % (u, 20000+i, u))
        out.close()
        run(params + ["sh out.sh"])
        #os.remove("out.sh")

def start6():
    params = ['gnome-terminal', '--geometry', '60x24', '--command']
    run(params + ["server.py server.json"])
    for i, u in enumerate(["a", "b", "c", "d", "e", "f"]):
        time.sleep(1)
        out = open("out.sh", "w")
        out.write(r'client.py start %s.json' % u)
        out.close()
        run(params + ["sh out.sh"])
        #os.remove("out.sh")
    

if __name__ == "__main__":
    if sys.argv[1] == "add":
        addFile(sys.argv[2], sys.argv[3], sys.argv[4])
    elif sys.argv[1] == "adddir":
        addDir(sys.argv[2], sys.argv[3], sys.argv[4])
    elif sys.argv[1] == "get":
        getFile(sys.argv[2], sys.argv[3], sys.argv[4])
    elif sys.argv[1] == "reg6":
        register6()
    elif sys.argv[1] == "start6":
        start6()
    elif sys.argv[1] == "startftp":
        startFtp(sys.argv[2], sys.argv[3])
    else:
        print "No such command"
        exit(1)
