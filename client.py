#!/usr/bin/env python2

import socket, json, sys, os, threading, time, hashlib, copy, math, random, io, shutil

import common, clientftp

class NeguraClient:
    protocol = "0.1"
    software = "NeguraClient 0.1"

    def __init__(self, configFile):
        self.configFile = configFile
        self.config = None
        self.serverAddr = None
        self.blockQueue = None
        self.blockQueueLock = threading.RLock()
        self.blockCodeLock = threading.RLock()
        self.storeCodesLock = threading.RLock()
        self.peerBlockCache = {}
        self.blockCacheLock = threading.RLock()
        self.lastBlockCacheUpdate = time.time()
        self.tellBlocks = []
        self.lastTellBlockUpdate = time.time()
        self.blockFinishedLock = threading.RLock()
        self.upConnections = 0
        self.upConnectionsLock = threading.RLock()
        self.alive = True
        self.lastFilesystemUpdate = time.time()
        self.filesystemLock = threading.RLock()
        self.ftpServer = None

    def log(self, text):
        sys.stdout.write(text + "\n")
        sys.stdout.flush()

    def sendResponse(self, sock, message):
        message["protocol"] = NeguraClient.protocol
        message["software"] = NeguraClient.software
        common.sendJson(sock, message)

    def sendAndGet(self, address, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)
        self.sendResponse(sock, message)
        ret = common.receiveJson(sock)
        sock.close()
        return ret

    def processRequest(self, sock, address):
        message = common.receiveJson(sock)

        if not message.has_key("request"):
            self.sendErrorAndClose(sock, "No request.")
            return

        self.log("From %s:%d: %s" % \
                (address[0], address[1], message["request"]))

        methodName = "process_" + message["request"].replace("-", "_")

        if not hasattr(self, methodName):
            self.sendErrorAndClose(sock, "Request not known.")
            return

        method = getattr(self, methodName)

        method(sock, address, message)
        sock.close()

    def process_local_add(self, sock, address, message):
        filePath = message["file-path"]
        blockSize = self.config["server-info"]["block-size"]

        fileSize = os.stat(filePath).st_size
        noBlocks = int(math.ceil(float(fileSize) / blockSize))

        # Get the operation id and the block ids.
        req = {"request": "allocate-operation", "number-of-blocks": noBlocks}
        ids = self.sendAndGet(self.serverAddr, req)
        
        req = {}
        req["request"] = "add-operation"
        req["uid"] = self.config["uid"]
        req["op"] = {}
        req["op"]["op"] = "add"
        req["op"]["opid"] = ids["opid"]
        req["op"]["date"] = int(time.time())
        req["op"]["path"] = message["store-path"]
        req["op"]["size"] = fileSize
        req["op"]["blocks"] = []

        k = 0
        f = open(filePath)
        fileDigest = hashlib.sha512()

        for i in xrange(noBlocks):
            block = f.read(blockSize)
            with self.blockCodeLock:
                storeCode = common.randomFileName(self.config["block-dir"], post=".blk")
            blockPath = os.path.join(self.config["block-dir"], storeCode + ".blk")
            out = open(blockPath, "w")
            out.write(block)
            k += len(block)

            blockHash = hashlib.sha256(block).hexdigest()
            with self.storeCodesLock:
                self.config["store-codes"][str(ids["block-ids"][i])] = storeCode
            self.config["extra-blocks"].add(ids["block-ids"][i])
            fileDigest.update(block)

            req["op"]["blocks"].append({"id": ids["block-ids"][i], "hash": blockHash})

        req["op"]["hash"] = fileDigest.hexdigest()

        response = self.sendAndGet(self.serverAddr, req)

        # Release tool after completion.
        self.sendResponse(sock, {})
        sock.close()

    def process_local_pull_file(self, sock, address, message):
        self.updateFilesystem(True)

        op = None
        with self.filesystemLock:
            for o in self.config["filesystem"]:
                if o["op"] == "add" and o["path"] == message["stored-path"]:
                    op = o
                    break

        if op == None:
            self.log("File to download doesn't exist: " + message["stored-path"])
            return

        fio = io.FileIO(message["save-path"], "w")

        for b in op["blocks"]:
            self.passBlockThrough(b["id"], fio)

        fio.close()

        # Release tool after completion.
        self.sendResponse(sock, {})
        sock.close()

    def process_local_start_ftp(self, sock, address, message):
        self.sendResponse(sock, {})
        sock.close()

        self.updateFilesystem(True)

        if self.ftpServer == None:
            self.ftpServer = clientftp.NeguraFtpServer(message["port"], self)
            self.ftpServer.start()

    def process_block_announce(self, sock, address, message):
        self.sendResponse(sock, {})
        sock.close()

        self.config["block-list"] = message["blocks"]
        with self.blockQueueLock:
            self.blockQueue = message["blocks"] # TEMP TODO FIX TODO
            self.log("Block queue is: %s" % self.blockQueue)

    def process_up_block(self, sock, address, message):
        bid = message["block-id"]

        if not self.config["store-codes"].has_key(str(bid)):
            common.sendInt32(sock, 3) # I don't have it.
            sock.close()
            return

        sendIt = False
        with self.upConnectionsLock:
            if self.upConnections < self.config["max-up-connections"]:
                self.upConnections += 1
                sendIt = True

        if not sendIt:
            common.sendInt32(sock, 2) # I have it, but no free connections.
            sock.close()
            return

        common.sendInt32(sock, 1) # I'll send it

        self.log("Sending block %d to %s:%d. Connections open: %d" % \
            (bid, address[0], address[1], self.upConnections))
        
        storeCode = self.config["store-codes"][str(bid)]
        blockPath = os.path.join(self.config["block-dir"], storeCode + ".blk")
        f = open(blockPath)
        if message.has_key("position"):
            f.seek(message["position"])
        if message.has_key("length"):
            block = f.read(message["length"])
        else:
            block = f.read()
        common.sendInt32(sock, len(block))
        sock.sendall(block)
        sock.close()

        with self.upConnectionsLock:
            self.upConnections -= 1

    def peersForBlockId(self, bid):
        updateCache = False
        if time.time() - self.lastBlockCacheUpdate > 120:
            updateCache = True
        if not self.peerBlockCache.has_key(str(bid)):
            updateCache = True

        if updateCache:
            with self.blockCacheLock:
                blist = copy.deepcopy(self.blockQueue)
                if blist.count(bid) == 0:
                    blist.append(bid)

                req = {"request": "peers-for-blocks", "blocks": blist}
                resp = self.sendAndGet(self.serverAddr, req)
                for p in resp["blocks"]:
                    self.peerBlockCache[str(p["id"])] = p["peers"]
                self.lastBlockCacheUpdate = time.time()

        return copy.deepcopy(self.peerBlockCache[str(bid)])

    def getPartOfBlock(self, bid, position, length):
        if self.config["store-codes"].has_key(str(bid)):
            self.log("Block %d (%d %d) exists locally. " % (bid, position, length))
            storeCode = self.config["store-codes"][str(bid)]
            blockPath = os.path.join(self.config["block-dir"], storeCode + ".blk")
            src = open(blockPath)
            src.seek(position)
            ret = src.read(length)
            src.close()
            return ret
        else:
            # Get the peers for this block in a random order.
            peers = self.peersForBlockId(bid)
            random.shuffle(peers)

            for peer in peers:
                req = {"request": "up-block", "block-id": bid}
                req["position"] = position
                req["length"] = length
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.connect(common.str2addr(peer))
                except:
                    sock.close()
                    continue
                self.sendResponse(sock, req)
                answerCode = common.recvInt32(sock)
                if answerCode == 2: # I have it, but no free conections.
                    sock.close()
                elif answerCode == 3: # I don't have it.
                    sock.close()
                elif answerCode == 1: # I have it.
                    sentLength = common.recvInt32(sock)
                    block = common.recvTotal(sock, sentLength)
                    self.log("Block %d (%d %d) from %s. " % (bid, position, length, peer))
                    sock.close()
                    return block
            self.log("Could not stream the file because of block %s" % bid)
        return None

    def readFromFile(self, opid, position, length):
        blockSize = self.config["server-info"]["block-size"]
        op = self.config["filesystem"][opid]
        blocks = op["blocks"]
        size = op["size"]

        buf = ""

        while length > 0:
            bindex = position / blockSize
            bPosition = position % blockSize
            bLength = length
            if bLength + bPosition > blockSize:
                bLength = blockSize - bPosition
            buf += self.getPartOfBlock(blocks[bindex]["id"], bPosition, bLength)
            position += bLength
            length -= bLength

        return buf

    def updateFilesystem(self, forced=False):
        if forced or time.time() - self.lastFilesystemUpdate > 60:
            self.lastFilesystemUpdate = time.time()
            with self.filesystemLock:
                req = {}
                req["request"] = "filesystem-state"
                req["from"] = len(self.config["filesystem"])
                resp = self.sendAndGet(self.serverAddr, req)
                for op in resp["operations"]:
                    self.config["filesystem"].append(op)
                    clientftp.NeguraFs.addOperation(op)

    def allocatedBlockGet(self):
        while self.alive:
            time.sleep(0.2)
            bid = None
            with self.blockQueueLock:
                if len(self.blockQueue) > 0:
                    bid = self.blockQueue.pop(0)

            if time.time() - self.lastTellBlockUpdate > 60:
                self.lastTellBlockUpdate = time.time()
                if len(self.tellBlocks) > 0:
                    req = {}
                    req["request"] = "have-blocks"
                    req["uid"] = self.config["uid"]
                    req["blocks"] = self.tellBlocks
                    discard = self.sendAndGet(self.serverAddr, req)
                    self.tellBlocks = []

            if bid == None:
                continue

            gotIt = self.getBlock(bid)

            # If I didn't get it, add it to the end of the queue.
            if not gotIt:
                with self.blockQueueLock:
                    self.blockQueue.append(bid)
            else:
                with self.blockFinishedLock:
                    self.config["block-list-finished"].append(bid)
                self.tellBlocks.append(bid)

    def passBlockThrough(self, bid, out):
        if self.config["store-codes"].has_key(str(bid)):
            self.log("Block %d exists locally. " % bid)
            storeCode = self.config["store-codes"][str(bid)]
            blockPath = os.path.join(self.config["block-dir"], storeCode + ".blk")
            src = io.FileIO(blockPath, "r")
            shutil.copyfileobj(src, out)
        else:
            # Get the peers for this block in a random order.
            peers = self.peersForBlockId(bid)
            random.shuffle(peers)

            for peer in peers:
                req = {"request": "up-block", "block-id": bid}
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.connect(common.str2addr(peer))
                except:
                    sock.close()
                    continue
                self.sendResponse(sock, req)
                answerCode = common.recvInt32(sock)
                if answerCode == 2: # I have it, but no free conections.
                    sock.close()
                elif answerCode == 3: # I don't have it.
                    sock.close()
                elif answerCode == 1: # I have it.
                    blockSize = common.recvInt32(sock)
                    common.copySocketToFile(sock, out, blockSize)
                    sock.close()
                    self.log("Got block %s from %s." % (bid, peer))
                    return
            self.log("Could not stream the file because of block %s" % bid)

    # Return True if the blocks is now in the storage.
    def getBlock(self, bid):
        # Check if the block exits.
        if self.config["store-codes"].has_key(str(bid)):
            return True

        # Get the peers for this block in a random order.
        peers = self.peersForBlockId(bid)
        random.shuffle(peers)

        for peer in peers:
            req = {"request": "up-block", "block-id": bid}
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(common.str2addr(peer))
            except:
                sock.close()
                continue
            self.sendResponse(sock, req)
            answerCode = common.recvInt32(sock)
            if answerCode == 2: # I have it, but no free conections.
                sock.close()
            elif answerCode == 3: # I don't have it.
                sock.close()
            elif answerCode == 1: # I have it.
                blockSize = common.recvInt32(sock)
                block = common.recvTotal(sock, blockSize)
                sock.close()
                blockHash = hashlib.sha256(block).hexdigest()
                # TODO check the hash TODO
                storeCode = None
                with self.blockCodeLock:
                    storeCode = common.randomFileName(self.config["block-dir"], post=".blk")
                blockPath = os.path.join(self.config["block-dir"], storeCode + ".blk")
                out = open(blockPath, "w")
                out.write(block)
                with self.storeCodesLock:
                    self.config["store-codes"][str(bid)] = storeCode
                self.log("Got block %s from %s." % (bid, peer))
                return True

    def start(self):
        self.config = json.loads(open(self.configFile).read())
        self.config["extra-blocks"] = set(self.config["extra-blocks"])
        self.serverAddr = (self.config["server-ip"], self.config["server-port"])
        self.blockQueue = self.config["block-list"]

        for op in self.config["filesystem"]:
            clientftp.NeguraFs.addOperation(op)

        # Create the block directory if it doesn't exist
        try:
            os.mkdir(self.config["block-dir"])
        except OSError:
            pass

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("", self.config["port"]))
        sock.listen(self.config["listen"])

        # Start the thread which gets the blocks which were allocated by the
        # server.
        thread = threading.Thread(target=self.allocatedBlockGet)
        thread.start()

        try:
            self.log("Started client with uid %s" % self.config["uid"])
            while self.alive:
                conn, address = sock.accept()
                thread = threading.Thread(target=self.processRequest,
                        args=(conn, address))
                thread.start()
        except KeyboardInterrupt:
            print "Client will shut down."
            self.alive = False
            # Wait for all the threads to finish so I can write the config file.
            while threading.activeCount() > 1:
                time.sleep(0.1)
            out = open(self.configFile, "w")
            self.config["extra-blocks"] = list(self.config["extra-blocks"])
            out.write(json.dumps(self.config, indent=4))
            out.close()
            exit(0)

    def registerAndStart(self):
        ipAddress = raw_input("Server IP address: ")
        serverPort = int(raw_input("Server port: "))
        serverAddress = (ipAddress, serverPort)
        info = self.sendAndGet(serverAddress, {"request": "server-info"})

        print "Server name: '%s'" % info["name"]
        print "Minimum blocks: %d" % info["minimum-blocks"]

        storedBlocks = 0
        while storedBlocks < info["minimum-blocks"]:
            storedBlocks = int(raw_input("Blocks to store: "))

        while True:
            blockDir = raw_input("Directory to store blocks: ")
            try:
                os.mkdir(blockDir)
            except OSError:
                pass
            if os.path.exists(blockDir):
                break

        port = int(raw_input("Port to serve: "))

        print "Maximum space used: %0.2f MiB" % \
                (storedBlocks * info["block-size"] / (1024 ** 2))

        config = {}
        config["server-ip"] = ipAddress
        config["server-port"] = serverPort
        config["server-info"] = info
        config["stored-blocks"] = storedBlocks
        config["block-list"] = []
        config["block-list-finished"] = []
        config["extra-blocks"] = []
        config["block-dir"] = os.path.abspath(blockDir)
        config["store-codes"] = {}
        config["port"] = port
        config["listen"] = 10
        config["max-up-connections"] = 3
        config["filesystem"] = []

        private, public = common.newKeyPair()
        config["private-key"] = private
        config["public-key"] = public

        registration = {}
        registration["request"] = "registration"
        registration["public-key"] = config["public-key"]
        registration["number-of-blocks"] = storedBlocks
        registration["port"] = port

        response = self.sendAndGet(serverAddress, registration)

        if response["registration"] == "failed":
            print "Registration failed: '%s'." % \
                    response["registration-failed-reason"]
            exit(1)

        config["uid"] = response["uid"]
             
        print "Registration succesfull."
        out = open(self.configFile, "w")
        out.write(json.dumps(config, indent=4))
        out.close()

        self.start()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "Help:"
        print "    %s start <configFile> - to start " % sys.argv[0]
        print "    %s register <configFile> - to create <configFile>\n" % \
                sys.argv[0]
        exit(1)

    client = NeguraClient(sys.argv[2])
    if sys.argv[1] == "start":
        client.start()
    elif sys.argv[1] == "register":
        client.registerAndStart()
