#!/usr/bin/env python2

import json, sys, threading, socket, random, time
import common

class BlockInfo:
    def __init__(self, id, hash, userIds, peers):
        self.id = id
        self.hash = hash
        self.userIds = userIds
        self.peers = peers

class NeguraServer:
    protocol = "0.1"
    software = "NeguraServer 0.1"

    def __init__(self, configFile):
        self.configFile = configFile
        self.config = json.loads(open(self.configFile).read())
        self.userLock = threading.RLock()
        self.blockLock = threading.RLock()
        self.blockLookupLock = threading.RLock()
        self.operationsLock = threading.RLock()
        self.finishedBlocksLock = threading.RLock()
        self.socket = None
        self.constructBlockLookup()

    def log(self, text):
        print text

    def newId(self):
        with self.userLock:
            now = self.config["next-id"]
            self.config["next-id"] = str(int(now) + 1)
            return now

    def allocateBlocks(self, number):
        with self.blockLock:
            blockIds = [b["id"] for b in self.config["blocks"]]
            random.shuffle(blockIds)
            if len(blockIds) <= number:
                return blockIds
            else:
                return blockIds[:number]

    def constructBlockLookup(self):
        self.blockLookup = {}
        for b in self.config["blocks"]:
            bi = BlockInfo(b["id"], b["hash"], set([]), set([]))
            self.blockLookup[b["id"]] = bi
        for u in self.config["users"].values():
            for b in u["finished-blocks"]:
                self.blockLookup[b].userIds.add(u["uid"])
                addr = (u["ip"], u["port"])
                self.blockLookup[b].peers.add("%s:%d" % addr) 

    def blockLookupAdd(self, userId, blocks):
        with self.userLock:
            u = self.config["users"][userId]
            s = "%s:%d" % (u["ip"], u["port"])
        with self.blockLookupLock:
            for b in blocks:
                self.blockLookup[b].userIds.add(userId)
                self.blockLookup[b].peers.add(s) 

    def blockLookupAddNewBlocks(self, userId, blocks):
        with self.blockLookupLock:
            for b in blocks:
                with self.userLock:
                    u = self.config["users"][userId]
                    x = set([userId])
                    y = set([u["ip"] + ":" + str(u["port"])])
                self.blockLookup[b["id"]] = BlockInfo(b["id"], b["hash"], x, y)


    def realocateAll(self):
        with self.userLock:
            for u in self.config["users"].values():
                u["blocks"] = self.allocateBlocks(u["number-of-blocks"])

    def reannounceUser(self, uid):
        with self.userLock:
            u = self.config["users"][uid]
            self.log("Announcing user %s of %s" % (uid, u["blocks"]))
            req = {"request": "block-announce", "blocks": u["blocks"]}
            try:
                self.sendAndGet((u["ip"], u["port"]), req)
            except socket.error:
                pass

    def reannounceAll(self):
        with self.userLock:
            for uid in self.config["users"].keys():
                self.reannounceUser(uid)

    def userExists(self, ip, port):
        with self.userLock:
            for user in self.config["users"].values():
                if user["ip"] == ip and user["port"] == port:
                    return True
        return False

    def sendResponse(self, sock, message):
        message["protocol"] = NeguraServer.protocol
        message["software"] = NeguraServer.software
        common.sendJson(sock, message)

    def sendAndGet(self, addr, message):
        message["protocol"] = NeguraServer.protocol
        message["software"] = NeguraServer.software
        return common.sendAndGet(addr, message)
        

    def sendErrorAndClose(self, client, errorText):
        self.sendResponse(client, {"error": errorText})
        client.close()

    def processRequest(self, client, address):
        message = common.receiveJson(client)

        if not message.has_key("request"):
            self.sendErrorAndClose(client, "No request.")
            return

        self.log("From %s:%d: %s" % \
                (address[0], address[1], message["request"]))

        methodName = "process_" + message["request"].replace("-", "_")

        if not hasattr(self, methodName):
            self.sendErrorAndClose(client, "Request not known.")
            return

        method = getattr(self, methodName)

        method(client, address, message)
        client.close()

    def process_server_info(self, client, address, message):
        response = {}
        response["name"] = self.config["name"]
        response["public-key"] = self.config["public-key"]
        response["admin-public-key"] = self.config["admin-public-key"]
        response["block-size"] = self.config["block-size"]
        response["minimum-blocks"] = self.config["minimum-blocks"]
        response["check-in-time"] = self.config["check-in-time"]
        self.sendResponse(client, response)

    def process_registration(self, client, address, message):
        if message["number-of-blocks"] < self.config["minimum-blocks"]:
            self.sendResponse(client, {"registration-failed-reason": \
                    "Not enough blocks."})
            client.close()
            return

        if self.userExists(address[0], message["port"]):
            self.sendResponse(client, {"registration-failed-reason": \
                    "IP address and port exists."})
            client.close()
            return

        user = {}
        user["uid"] = self.newId()
        user["ip"] = address[0]
        user["port"] = message["port"]
        user["public-key"] = message["public-key"]
        user["number-of-blocks"] = message["number-of-blocks"]
        user["blocks"] = self.allocateBlocks(message["number-of-blocks"])
        user["finished-blocks"] = []

        with self.userLock:
            self.config["users"][user["uid"]] = user

        response = {}
        response["registration"] = "accepted"
        response["uid"] = user["uid"]

        self.log("User %s registered." % user["uid"])
        self.sendResponse(client, response)

        time.sleep(5)
        self.reannounceUser(user["uid"])

    def process_allocate_operation(self, client, address, message):
        response = {}
        response["block-ids"] = []

        with self.blockLock:
            for i in xrange(message["number-of-blocks"]):
                response["block-ids"].append(len(self.config["blocks"]))
                self.config["blocks"].append(None)
        with self.operationsLock:
            response["opid"] = len(self.config["operations"])
            self.config["operations"].append(None)

        self.sendResponse(client, response)

    def process_add_operation(self, client, address, message):
        self.sendResponse(client, {})
        client.close()

        blockIds = []
        for b in message["op"]["blocks"]:
            blockIds.append(b["id"])

        with self.blockLock:
            for b in message["op"]["blocks"]:
                self.config["blocks"][b["id"]] = b

        with self.operationsLock:
            message["op"]["signature"] = "sig"
            self.config["operations"][message["op"]["opid"]] = message["op"]

        with self.finishedBlocksLock:
            l = self.config["users"][message["uid"]]["finished-blocks"]
            l.extend(blockIds)
            l = list(set(l))

        self.blockLookupAddNewBlocks(message["uid"], message["op"]["blocks"])
        self.realocateAll()
        self.reannounceAll()

    def process_have_blocks(self, client, address, message):
        self.sendResponse(client, {})
        client.close()
        with self.finishedBlocksLock:
            l = self.config["users"][message["uid"]]["finished-blocks"]
            l.extend(message["blocks"])
            l = list(set(l))
        self.blockLookupAdd(message["uid"], message["blocks"])

    def process_filesystem_state(self, client, address, message):
        start = message["from"]

        res = {}
        res["operations"] = []

        ops = self.config["operations"]
        with self.operationsLock:
            if start < len(ops):
                for i in xrange(start, len(ops) - 1):
                    if ops[i] == None:
                        break
                    else:
                        res["operations"].append(ops[i])
        self.sendResponse(client, res)

    def process_peers_for_blocks(self, client, address, message):
        res = {}
        res["blocks"] = []
        for b in message["blocks"]:
            with self.blockLookupLock:
                peers = list(self.blockLookup[b].peers)
            res["blocks"].append({"id": b, "peers": peers})
        self.log(json.dumps(res, indent=2))
        self.sendResponse(client, res)

    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("", self.config["port"]))
        self.socket.listen(self.config["listen"])

        try:
            while True:
                client, address = self.socket.accept()
                thread = threading.Thread(target=self.processRequest,
                        args=(client, address))
                thread.start()
        except KeyboardInterrupt:
            # Wait for all the threads to finish so I can write the config file.
            while threading.activeCount() > 1:
                time.sleep(1)
            out = open(self.configFile, "w")
            out.write(json.dumps(self.config, indent=4))
            out.close()
            exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "You need to supply the config file."
        exit(1)
    server = NeguraServer(sys.argv[1])
    server.start()
