package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxSuccessors = 3
	m             = 32 // size of key space
	maxSteps      = 32 // maximum number of steps in a lookup
	base          = 2  // base of the finger table
	keySize       = sha1.Size * 8
)

type Key string
type NodeAddress string
type Nothing struct{}
type FindSuccessorReturn struct {
	Bool      bool
	Successor NodeAddress
}
type NodeData struct {
	Successors  []NodeAddress
	Predecessor NodeAddress
}

var two = big.NewInt(2)
var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(keySize), nil)

type Node struct {
	Address     NodeAddress
	FingerTable []NodeAddress
	Predecessor NodeAddress
	Successors  []NodeAddress

	Bucket map[Key]string
	mu     sync.RWMutex
}

func Call(address, method string, request, response interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Printf("rpc.DialHTTP: %v", err)
		return err
	}
	defer client.Close()

	if err = client.Call(method, request, response); err != nil {
		log.Printf("client.Call: %v", err)
		return err
	}

	return nil
}

func help() {
	fmt.Print("help) shows commands\n" +
		"quit) quits program\n" +
		"port <value>) changes port\n" +
		"put <key> <value>) adds key value pair\n" +
		"create) creates new ring\n" +
		"get <key>) gets value from key\n" +
		"delete <key>) deletes key value pair\n" +
		"join <address>) joins a circle where one of the \n")
}

func create(address, port string, node *Node) {
	rpc.Register(node)
	rpc.HandleHTTP()
	node.Bucket = make(map[Key]string)
	node.Address = NodeAddress(address + port)
	log.Print("start server: creating new ring")
	l, err := net.Listen("tcp", port)
	if err != nil {
		log.Print("Listen Error: ", err)
	}
	log.Printf("Starting to listen on %s", address+port)
	for {
		if err := http.Serve(l, nil); err != nil {
			log.Print("HTTP Serve Error: ", err)
		}
	}
}

func (n *Node) Ping(_ *Nothing, reply *string) error {
	log.Print("Pinged")
	*reply = "Pong"
	return nil
}

func (n *Node) Put(args []string, _ *Nothing) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Bucket[Key(args[0])] = args[1]
	log.Printf("added key pair %s %s to this bucket", Key(args[0]), args[1])
	return nil
}
func (n *Node) Delete(key string, _ *Nothing) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.Bucket[Key(key)]; !ok {
		log.Printf("Delete: key %s not found", key)
	}
	deleted := n.Bucket[Key(key)]
	delete(n.Bucket, Key(key))
	log.Printf("Delete: [%s] was removed", deleted)
	return nil
}

type KeyBucket struct {
	Key  string
	Value string
}
func (n *Node) Get(key string, value *KeyBucket) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.Bucket[Key(key)]; !ok {
		log.Printf("Get: key %s not found", key)
	}
	value.Key = key
	value.Value = n.Bucket[Key(key)]
	return nil
}
func (n *Node) dump() {
	v := n.Address
	hex := fmt.Sprintf("%040x", hash(string(v)))
	s := hex[:8] + ".. (" + string(v) + ")"
	log.Printf("Self: %s\n", s)
	log.Print("")
	if len(n.Bucket) != 0 {
		log.Print("Listing all key value pairs")
		for k, v := range n.Bucket {
			hex := fmt.Sprintf("%040x", hash(string(k)))
			s := hex[:8] + ".. (" + string(k) + ")"
			log.Printf("Key: %s, Value %s\n", s, v)
		}
		log.Print("")
	}
	log.Print("Listing successors")
	for _, v := range n.Successors {
		hex := fmt.Sprintf("%040x", hash(string(v)))
		s := hex[:8] + ".. (" + string(v) + ")"
		log.Printf("%s\n", s)
	}
	log.Print("")
	v = n.Predecessor
	hex = fmt.Sprintf("%040x", hash(string(v)))
	s = hex[:8] + ".. (" + string(v) + ")"
	log.Printf("Predecessor: %s\n", s)
	log.Print("")
	log.Print("Finger table")
	same_value := n.FingerTable[0]
	for inc, _ := range n.FingerTable {
		if same_value != n.FingerTable[inc] {
			hex := fmt.Sprintf("%040x", hash(string(n.FingerTable[inc-1])))
			s := hex[:8] + ".. (" + string(n.FingerTable[inc-1]) + ")"
			log.Printf("[%d] %s\n",inc-1,s)
			same_value = n.FingerTable[inc]
		}
	}
	hex = fmt.Sprintf("%040x", hash(string(n.FingerTable[160])))
	s = hex[:8] + ".. (" + string(n.FingerTable[160]) + ")"
	log.Printf("[%d] %s\n",160,s)
}
func (n *Node) Join(address string, _ *Nothing) error {
	n.Predecessor = NodeAddress(address)
	return nil
}

func (n *Node) Find_successor(id string, response *FindSuccessorReturn) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if between(hash(string(id)), hash(string(n.Address)), hash(string(n.Successors[0])), true) {
		response.Successor = n.Successors[0]
		response.Bool = true
	} else {
		response.Bool = false
		response.Successor = n.closest_preceding_node(id)
	}

	return nil
}
func (n *Node) closest_preceding_node(id string) NodeAddress {
	// skip this loop if you do not have finger tables implemented yet
	for i := range n.FingerTable {
		isBetween := between(hash(string(n.Address)), hash(string(n.FingerTable[160-i])), hash(id), true)
		if isBetween == true {
			return n.FingerTable[160-i]
		}
	}
	return n.Successors[0]
	// find the successor of id
}
func find(id string, start NodeAddress) NodeAddress {
	found, nextNode := false, start
	nextNode_struct := FindSuccessorReturn{}
	for i := 0; !found && i < maxSteps; i++ {
		if err := Call(string(nextNode), "Node.Find_successor", &id, &nextNode_struct); err == nil {
			found = nextNode_struct.Bool
			if !found {
				nextNode = nextNode_struct.Successor
			}
		}
	}
	if found {
		return nextNode
	} else {
		log.Print("error there was no next node")
	}
	return ""
}
func (n *Node) Put_all(kv map[string]string, reply *bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for k, v := range kv {
		n.Bucket[Key(k)] = v
	}

	*reply = true
	return nil
}

func (n *Node) fixFingers() {
	n.mu.Lock()
	defer n.mu.Unlock()
	i := 0
	betweenB := false
	currentSucc := n.Successors[0]
	data := new(NodeData)
	for betweenB == false {
		if currentSucc == n.Address {
			for i = 0; i <= 160;i++ {
				n.FingerTable[i] = n.Address
			}
			return
		}
		betweenB = between(hash(string(n.Address)), jump(string(n.Address), i), hash(string(currentSucc)), true)
		if betweenB {
			n.FingerTable[i] = currentSucc
		} else {
			if err := Call(string(currentSucc), "Node.GetNodeData", &Nothing{}, &data); err != nil {
				log.Printf("Error getting Node Data: %v", err)
				return
			}
			currentSucc = data.Successors[0]
		}
	}
	betweenB = false

	// loop and keep adding to successive entries as long as it is still in the right range
	for i = 1; i <= 160; i++ {
		for betweenB == false {
			if currentSucc == n.Address {
				for i = i; i <= 160;i++ {
					n.FingerTable[i] = n.Address
				}
				return
			}
			betweenB = between(hash(string(n.Address)), jump(string(n.Address), i), hash(string(currentSucc)), true)
			if betweenB {
				n.FingerTable[i] = currentSucc
			} else {
				if err := Call(string(currentSucc), "Node.GetNodeData", &Nothing{}, &data); err != nil {
					log.Printf("Error getting Node Data: %v", err)
				}
				currentSucc = data.Successors[0]
			}
		}
		betweenB = false
	}
}

func (n *Node) Get_all(addr string, reply *map[string]string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	keysToRemove := make(map[string]string)
	for k, v := range n.Bucket {
		if between(hash(string(n.Predecessor)), hash(string(addr)), hash(string(k)), false) {
			keysToRemove[string(k)] = v
		}
	}

	for k := range keysToRemove {
		delete(n.Bucket, Key(k))
	}

	*reply = keysToRemove
	return nil
}
func (n *Node) GetNodeData(_ *Nothing, data *NodeData) error {
	data.Predecessor = n.Predecessor
	data.Successors = n.Successors
	return nil
}
func (n *Node) Nothing(_ *Nothing, _ *Nothing) error {
	return nil
}
func (n *Node) checkPredecessor() {
	if err := Call(string(n.Predecessor), "Node.Nothing", &Nothing{}, &Nothing{}); err != nil {
		n.Predecessor = ""
		log.Print("Predecessor has been removed")
	}
}
func (n *Node) stabilize() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	var succPredecessor NodeAddress
	data := new(NodeData)
	if len(n.Successors) == 0 {
		n.Successors = append(n.Successors, n.Address)
		return nil
	}
	if err := Call(string(n.Successors[0]), "Node.GetNodeData", &Nothing{}, &data); err != nil {
		if len(n.Successors) == 1 {
			n.Successors[0] = n.Address
		} else {
			n.Successors = n.Successors[1:]
		}
		log.Print("Removed Successor")
		log.Printf("error while getting predecessor: %v", err)
		return err
	}
	succPredecessor = data.Predecessor
	var successors []NodeAddress
	successors = data.Successors
	successors = append([]NodeAddress{n.Successors[0]}, successors...)
	if len(successors) > maxSuccessors {
		successors = successors[:maxSuccessors]
	}
	same := true
	if len(n.Successors) != len(successors) {
		same = false
	}
	if same == true {
		for i, v := range n.Successors {
			if v != successors[i] {
				same = false
			}
		}
	}
	if same == false{
		n.Successors = successors
		log.Print("Successor list changed")
	}
	if succPredecessor != "" {
		if between(hash(string(n.Address)), hash(string(succPredecessor)), hash(string(n.Successors[0])), true) {
			if err := Call(string(succPredecessor), "Node.GetNodeData", &Nothing{}, &data); err != nil {
				if len(n.Successors) == 1 {
					n.Successors[0] = n.Address
				} else {
					n.Successors = n.Successors[1:]
				}
				log.Print("Removed Successor")
				log.Printf("error while getting successors: %v", err)
				return err
			}
			successors = data.Successors
			successors = append([]NodeAddress{succPredecessor}, successors...)
			if len(successors) > maxSuccessors {
				successors = successors[:maxSuccessors]
			}
			same = true
			if len(n.Successors) != len(successors) {
				same = false
			}
			if same == true {
				for i, v := range n.Successors {
					if v != successors[i] {
						same = false
					}
				}
			}
			if same == false {
				log.Print("Successors list changed")
				n.Successors = successors
			}
		}
	}
	if err := Call(string(n.Successors[0]), "Node.Notify", n.Address, &Nothing{}); err != nil {
		if len(n.Successors) == 1 {
			n.Successors[0] = n.Address
		} else {
			n.Successors = n.Successors[1:]
		}
		log.Print("removed Successor")
		log.Printf("error while notifying: %v", err)
		return err
	}
	return nil
}
func (n *Node) Notify(address NodeAddress, _ *Nothing) error {
	if n.Predecessor == "" || between(hash(string(n.Predecessor)), hash(string(address)), hash(string(n.Address)), false) {
		if n.Predecessor != address {
			log.Print("Predecessor changed")
			n.Predecessor = address
		}
	}
	return nil
}
func (n *Node) stabilizeAndFix() {
	for {
		n.stabilize()
		time.Sleep(time.Millisecond * 250)
		n.fixFingers()
		time.Sleep(time.Millisecond * 250)
		n.checkPredecessor()
		time.Sleep(time.Millisecond * 250)
	}
}
func main() {
	quit := false
	listening := false
	// read inputs
	port := "3410"
	address := getLocalAddress()
	node := new(Node)
	for quit == false {
		fmt.Print("> ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		err := scanner.Err()
		if err != nil {
			log.Print(err)
		}
		s := strings.Split(scanner.Text(), " ")
		switch s[0] {
		case "help":
			help()
		case "quit":
			quit = true
			var reply bool
			if len(node.Successors[0]) != 0 {
				if err := Call(string(node.Successors[0]), "Node.Put_all", node.Bucket, &reply); err != nil {
					log.Printf("error calling Ping: %v", err)
				}
			}
		case "ping":
			var reply string
			if err := Call("localhost:"+port, "Node.Ping", &Nothing{}, &reply); err != nil {
				log.Printf("error calling Ping: %v", err)
			} else {
				log.Printf("received reply from Ping: %s", reply)
			}
		case "create":
			if listening == false {
				listening = true
				go create(address, ":"+port, node)
				time.Sleep(time.Millisecond * 100)
				node.Predecessor = NodeAddress(address + ":" + port)
				node.Successors = append(node.Successors, NodeAddress(address+":"+port))
				for i := 0; i <= 160; i++ {
					node.FingerTable = append(node.FingerTable, node.Address)
				}
				go node.stabilizeAndFix()
			} else {
				log.Print("Already created or joined a node.")
			}
		case "join":
			if listening == false {
				found := find(string(node.Address), NodeAddress(s[1]))
				if err := Call(string(found), "Node.Join", address+":"+port, &Nothing{}); err == nil {
					node.Successors = append(node.Successors, NodeAddress(s[1]))
					go create(address, ":"+port, node)
					time.Sleep(time.Millisecond * 100)
					for i := 0; i <= 160; i++ {
						node.FingerTable = append(node.FingerTable, node.Address)
					}
					go node.stabilizeAndFix()
					listening = true
					log.Printf("Successfully join circle at %s:%s", address, port)
					if len(node.Successors) > 0 {
						success := node.Successors[0]
						var reply map[string]string
						if err := Call(string(found), "Node.Get_all", string(success), &reply); err != nil {
							log.Printf("error calling get all: %v", err)
						}
						for key, value := range reply {
							node.Bucket[Key(key)] = value
						}
						if err != nil {
							log.Print("Error with getting the values in the successor")
						}
					} else {
						log.Print("there are no successors")
					}
				} else {
					log.Print("Error joining circle")
				}
			} else {
				log.Print("Already joined or created circle.")
			}
		case "port":
			if listening == false {
				port = s[1]
			} else {
				help()
			}
			log.Printf("Changed port to %s", port)
		case "get":
			if listening == true {
				found := find(s[1], node.Address)
				keyValues := KeyBucket{}
				if err = Call(string(found), "Node.Get", s[1], &keyValues); err != nil {
					log.Printf("error calling Get: %v", err)
				}
				log.Printf("Get: found %s => %s", keyValues.Key, keyValues.Value)
			} else {
				log.Print("Not in a circle")
			}
		case "put":
			if listening == true && len(s) == 3 {
				found := find(s[1], node.Address)
				if err = Call(string(found), "Node.Put", []string{s[1], s[2]}, &Nothing{}); err != nil {
					log.Printf("error calling Put: %v", err)
				} else {
					log.Printf("Put key pair %s %s into bucket", s[1], s[2])
				}
			} else {
				log.Print("Not in a circle")
			}
		case "putrandom":
			if listening == true && len(s) == 2 {
				num_random, err := strconv.Atoi(s[1])
				if err != nil {
					help()
				}
				const letterBytes = "abcdefghijklmnopqrstuvwxyz"
				for i := 0; i < num_random; i++ {
					result := make([]byte, 5)
					for i := range result {
						result[i] = letterBytes[rand.Intn(len(letterBytes))]
					}
					value := "random(" + string(result) + ")"
					found := find(string(result), node.Address)
					if err = Call(string(found), "Node.Put", []string{string(result), value}, &Nothing{}); err != nil {
						log.Printf("error calling Put: %v", err)
					} else {
						log.Printf("Put key pair %s %s into bucket", string(result), value)
					}
				}
			} else {
				log.Print("Not in a circle")
			}
		case "delete":
			if listening == true {
				found := find(s[1], node.Address)
				if err = Call(string(found), "Node.Delete", s[1], &Nothing{}); err != nil {
					log.Printf("error calling Delete: %v", err)
				}
			} else {
				log.Print("Not in a circle")
			}
		case "dump":
			if listening == true {
				node.dump()
			} else {
				log.Print("Have not created or joined a circle yet")
			}
		case "":
		default:
			fmt.Print("Unrecognized command\n")
			help()
		}
	}
}
func hash(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}
func jump(address string, fingerentry int) *big.Int {
	n := hash(address)
	fingerentryminus1 := big.NewInt(int64(fingerentry) - 1)
	jump := new(big.Int).Exp(two, fingerentryminus1, nil)
	sum := new(big.Int).Add(n, jump)

	return new(big.Int).Mod(sum, hashMod)
}
func between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
}
func getLocalAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}
