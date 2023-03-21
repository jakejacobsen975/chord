package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

const (
	numSuccessors = 3
	m             = 32 // size of key space
	maxSteps      = 32 // maximum number of steps in a lookup
	base          = 2  // base of the finger table
	keySize       = sha1.Size * 8
)

type Key string
type NodeAddress string
type Nothing struct{}

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
		"put <key> <value> <address>) adds key value pair\n" +
		"create) creates new ring\n")
}

func create(address, port string, node *Node) {
	rpc.Register(node)
	rpc.HandleHTTP()
	node.Bucket = make(map[Key]string)

	log.Print("start server: creating new ring")
	l, err := net.Listen("tcp", port)
	if err != nil {
		log.Print("Listen Error: ", err)
	}
	log.Printf("Starting to listen on %s", address+port)
	fmt.Print("> ")
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
func (n *Node) Get(key string, _ *Nothing) error {
	if _, ok := n.Bucket[Key(key)]; !ok {
		log.Printf("Get: key %s not found", key)
	}
	log.Printf("Get: found %s => %s", key, n.Bucket[Key(key)])
	return nil
}
func (n *Node) dump() {
	if len(n.Bucket) != 0 {
		log.Print("Listing all key value pairs")
		for k, v := range n.Bucket {
			log.Printf("Key: %s, Value %s\n", k, v)
		}
	}
	log.Print("Listing successors")
	for _, v := range n.Successors {
		log.Printf("%s\n", v)
	}
	log.Printf("Predecessor: %s\n", n.Predecessor)
}
func (n* Node) Join(address string, _ *Nothing) error {
	n.Predecessor = NodeAddress(address)
	return nil
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
			} else {
				log.Print("Already created or joined a node.")
			}
		case "join":
			if listening == false {
				if err = Call(s[1], "Node.Join", address + port, &Nothing{}); err != nil {
					node.Successors = append(node.Successors, NodeAddress(s[1]))
					listening = true
				} else {
					log.Print("Error joining circle")
				}
			} else {
				log.Print("Already joined or created circle.")
			}
		case "port":
			copy := port
			port = s[1]
			if copy == port {
				log.Printf("port: set to %s", port)
			} else {
				log.Printf("port: insert new value")
			}
		case "get":
			if listening == true {
				if err = Call(s[2], "Node.Get", s[1], &Nothing{}); err != nil {
					log.Printf("error calling Get: %v", err)
				}
			} else {
				log.Print("Not in a circle")
			}
		case "put":
			if listening == true && len(s) == 4 {
				if err = Call(s[3], "Node.Put", []string{s[1], s[2]}, &Nothing{}); err != nil {
					log.Printf("error calling Put: %v", err)
				} else {
					log.Printf("Put key pair %s %s into bucket", s[1], s[2])
				}
			} else {
				log.Print("Not in a circle")
			}
		case "delete":
			if listening == true {
				if err = Call(s[2], "Node.Delete", s[1], &Nothing{}); err != nil {
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

func (n *Node) fixFingers() error {
	panic("imp")
}

func (n *Node) checkPredecessor() error {
	panic("imp")
}

func (n *Node) start() error {
	panic("imp")
}

func (n *Node) stop() error {
	panic("imp")
}

func (n *Node) listenAndServe() error {
	panic("imp")
}

/*func main() {
	// : initialize DHT, parse command-line arguments, and start REPL
	panic("imp")

}*/
