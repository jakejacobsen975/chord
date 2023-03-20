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
		"port <value>: changes port\n")
}

func server(address, port string, node *Node) {
	rpc.Register(node)
	rpc.HandleHTTP()
	node.Bucket = make(map[Key]string)

	l, err := net.Listen("tcp", port)
	if err != nil {
		log.Print("Listen Error: ", err)
	}
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
	n.Bucket[Key(args[0])] = args[1]
	return nil
}

func main() {
	quit := false
	listening := false
	// read inputs
	port := "3410"
	address := getLocalAddress()
	node := new(Node)
	arg = args[0]
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
				log.Print("start server: creating new ring")
				go server(address, ":"+port, node)
			} else {
				log.Print("Already created or joined a node.")
			}
		case "join":
		case "port":
			port = s[1]
		case "get":
		case "put":
			if listening == true {
				if err = Call(s[3], "Node.Put", []string{s[1], s[2]}, &Nothing{}); err != nil {
					log.Printf("error calling Put: %v", err)
				} else {
					log.Printf("Put key pair %s %s into bucket", s[1], s[2])
				}
			} else {
				log.Print("Not in a circle")
			}
		case "delete":
		case "dump":
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

// func call(address string, method string, request interface{}, reply interface{}) error {
// 	client, err := rpc.Dial("tcp", address)
// 	if err != nil {
// 		return err
// 	}
// 	defer client.Close()

// 	err = client.Call(method, request, reply)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

func ping(address string) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return err
	}

	var pong string
	err = client.Call("Node.Ping", "", &pong)
	if err != nil {
		return err
	}

	fmt.Println(pong)
	return nil
}
func (n *Node) Delete(key string, success *bool) error {
	// delete(n.Bucket, key)
	*success = true
	return nil
}
func delete(address, key string) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return err
	}

	var success bool
	err = client.Call("Node.Delete", key, &success)
	if err != nil {
		return err
	}
	return nil
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
