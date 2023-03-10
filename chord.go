package main

import (
	"crypto/sha1"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
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

type Node struct {
	Address     NodeAddress
	FingerTable []NodeAddress
	Predecessor NodeAddress
	Successors  []NodeAddress

	Bucket map[Key]string
}


func help() {
	fmt.Print("
	help) shows commands\n
	quit) quits program\n
	")
}

func main() {
	quit := false
	// read inputs
	port := "3410"
	for quit == false {
		fmt.Print("> ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		err := scanner.Err()
		if err != nil {
			log.Fatal(err)
		}
		s := strings.Split(scanner.Text(), " ")
		switch s[0] {
		case "help":
			help()
		case "quit":
			quit = true
		case "ping":
		case "port":
			port = s[1]
		case "get":
		case "put":
		case "delete":
		case "dump":
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

func call(address string, method string, request interface{}, reply interface{}) error {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.Call(method, request, reply)
	if err != nil {
		return err
	}

	return nil
}

// func (n *Node) findSuccessor(id int) (*Node, error) {
// 	if id <= n.id || id > n.successor.id {
// 		closest, _ := n.closestPrecedingNode(id)
// 		return closest.findSuccessor(id)
// 	}
// 	return n.successor, nil
// }

// func (n *Node) closestPrecedingNode(id int) (*Node, error) {
// 	for i := m - 1; i >= 0; i-- {
// 		if n.fingers[i] != nil && n.fingers[i].id > n.id && n.fingers[i].id < id {
// 			return n.fingers[i], nil
// 		}
// 	}
// 	return n, nil
// }

// func (n *Node) find(id int, start *Node) (*Node, error) {
// 	found, nextNode := false, start
// 	i := 0
// 	for !found && i < maxSteps {
// 		var err error
// 		nextNode, err = nextNode.findSuccessor(id)
// 		if err != nil {
// 			return nil, err
// 		}
// 		found = n.id == nextNode.id || (id > n.id && id <= nextNode.id) || (n.id > nextNode.id && (id > n.id || id <= nextNode.id))
// 		i++
// 	}
// 	if found {
// 		return nextNode, nil
// 	}
// 	return nil, fmt.Errorf("failed to find successor for key %d", id)
// }
func create(port int) *Node {
	node := new(Node)
	node.Values = make(map[string]string)

	// Start RPC server
	rpc.Register(node)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatal("listen error: ", err)
	}

	go http.Serve(listener, nil)

	return node
}
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

func (n *Node) Get(key string, value *string) error {
	*value = n.Values[key]
	return nil
}
func get(address, key string) (string, error) {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return "", err
	}

	var value string
	err = client.Call("Node.Get", key, &value)
	if err != nil {
		return "", err
	}

	return value, nil
}
func (n *Node) Put(kv [2]string, success *bool) error {
	n.Values[kv[0]] = kv[1]
	*success = true
	return nil
}
func put(address, key, value string) (bool, error) {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return false, err
	}

	var success bool
	err = client.Call("Node.Put", [2]string{key, value}, &success)
	if err != nil {
		return false, err
	}

	return success, nil
}

func (n *Node) Delete(key string, success *bool) error {
	delete(n.Values, key)
	*success = true
	return nil
}
func delete(address, key string) (bool, error) {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return false, err
	}

	var success bool
	err = client.Call("Node.Delete", key, &success)
	if err != nil {
		return false, err
	}
	return success, nil
}
func (dht *DHT) putRandom(n int) {
	panic("imp")
}

func (dht *DHT) dump() {
	panic("imp")
}

func (n *Node) create() error {
	panic("imp")
}

func (n *Node) stabilize() error {
	panic("imp")
}

func (n *Node) notify(node NodeAddr) error {
	panic("imp")
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

func main() {
	// : initialize DHT, parse command-line arguments, and start REPL
	panic("imp")

}
