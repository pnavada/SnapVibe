package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	token      = 0
	marker     = 1
	serverPort = 8080
)

var (
	debugLogger             = log.New(os.Stdout, "SnapVibe", log.Default().Flags())
	errorLogger             = log.New(os.Stderr, "SnapVibe", log.Default().Flags())
	wg                      sync.WaitGroup
	readChannel             = make(chan Message)
	writeChannel            = make(chan Message)
	readConnMap             = make(map[net.Addr]net.Conn)
	writeConnMap            = make(map[net.Addr]net.Conn)
	peerHostnameToAddr      = make(map[string]net.Addr)
	peerReadAddrToHostname  = make(map[net.Addr]string)
	peerWriteAddrToHostname = make(map[net.Addr]string)
	state                   int
	procIds                 = make(map[string]int)
	snapshots               = make(map[int]*Snapshot)
	stateLock               sync.Mutex
	mapLock                 sync.Mutex
	hasToken                *bool
	hasTokenLock            sync.Mutex
)

type Channel struct {
	queue []int
	close bool
	lock  sync.Mutex
}

type Snapshot struct {
	state    int
	channels map[int]*Channel
}

type Message struct {
	Addr net.Addr
	Data []byte
}

func passToken(succ string) {
	data := make([]byte, 12)
	binary.LittleEndian.PutUint32(data[0:4], uint32(token))
	writeChannel <- Message{Addr: peerHostnameToAddr[succ], Data: data}
}

func passTokenWithDelay(delay float64, succ string, host string) {
	stateLock.Lock()
	state += 1
	errorLogger.Printf("{proc_id: %d, state: %d}\n", procIds[host], state)
	stateLock.Unlock()
	time.Sleep(time.Duration(int64(delay * 1e9)))
	passToken(succ)
}

func getPeers(fileName string) ([]string, error) {

	var peers []string
	file, err := os.Open(fileName)

	if err != nil {
		return nil, err
	}

	// Close the file before returning the list of peers
	defer file.Close()

	scanner := bufio.NewScanner(file)

	procId := 1
	for scanner.Scan() {
		peer := scanner.Text()
		procIds[peer] = procId
		peers = append(peers, peer)
		procId += 1
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return peers, nil

}

func findIndex(slice []string, target string) int {
	for i, v := range slice {
		if v == target {
			return i // Return the index if the element is found
		}
	}
	return -1 // Return -1 if the element is not found
}

func getPredecessor(peers []string, host string) string {

	hostIndex := findIndex(peers, host)
	predIndex := hostIndex - 1
	if predIndex < 0 {
		predIndex = len(peers) - 1
	}
	return peers[predIndex]

}

func getSuccessor(peers []string, host string) string {

	hostIndex := findIndex(peers, host)
	succIndex := hostIndex + 1
	if succIndex == len(peers) {
		succIndex = 0
	}
	return peers[succIndex]

}

func listenToPeers() {

	defer wg.Done()

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(serverPort))
	if err != nil {
		errorLogger.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	debugLogger.Printf("Server is listening on port: %d\n", serverPort)

	for {
		// Accept a connection
		conn, err := listener.Accept()
		if err != nil {
			errorLogger.Println("Error accepting connection:", err)
			continue
		}
		// Handle the connection in a separate goroutine
		go handleConnection(conn)
	}

}

func tidyHostName(hostname string) string {
	return strings.Split(hostname, ".")[0]
}

func getHostnameFromIP(addr net.Addr) (string, error) {

	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "", err
	}
	hostnames, err := net.LookupAddr(host)
	if err != nil {
		return "", err
	}
	if len(hostnames) == 0 {
		return "", fmt.Errorf("could not find any host names for the IP")
	}
	host = tidyHostName(hostnames[0])
	return host, nil

}

func handleConnection(conn net.Conn) {

	defer conn.Close()
	debugLogger.Println("Peer connected: ", conn.RemoteAddr())
	mapLock.Lock()
	readConnMap[conn.RemoteAddr()] = conn
	peer, err := getHostnameFromIP(conn.RemoteAddr())

	if err != nil {
		errorLogger.Println("Unable to resolve hostname from IP", err)
	}

	peerReadAddrToHostname[conn.RemoteAddr()] = peer
	mapLock.Unlock()

	// Read messages from the peer
	buffer := make([]byte, 100)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			errorLogger.Println("Error reading from peer:", err)
			return
		}
		if n == 12 { // Expecting 12 bytes
			message := buffer[:n]
			readChannel <- Message{Addr: conn.RemoteAddr(), Data: message}
		}
	}

}

func dialPeers(peers []string) {

	defer wg.Done()

	for _, peer := range peers {

		conn, err := net.Dial("tcp", net.JoinHostPort(peer, strconv.Itoa(serverPort)))
		if err != nil {
			errorLogger.Println("Error connecting to peer:", err)
			continue
		}
		writeConnMap[conn.RemoteAddr()] = conn
		peerHostnameToAddr[peer] = conn.RemoteAddr()
		peerWriteAddrToHostname[conn.RemoteAddr()] = peer
		debugLogger.Println("Successfully connected to peer: ", peer)

	}

}

func sendMarker(snapshotId int, initiator int, me int) {

	for peer, id := range procIds {
		if id != me {
			data := make([]byte, 12)
			binary.LittleEndian.PutUint32(data[0:4], uint32(marker))
			binary.LittleEndian.PutUint32(data[4:8], uint32(snapshotId))
			binary.LittleEndian.PutUint32(data[8:12], uint32(initiator))
			writeChannel <- Message{Addr: peerHostnameToAddr[peer], Data: data}
		}
	}
}

func sendMarkerWithDelay(markerDelay float64, snapshotId int, initiator int, me int) {
	time.Sleep(time.Duration(int64(markerDelay * 1e9)))
	go sendMarker(snapshotId, initiator, me)
}

func addToQueue(sender int, message int) {
	for _, snap := range snapshots {
		snap.channels[sender].lock.Lock()
		if !snap.channels[sender].close {
			snap.channels[sender].queue = append(snap.channels[sender].queue, message)
		}
		snap.channels[sender].lock.Unlock()
	}
}

func closeChannel(snapshotId int, sender int, me int) {
	snap := snapshots[snapshotId]
	channel := snap.channels[sender]
	channel.lock.Lock()
	if !channel.close {
		channel.close = true
		errorLogger.Printf(
			"{proc_id:%d, snapshot_id: %d, snapshot:\"channel closed\", channel:%d-%d, queue:%v}\n",
			me,
			snapshotId,
			sender,
			me,
			channel.queue,
		)
	}
	channel.lock.Unlock()
}

func checkIfSnapshotFinished(snapshotId int, me int) {
	for {
		snap := snapshots[snapshotId]
		channels := snap.channels
		count := 0
		for _, channel := range channels {
			channel.lock.Lock()
			if channel.close {
				count += 1
			}
			channel.lock.Unlock()
		}
		if count == len(snap.channels) {
			// Snapshot finished
			errorLogger.Printf(
				"{proc_id:%d, snapshot_id: %d, snapshot:\"complete\"}\n",
				me,
				snapshotId,
			)
			return
		}
	}
}

func removeByValue(slice []string, value string) []string {
	indexToRemove := -1
	// Find the index of the value to remove
	for i, num := range slice {
		if num == value {
			indexToRemove = i
			break
		}
	}

	// If the value was found, remove it
	if indexToRemove != -1 {
		return removeByIndex(slice, indexToRemove)
	}

	// Return the original slice if the value was not found
	return slice
}

func removeByIndex(slice []string, index int) []string {
	if index < 0 || index >= len(slice) {
		return slice // Return original slice if index is out of bounds
	}
	return append(slice[:index], slice[index+1:]...)
}

func initSnapshot(markerDelay float64, sender int, snapshotId int, initiator int, me int) {
	errorLogger.Printf("{proc_id:%d, snapshot_id: %d, snapshot:\"started\"}\n", me, snapshotId)
	stateLock.Lock()
	var snap = Snapshot{state: state}
	stateLock.Unlock()
	snapshots[snapshotId] = &snap
	snap.channels = make(map[int]*Channel)
	for _, id := range procIds {
		if id != me {
			snap.channels[id] = &Channel{queue: make([]int, 0)}
		}
	}
	if sender != -1 {
		closeChannel(snapshotId, sender, me)
	}
	go sendMarkerWithDelay(markerDelay, snapshotId, initiator, me)
	go checkIfSnapshotFinished(snapshotId, me)
}

func initSnapshotAfterState(markerDelay float64, sender int, snapshotDelay int, snapshotId int, initiator int, me int) {
	for {
		stateLock.Lock()
		if state == snapshotDelay {
			stateLock.Unlock()
			go initSnapshot(markerDelay, sender, snapshotId, initiator, me)
			return
		}
		stateLock.Unlock()
	}
}

func main() {

	hostsfile := flag.String("h", "", "Name of the file containing the list of peers")
	tokenDelay := flag.Float64("t", float64(0), "Delay in seconds before sending the token")
	markerDelay := flag.Float64("m", float64(-1), "Delay in seconds before sending the marker")
	snapshotDelay := flag.Int("s", -1, "Delay in seconds before initiating the snapshot")
	snapshotId := flag.Int("p", -1, "ID of the snapshot")
	hasToken = flag.Bool("x", false, "Do I have the token?")

	flag.Parse()

	debugLogger.Printf("Hostsfile name: %s\n", *hostsfile)
	debugLogger.Printf("Token delay: %f\n", *tokenDelay)
	debugLogger.Println("Do I have the token? ", *hasToken)

	// Get the list of peers
	peers, err := getPeers(*hostsfile)

	if err != nil {
		errorLogger.Println("Error reading peers:", err)
		return
	}

	host, err := os.Hostname()

	if err != nil {
		errorLogger.Println("Failed to get local hostname:", err)
	}

	debugLogger.Println("Peers:", peers)

	// Find the predecessor and successor
	pred := getPredecessor(peers, host)
	succ := getSuccessor(peers, host)

	peers = removeByValue(peers, host)

	debugLogger.Println("Predecessor:", pred)
	debugLogger.Println("Successor:", succ)

	if *hasToken {
		state = 1
	}

	errorLogger.Printf(
		"{proc_id: %d, state: %d, predecessor: %d, successor: %d}\n",
		procIds[host],
		state,
		procIds[pred],
		procIds[succ],
	)

	// Start listening for peers
	wg.Add(1)
	go listenToPeers()

	time.Sleep(time.Second * 5)

	wg.Add(1)
	go dialPeers(peers)

	time.Sleep(time.Second)

	if *hasToken {
		go passTokenWithDelay(*tokenDelay, succ, host)
	}

	if *snapshotId != -1 {
		go initSnapshotAfterState(*markerDelay, -1, *snapshotDelay, *snapshotId, procIds[host], procIds[host])
	}

	for {
		select {
		case Msg := <-readChannel:
			MsgType := int(binary.LittleEndian.Uint32(Msg.Data[0:4]))
			switch MsgType {
			case token: // Token received
				errorLogger.Printf(
					"{proc_id: %d, sender: %d, receiver: %d, message:\"token\"}\n",
					procIds[host],
					procIds[peerReadAddrToHostname[Msg.Addr]],
					procIds[host],
				)
				hasTokenLock.Lock()
				*hasToken = true
				hasTokenLock.Unlock()
				go passTokenWithDelay(*tokenDelay, succ, host)
				// Add the message to the queue
				go addToQueue(procIds[peerReadAddrToHostname[Msg.Addr]], token)
			case marker: // Marker Received
				SnapshotId := int(binary.LittleEndian.Uint32(Msg.Data[4:8]))
				Initiator := int(binary.LittleEndian.Uint32(Msg.Data[8:12]))
				_, ok := snapshots[SnapshotId]
				if !ok {
					sender := procIds[peerReadAddrToHostname[Msg.Addr]]
					go initSnapshot(*markerDelay, sender, SnapshotId, Initiator, procIds[host])
				} else {
					// Close the communication on the channel
					go closeChannel(SnapshotId, procIds[peerReadAddrToHostname[Msg.Addr]], procIds[host])
				}
			}
		case Msg := <-writeChannel:
			conn := writeConnMap[Msg.Addr]
			_, err := conn.Write([]byte(Msg.Data))
			if err != nil {
				fmt.Println("Error sending message:", err)
			}
			MsgType := int(binary.LittleEndian.Uint32(Msg.Data[0:4]))
			switch MsgType {
			case token:
				hasTokenLock.Lock()
				*hasToken = false
				hasTokenLock.Unlock()
				errorLogger.Printf(
					"{proc_id: %d, sender: %d, receiver: %d, message:\"token\"}\n",
					procIds[host],
					procIds[host],
					procIds[peerWriteAddrToHostname[Msg.Addr]],
				)
			case marker:
				SnapshotId := int(binary.LittleEndian.Uint32(Msg.Data[4:8]))
				stateLock.Lock()
				hasTokenLock.Lock()
				var tokenStatus string
				if *hasToken {
					tokenStatus = "YES"
				} else {
					tokenStatus = "NO"
				}
				errorLogger.Printf(
					"{proc_id:%d, snapshot_id: %d, sender:%d, receiver:%d, msg:\"marker\", state:%d, has_token:%s}",
					procIds[host],
					SnapshotId,
					procIds[host],
					procIds[peerWriteAddrToHostname[Msg.Addr]],
					state,
					tokenStatus,
				)
				hasTokenLock.Unlock()
				stateLock.Unlock()
			}
		}
	}

	// wg.Wait()

}
