package main

import (
	"bufio"
	"encoding/json"
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
	marker     = "marker"
	token      = "token"
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
	lock                    sync.Mutex
)

type Channel struct {
	queue []string
	close bool
	lock  sync.Mutex
}

type Snapshot struct {
	state    int
	channels map[int]*Channel
}

type Message struct {
	Addr net.Addr
	Data string
}

type Packet struct {
	MsgType string
	Msg     string
}

type Marker struct {
	SnapshotId int
	Initiator  int
}

func passToken(succ string) {
	var packet = Packet{MsgType: token}
	data, err := json.Marshal(packet)
	if err != nil {
		errorLogger.Println("Error serializing JSON", err)
		return
	}
	writeChannel <- Message{Addr: peerHostnameToAddr[succ], Data: string(data)}
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
	lock.Lock()
	readConnMap[conn.RemoteAddr()] = conn
	peer, err := getHostnameFromIP(conn.RemoteAddr())

	if err != nil {
		errorLogger.Println("Unable to resolve hostname from IP", err)
	}

	peerReadAddrToHostname[conn.RemoteAddr()] = peer
	lock.Unlock()

	// Read messages from the peer
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			errorLogger.Println("Error reading from peer:", err)
			return
		}
		message := string(buffer[:n])
		readChannel <- Message{Addr: conn.RemoteAddr(), Data: message}
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
			msg, err := json.Marshal(Marker{SnapshotId: snapshotId, Initiator: initiator})
			if err != nil {
				errorLogger.Println("Error serializing JSON:", err)
				continue
			}
			var packet = Packet{MsgType: marker, Msg: string(msg)}
			msg, err = json.Marshal(packet)
			if err != nil {
				errorLogger.Println("Error serializing JSON:", err)
				continue
			}
			writeChannel <- Message{Addr: peerHostnameToAddr[peer], Data: string(msg)}
		}
	}
}

func sendMarkerWithDelay(markerDelay float64, snapshotId int, initiator int, me int) {
	time.Sleep(time.Duration(int64(markerDelay * 1e9)))
	go sendMarker(snapshotId, initiator, me)
}

func initSnapshot(markerDelay float64, sender int, snapshotDelay float64, snapshotId int, initiator int, me int) {
	time.Sleep(time.Duration(int64(snapshotDelay * 1e9)))
	errorLogger.Printf("{proc_id:%d, snapshot_id: %d, snapshot:\"started\"}\n", me, snapshotId)
	stateLock.Lock()
	var snap = Snapshot{state: state}
	stateLock.Unlock()
	snapshots[snapshotId] = &snap
	snap.channels = make(map[int]*Channel)
	for _, id := range procIds {
		if id != me {
			snap.channels[id] = &Channel{queue: make([]string, 0)}
		}
	}
	if sender != -1 {
		closeChannel(snapshotId, sender, me)
	}
	go sendMarkerWithDelay(markerDelay, snapshotId, initiator, me)
}

func addToQueue(sender int, message string) {
	for _, snap := range snapshots {
		channel := snap.channels[sender]
		channel.lock.Lock()
		if !channel.close {
			queue := channel.queue
			queue = append(queue, message)
			channel.queue = queue
		}
		channel.lock.Unlock()
		snap.channels[sender] = channel
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
	go checkIfSnapshotFinished(snapshotId, me)
}

func checkIfSnapshotFinished(snapshotId int, me int) {
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

func main() {

	hostsfile := flag.String("h", "", "Name of the file containing the list of peers")
	tokenDelay := flag.Float64("t", float64(0), "Delay in seconds before sending the token")
	markerDelay := flag.Float64("m", float64(-1), "Delay in seconds before sending the marker")
	snapshotDelay := flag.Float64("s", float64(-1), "Delay in seconds before initiating the snapshot")
	snapshotId := flag.Int("p", -1, "ID of the snapshot")
	hasToken := flag.Bool("x", false, "Do I have the token?")

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
		go initSnapshot(*markerDelay, -1, *snapshotDelay, *snapshotId, procIds[host], procIds[host])
	}

	for {
		select {
		case Msg := <-readChannel:
			var packet Packet
			json.Unmarshal([]byte(Msg.Data), &packet)
			MsgType := packet.MsgType
			switch MsgType {
			case token: // Token received
				// errorLogger.Printf(
				// 	"{proc_id: %d, sender: %d, receiver: %d, message:\"token\"}\n",
				// 	procIds[host],
				// 	procIds[peerReadAddrToHostname[Msg.Addr]],
				// 	procIds[host],
				// )
				go passTokenWithDelay(*tokenDelay, succ, host)
				// Add the message to the queue
				go addToQueue(procIds[peerReadAddrToHostname[Msg.Addr]], token)
			case marker: // Marker Received
				var marker Marker
				json.Unmarshal([]byte(packet.Msg), &marker)
				Initiator := marker.Initiator
				SnapshotId := marker.SnapshotId
				_, ok := snapshots[SnapshotId]
				if !ok {
					sender := procIds[peerReadAddrToHostname[Msg.Addr]]
					go initSnapshot(*markerDelay, sender, 0, SnapshotId, Initiator, procIds[host])
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
			var packet Packet
			json.Unmarshal([]byte(Msg.Data), &packet)
			MsgType := packet.MsgType
			switch MsgType {
			case token:
				errorLogger.Printf(
					"{proc_id: %d, sender: %d, receiver: %d, message:\"token\"}\n",
					procIds[host],
					procIds[host],
					procIds[peerWriteAddrToHostname[Msg.Addr]],
				)
			case marker:
				var marker Marker
				json.Unmarshal([]byte(packet.Msg), &marker)
				SnapshotId := marker.SnapshotId
				stateLock.Lock()
				errorLogger.Printf(
					"{proc_id:%d, snapshot_id: %d, sender:%d, receiver:%d, msg:\"marker\", state:%d, has_token:YES/NO}",
					procIds[host],
					SnapshotId,
					procIds[host],
					procIds[peerWriteAddrToHostname[Msg.Addr]],
					state,
				)
				stateLock.Unlock()
			}
		}
	}

	// wg.Wait()

}
