package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	tcpPort = ":9000"
	udpPort = ":9001"
)

type Client struct {
	conn net.Conn
	addr string
}

type Server struct {
	mu          sync.RWMutex
	clients     map[net.Conn]*Client
	tcpListener net.Listener
	udpConn     net.PacketConn
	wg          sync.WaitGroup
	quit        chan struct{}
}

func NewServer() *Server {
	return &Server{
		clients: make(map[net.Conn]*Client),
		quit:    make(chan struct{}),
	}
}

func main() {
	fmt.Printf("=== Мережевий сервіс диспетчеризації (TCP: %s, UDP: %s) ===\n", tcpPort[1:], udpPort[1:])

	server := NewServer()

	var err error
	server.tcpListener, err = net.Listen("tcp", tcpPort)
	if err != nil {
		fmt.Printf("[ПОМИЛКА] Не вдалося відкрити TCP-сокет: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[СЕРВЕР] TCP-слухач успішно відкрито на порту %s\n", tcpPort[1:])


	server.udpConn, err = net.ListenPacket("udp", udpPort)
	if err != nil {
		fmt.Printf("[ПОМИЛКА] Не вдалося відкрити UDP-сокет: %v\n", err)
		server.tcpListener.Close()
		os.Exit(1)
	}
	fmt.Printf("[СЕРВЕР] UDP-слухач телеметрії відкрито на порту %s\n", udpPort[1:])

	server.wg.Add(2)
	go server.acceptTCPConnections()
	go server.listenUDPTelemetry()

	
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\n[СЕРВЕР] Отримано сигнал завершення роботи. Запуск Graceful Shutdown...")

	
	server.Shutdown()
}

.
func (s *Server) acceptTCPConnections() {
	defer s.wg.Done()

	for {
		conn, err := s.tcpListener.Accept()
		if err != nil {
			select {
			case <-s.quit:
			
				return
			default:
				fmt.Printf("[ПОМИЛКА] Помилка прийняття з'єднання: %v\n", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleClient(conn)
	}
}


func (s *Server) handleClient(conn net.Conn) {
	defer s.wg.Done()

	addr := conn.RemoteAddr().String()
	client := &Client{conn: conn, addr: addr}


	s.registerClient(client)


	defer func() {
		s.unregisterClient(client)
		conn.Close()
	}()

	fmt.Fprintln(conn, "[СЕРВЕР] Ласкаво просимо до мережевого сервісу диспетчеризації!")

	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
			
				select {
				case <-s.quit:
				default:
					fmt.Printf("[ПОМИЛКА] Читання від %s: %v\n", addr, err)
				}
			}
			break
		}

		cmdRaw := strings.TrimSpace(line)
		if cmdRaw == "" {
			continue
		}

		parts := strings.SplitN(cmdRaw, " ", 2)
		cmd := strings.ToUpper(parts[0])
		arg := ""
		if len(parts) > 1 {
			arg = parts[1]
		}

		switch cmd {
		case "ECHO":
			fmt.Fprintf(conn, "[ECHO]: %s\n", arg)

		case "TIME":
			fmt.Fprintf(conn, "[TIME]: %s\n", time.Now().Format(time.RFC3339))

		case "BROADCAST", "MSG":
			if arg == "" {
				fmt.Fprintln(conn, "[ПОМИЛКА]: Повідомлення не може бути порожнім. Використання: BROADCAST <текст>")
				continue
			}
			s.broadcastMessage(client, fmt.Sprintf("[СПОВІЩЕННЯ від %s]: %s\n", addr, arg))
			fmt.Printf("[РОЗСИЛКА] Від %s -> \"%s\"\n", addr, arg)

		case "CLOSE", "QUIT":
			fmt.Fprintln(conn, "[СЕРВЕР] До побачення!")
			return

		default:
			fmt.Fprintln(conn, "[ПОМИЛКА] Невідома команда. Доступні команди: ECHO <msg>, TIME, BROADCAST <msg>, CLOSE, QUIT")
		}
	}
}


func (s *Server) registerClient(client *Client) {
	s.mu.Lock()
	s.clients[client.conn] = client
	s.mu.Unlock()

	fmt.Printf("[ПІДКЛЮЧЕННЯ] Новий клієнт підключився: %s\n", client.addr)
	s.broadcastMessage(client, fmt.Sprintf("[СИСТЕМА] Підключився новий клієнт: %s\n", client.addr))
}


func (s *Server) unregisterClient(client *Client) {
	s.mu.Lock()
	delete(s.clients, client.conn)
	s.mu.Unlock()

	fmt.Printf("[ВІД'ЄДНАННЯ] Клієнт відключився: %s\n", client.addr)
	s.broadcastMessage(nil, fmt.Sprintf("[СИСТЕМА] Клієнт %s залишив мережу\n", client.addr))
}


func (s *Server) broadcastMessage(sender *Client, message string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, client := range s.clients {
		if sender != nil && client.conn == sender.conn {
			continue
		_, _ = client.conn.Write([]byte(message))
	}
}

func (s *Server) listenUDPTelemetry() {
	defer s.wg.Done()

	buf := make([]byte, 2048)

	for {
		n, addr, err := s.udpConn.ReadFrom(buf)
		if err != nil {
			select {
			case <-s.quit:
			
				return
			default:
				fmt.Printf("[ПОМИЛКА] Читання UDP: %v\n", err)
				continue
			}
		}

		data := strings.TrimSpace(string(buf[:n]))
		fmt.Printf("[UDP ТЕЛЕМЕТРІЯ] Отримано %d байт від %s -> \"%s\"\n", n, addr.String(), data)

		telemetryMsg := fmt.Sprintf("[TELEMETRY from %s]: %s\n", addr.String(), data)
		s.broadcastMessage(nil, telemetryMsg)
	}
}

func (s *Server) Shutdown() {
	close(s.quit)


	s.mu.Lock()
	count := len(s.clients)
	for _, client := range s.clients {
		_, _ = client.conn.Write([]byte("[СИСТЕМА]: SERVER_SHUTTING_DOWN\n"))
		client.conn.Close()
	}

	s.clients = make(map[net.Conn]*Client)
	s.mu.Unlock()

	
	if s.tcpListener != nil {
		s.tcpListener.Close()
	}
	if s.udpConn != nil {
		s.udpConn.Close()
	}

	
	s.wg.Wait()

	fmt.Printf("[СЕРВЕР] Сповіщено активних клієнтів: %d. З'єднання безпечно закриті.\n", count)
	fmt.Println("[СЕРВЕР] Роботу сервера успішно завершено.")
}