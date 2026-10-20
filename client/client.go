package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		fmt.Println("[ПОМИЛКА] Не вдалося підключитися до сервера:", err)
		return
	}
	defer conn.Close()

	fmt.Println("[КЛІЄНТ] Успішно підключено до сервера. Вводьте команди:")

	go func() {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fmt.Fprintln(conn, scanner.Text())
	}
}