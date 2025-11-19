package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": KafkaBroker,
		"group.id":          "grup-pengiriman", // Beda Group ID
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Gagal membuat consumer: %s\n", err)
	}
	defer c.Close()

	c.SubscribeTopics([]string{TopicSiapKirim}, nil) // Beda Topic Baca
	fmt.Println("Departemen PENGIRIMAN siap mencetak resi...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Tertangkap sinyal %v: shutting down...\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				pesanan, err := DeserializePesanan(e.Value)
				if err != nil {
					log.Printf("Gagal deserialize: %s\n", err)
					continue
				}

				fmt.Printf("[PENGIRIMAN] Menerima Pesanan #%d\n", pesanan.ID)

				// 1. Pura-pura cetak resi
				time.Sleep(1 * time.Second)
				pesanan.Status = "DALAM PERJALANAN"
				fmt.Printf("=========================================\n")
				fmt.Printf("[PENGIRIMAN] Pesanan #%d BERHASIL DIKIRIM!\n", pesanan.ID)
				fmt.Printf("=========================================\n")

			case kafka.Error:
				log.Printf("%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			}
		}
	}
	fmt.Println("Departemen PENGIRIMAN berhenti.")
}
