package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-ecommerce/common"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": common.KafkaBroker,
		"group.id":          "grup-pengiriman",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Gagal membuat consumer: %s\n", err)
	}
	defer c.Close()

	// Subscribe ke TopicSiapKirim
	c.SubscribeTopics([]string{common.TopicSiapKirim}, nil)
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
				pesanan, err := common.DeserializePesanan(e.Value)
				if err != nil {
					log.Printf("Gagal deserialize: %s\n", err)
					continue
				}

				fmt.Printf("[PENGIRIMAN] Menerima Pesanan #%d\n", pesanan.ID)
				time.Sleep(1 * time.Second)

				pesanan.Status = "DALAM PERJALANAN"
				fmt.Printf("=== [PENGIRIMAN] Pesanan #%d BERHASIL DIKIRIM! ===\n", pesanan.ID)

			case kafka.Error:
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			}
		}
	}
	fmt.Println("Departemen PENGIRIMAN berhenti.")
}
