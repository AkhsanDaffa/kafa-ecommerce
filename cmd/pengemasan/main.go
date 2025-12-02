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
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": common.KafkaBroker})
	if err != nil {
		log.Fatalf("Gagal membuat producer: %s\n", err)
	}
	defer p.Close()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": common.KafkaBroker,
		"group.id":          "grup-pengemasan", // Beda Group ID
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Gagal membuat consumer: %s\n", err)
	}
	defer c.Close()

	c.SubscribeTopics([]string{common.TopicStokAman}, nil) // Beda Topic Baca
	fmt.Println("Departemen PENGEMASAN siap menerima pekerjaan...")

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

				fmt.Printf("[PENGEMASAN] Menerima Pesanan #%d\n", pesanan.ID)

				// 1. Pura-pura mengemas
				time.Sleep(1 * time.Second)
				pesanan.Status = "Siap Kirim"
				fmt.Printf("[PENGEMASAN] Pesanan #%d SELESAI DIKEMAS\n", pesanan.ID)

				// 2. Kirim ke ban berjalan selanjutnya
				jsonData, _ := pesanan.Serialize()

				topicTujuan := common.TopicSiapKirim
				p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topicTujuan, Partition: kafka.PartitionAny},
					Value:          jsonData,
				}, nil)

			case kafka.Error:
				log.Printf("%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			}
		}
	}
	fmt.Println("Departemen PENGEMASAN berhenti.")
}
