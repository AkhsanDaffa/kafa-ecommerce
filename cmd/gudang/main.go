package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"kafka-ecommerce/common"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": common.KafkaBroker})
	if err != nil {
		log.Fatalf("Gagal membuat producer: %s\n", err)
	}
	defer p.Close()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": common.KafkaBroker,
		"group.id":          "gudang_group",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Fatalf("Gagal membuat consumer: %s\n", err)
	}
	defer c.Close()

	c.SubscribeTopics([]string{common.TopicPesananMasuk}, nil)
	fmt.Println("Gudang siap menerima pesanan...")

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
					fmt.Printf("⚠️ ERROR: Data rusak ditemukan! '%s'\n", string(e.Value))

					topicDLQ := common.TopicPesananError
					p.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topicDLQ, Partition: kafka.PartitionAny},
						Value:          e.Value,
						Key:            e.Key,
					}, nil)

					fmt.Printf("🗑️  Dibuang ke DLQ: %s\n", topicDLQ)

					continue
				}

				fmt.Printf("[GUDANG] Menerim Pesanan #%d\n", pesanan.ID)

				time.Sleep(100 * time.Millisecond)

				jsonData, _ := pesanan.Serialize()

				topicTujuan := common.TopicStokAman
				p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topicTujuan, Partition: kafka.PartitionAny},
					Value:          jsonData,
				}, nil)

			case kafka.Error:
				log.Printf("Error: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			}
		}
	}
	fmt.Println("Departemen gudang selesai bekerja.")
}
