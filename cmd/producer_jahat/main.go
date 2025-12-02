package main

import (
	"fmt"
	"kafka-ecommerce/common"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": common.KafkaBroker})
	if err != nil {
		log.Fatalf("Gagal membuat producer: %s\n", err)
	}
	defer p.Close()

	topic := common.TopicPesananMasuk

	// DATA SAMPAH (Bukan JSON yang valid)
	pesanSampah := []string{
		"INI BUKAN JSON",
		"DATA_RUSAK_123",
		"{json: 'tidak valid'}", // Json ngawur
	}

	fmt.Printf("😈 Mengirim %d pesan SAMPAH ke topic %s...\n", len(pesanSampah), topic)

	for _, sampah := range pesanSampah {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(sampah), // Kirim raw string, bukan hasil Serialize()
		}, nil)

		if err != nil {
			log.Printf("Gagal kirim sampah: %s", err)
		}
	}

	p.Flush(5000)
	fmt.Println("✅ Sampah berhasil dikirim. Siap-siap error!")
}
