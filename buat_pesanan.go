package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": KafkaBroker})
	if err != nil {
		log.Fatalf("Gagal membuat producer: %s\n", err)
	}
	defer p.Close()

	// Goroutine untuk menangani laporan pengiriman
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Gagal mengirim pesan: %v\n", ev.TopicPartition.Error)
				} else {
					log.Printf("Sukses kirim pesan ke topic %s [%d] di offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	topic := TopicPesananMasuk
	jumlahPesanan := 5

	fmt.Printf("Mengirim %d pesanan ke topic %s...\n", jumlahPesanan, topic)

	for i := 1; i <= jumlahPesanan; i++ {
		pesanan := Pesanan{
			ID:         i,
			NamaBarang: fmt.Sprintf("Barang Keren #%d", i),
			Status:     "BARU",
		}

		jsonData, err := pesanan.Serialize()
		if err != nil {
			log.Printf("Gagal serialize pesanan %d: %s\n", i, err)
			continue
		}

		// Kirim pesan secara asinkron
		topic := TopicPesananMasuk
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          jsonData,
		}, nil)

		time.Sleep(100 * time.Millisecond) // Kasih jeda sedikit
	}

	// Tunggu semua pesan terkirim sebelum keluar
	p.Flush(15 * 1000)
	fmt.Println("Semua pesanan selesai dikirim.")
}
