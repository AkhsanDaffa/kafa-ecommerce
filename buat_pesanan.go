package main

import (
	"fmt"
	"log"

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
				}
				// Kita matikan log sukses biar tidak berisik
			}
		}
	}()

	topic := TopicPesananMasuk
	jumlahPesanan := 10 // Kita kurangi jadi 10, tapi tiap pesanan punya 3 status

	fmt.Printf("🌊 Mengirim update status berurutan untuk %d Pesanan...\n", jumlahPesanan)

	for i := 1; i <= jumlahPesanan; i++ {
		// Kita kirim 3 status berurutan untuk setiap ID
		statusList := []string{"DIBUAT", "DIBAYAR", "DIKIRIM"}

		for _, status := range statusList {
			pesanan := Pesanan{
				ID:         i,
				NamaBarang: fmt.Sprintf("Barang #%d", i),
				Status:     status,
			}

			jsonData, err := pesanan.Serialize()
			if err != nil {
				log.Printf("Gagal serialize: %s", err)
				continue
			}

			// KUNCI UTAMA ADA DI SINI:
			// Kita ubah ID (int) menjadi byte array untuk dijadikan Key
			kunci := []byte(fmt.Sprintf("%d", i))

			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          jsonData,
				Key:            kunci, // <--- INI DIA RAHASIANYA
			}, nil)

			if err != nil {
				log.Printf("Gagal produce: %s", err)
			}
		}
	}

	fmt.Println("Menunggu konfirmasi pengiriman...")
	p.Flush(15 * 1000)
	fmt.Println("✅ Semua status pesanan selesai dikirim.")
}
