package common

import "encoding/json"

const KafkaBroker = "192.168.200.212:9092"

const (
	TopicPesananMasuk = "topic_pesanan_masuk"
	TopicStokAman     = "topic_stok_aman"
	TopicSiapKirim    = "topic_siap_kirim"
)

type Pesanan struct {
	ID         int    `json:"id_pesan"`
	NamaBarang string `json:"nama_barang"`
	Status     string `json:"status"`
}

func (p *Pesanan) Serialize() ([]byte, error) {
	return json.Marshal(p)
}

func DeserializePesanan(data []byte) (Pesanan, error) {
	var p Pesanan
	err := json.Unmarshal(data, &p)
	return p, err
}
