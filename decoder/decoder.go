package decoder

import (
	"os"
	"strings"
	"time"

	"github.com/negbie/fluxify/config"
	"github.com/tsg/gopacket"
	"github.com/tsg/gopacket/layers"
)

type Packet struct {
	Ts      time.Time `json:"ts"`
	Host    string    `json:"host,omitempty"`
	Payload []byte    `json:"-"`
}

type Decoder struct {
	Host string
}

func NewDecoder() *Decoder {
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	return &Decoder{Host: host}
}

func (d *Decoder) Process(data []byte, ci *gopacket.CaptureInfo) (*Packet, error) {
	pkt := &Packet{
		Host: d.Host,
		Ts:   ci.Timestamp,
	}

	packet := gopacket.NewPacket(data, layers.LayerTypeEthernet, gopacket.NoCopy)
	if app := packet.ApplicationLayer(); app != nil {

		if config.Cfg.InfluxFilter != "" && strings.Contains(string(app.Payload()), config.Cfg.InfluxFilter) {
			return nil, nil
		}
		for _, layer := range packet.Layers() {
			switch layer.LayerType() {

			case layers.LayerTypeUDP:
				udpl := packet.Layer(layers.LayerTypeUDP)
				udp, ok := udpl.(*layers.UDP)
				if !ok {
					break
				}
				pkt.Payload = udp.Payload
				return pkt, nil

			case layers.LayerTypeTCP:
				tcpl := packet.Layer(layers.LayerTypeTCP)
				tcp, ok := tcpl.(*layers.TCP)
				if !ok {
					break
				}
				pkt.Payload = tcp.Payload
				return pkt, nil
			}
		}
	}
	return nil, nil
}
