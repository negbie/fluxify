package outputs

import (
	"github.com/negbie/fluxify/decoder"
	"github.com/negbie/fluxify/logp"
)

type Publisher struct {
	Queue    chan *decoder.Packet
	outputer Outputer
}

type Outputer interface {
	Output(msg []byte)
}

func NewPublisher(o Outputer) *Publisher {
	p := &Publisher{
		Queue:    make(chan *decoder.Packet),
		outputer: o,
	}
	go p.Start()
	return p
}

func (pub *Publisher) PublishEvent(pkt *decoder.Packet) {
	pub.Queue <- pkt
}

func (pub *Publisher) output(pkt *decoder.Packet) {
	defer func() {
		if err := recover(); err != nil {
			logp.Err("%v", err)
		}
	}()
	pub.outputer.Output(pkt.Payload)
}

func (pub *Publisher) Start() {
	for {
		select {
		case pkt := <-pub.Queue:
			pub.output(pkt)
		}
	}
}
