package config

import (
	"github.com/negbie/fluxify/logp"
)

var Cfg Config

type Config struct {
	Iface          *InterfacesConfig
	Logging        *logp.Logging
	InfluxFilter   string
	InfluxdbServer string
}

type InterfacesConfig struct {
	Device       string `config:"device"`
	Type         string `config:"type"`
	File         string `config:"file"`
	WithVlans    bool   `config:"with_vlans"`
	BpfFilter    string `config:"bpf_filter"`
	Snaplen      int    `config:"snaplen"`
	BufferSizeMb int    `config:"buffer_size_mb"`
	TopSpeed     bool
	Dumpfile     string
	OneAtATime   bool
	Loop         int
}
