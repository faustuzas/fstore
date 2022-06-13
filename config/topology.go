package config

import "fmt"

const (
	_topologyFile = "topology.yaml"
)

type ServerId int

type Server struct {
	Id ServerId `yaml:"id"`

	Host     string `yaml:"host"`
	RaftPort int    `yaml:"raft_port"`
	HttpPort int    `yaml:"http_port"`

	StateFile string `yaml:"state_file"`
	LogDir    string `yaml:"log_dir"`
}

func (s Server) RaftAddress() string {
	return fmt.Sprintf("%s:%d", s.Host, s.RaftPort)
}

func (s Server) HttpAddress() string {
	return fmt.Sprintf("%s:%d", s.Host, s.HttpPort)
}

type Topology struct {
	Servers map[ServerId]Server
}

func (f Fetcher) FetchTopology() (Topology, error) {
	topology := Topology{}

	if err := f.fillFromFile(_topologyFile, &topology); err != nil {
		return Topology{}, err
	}

	// assign ids for easier access
	for id, server := range topology.Servers {
		server.Id = id
		topology.Servers[id] = server
	}

	return topology, nil
}
