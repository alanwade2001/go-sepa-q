package q

import (
	"log"

	utils "github.com/alanwade2001/go-sepa-utils"
	stmp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

type Stomp struct {
	Conn *stmp.Conn
}

func NewStomp() *Stomp {
	service := &Stomp{}

	service.Connect()

	return service
}

func (s *Stomp) Connect() {

	address := utils.Getenv("STOMP_ADDRESS", "0.0.0.0:61613")
	user := utils.Getenv("STOMP_USER", "user")
	pass := utils.Getenv("STOMP_PASSWORD", "pass")
	host := utils.Getenv("STOMP_HOST", "/")
	network := utils.Getenv("STOMP_NETWORK", "tcp")

	//option := stmp.ConnOpt.Login(user, pass)
	// these are the default options that work with RabbitMQ
	loginOpt := stmp.ConnOpt.Login(user, pass)
	hostOpt := stmp.ConnOpt.Host(host)

	log.Printf("options:[%s] [%s]", user, pass)

	log.Printf("connecting to stomp address [%s]", address)
	conn, err := stmp.Dial(network, address, loginOpt, hostOpt)

	// Get a database handle.
	if err != nil {
		log.Fatal(err)
	}

	log.Println("connected to queue: ", conn)

	s.Conn = conn

}

func (s *Stomp) Disconnect() {
	s.Conn.Disconnect()
}

type MessageSender interface {
	SendMessage(dest string, bytes []byte) error
	PublishMessage(dest string, bytes []byte) error
}

func SetDestinationQueueOption(frame *frame.Frame) error {
	frame.Header.Add("destination-type", "ANYCAST")
	return nil
}

func SetDestinationTopicOption(frame *frame.Frame) error {
	frame.Header.Add("destination-type", "MULTICAST")
	return nil
}

func (s *Stomp) SendMessage(dest string, bytes []byte) error {
	return s.sendMessageWithOption(dest, bytes, false)
}

func (s *Stomp) PublishMessage(dest string, bytes []byte) error {
	return s.sendMessageWithOption(dest, bytes, true)
}

func (s *Stomp) sendMessageWithOption(dest string, bytes []byte, publish bool) error {
	log.Printf("sending bytes dest:[%s] body:[%s]", dest, string(bytes))

	opt := SetDestinationQueueOption

	if publish {
		opt = SetDestinationTopicOption
	}

	err := s.Conn.Send(
		dest,         // destination
		"text/plain", // content-type
		bytes,        // body
		opt,
	)

	if err != nil {
		return err
	}

	return nil
}
