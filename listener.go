package q

import (
	"log"

	"github.com/go-stomp/stomp/v3"
)

type MessageReceiver interface {
	Process(body []byte) error
}

type Listener struct {
	Stomp    *Stomp
	Receiver MessageReceiver
}

func Newlistener(Stomp *Stomp, receiver MessageReceiver) *Listener {
	pg := &Listener{
		Stomp:    Stomp,
		Receiver: receiver,
	}
	return pg
}

func (l *Listener) Listen(dest string) error {
	log.Println("listen to queue: " + dest)

	sub, err := l.Stomp.Conn.Subscribe(dest, stomp.AckAuto, SetDestinationTopicOption)

	if err != nil {
		log.Fatalf("failed to subscribe to queue: [%v]", err)
		return err
	}

	for {
		if msg, err := sub.Read(); err != nil {
			log.Fatalf("failed to read message: [%v]", err)
			return err
		} else if err = l.Receiver.Process(msg.Body); err != nil {
			log.Fatalf("failed to process message: [%v]", err)
			return err
		}
	}

}
