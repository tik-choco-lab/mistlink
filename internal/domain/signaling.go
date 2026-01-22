package domain

type Message struct {
	Type       string
	Data       string
	SenderId   string
	ReceiverId string
	RoomId     string
}

type SignalingService interface {
	Send(msg Message) error
	OnMessage(handler func(Message))
	SetCallbacks(
		onOffer func(offer string, senderID string),
		onAnswer func(answer string, senderID string),
		onCandidate func(candidate string, senderID string),
		onRequest func(senderID string),
		onDisconnect func(senderID string),
	)
	SendOffer(offer string, receiverID string) error
	SendAnswer(answer string, receiverID string) error
	SendCandidate(candidate string, receiverID string) error
	SendRequest(receiverID string) error
	Close() error
}
