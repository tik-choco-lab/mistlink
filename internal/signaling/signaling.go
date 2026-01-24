package signaling

type Message struct {
	Type       string `json:"Type"`
	Data       string `json:"Data"`
	SenderId   string `json:"SenderId"`
	ReceiverId string `json:"ReceiverId"`
	RoomId     string `json:"RoomId"`
}

type Service interface {
	Send(msg Message) error
	OnMessage(handler func(Message))
	SetCallbacks(
		onOffer func(offer string, senderID string),
		onAnswer func(answer string, senderID string),
		onCandidate func(candidate string, senderID string),
		onRequest func(senderID string),
		onRedirect func(targetID string, senderID string),
		onDisconnect func(senderID string),
	)
	SendOffer(offer string, receiverID string) error
	SendAnswer(answer string, receiverID string) error
	SendCandidate(candidate string, receiverID string) error
	SendRequest(receiverID string) error
	SendRedirect(targetID string, receiverID string) error
	Close() error
}
