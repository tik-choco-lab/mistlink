package signaling

import (
	"sync"

	"github.com/gorilla/websocket"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/domain"
	"github.com/tik-choco-lab/mistlink/internal/logger"
)

type signalingMessage struct {
	Type       string `json:"Type"`
	Data       string `json:"Data"`
	SenderId   string `json:"SenderId"`
	ReceiverId string `json:"ReceiverId"`
	RoomId     string `json:"RoomId"`
}

type Client struct {
	conn     *websocket.Conn
	roomID   string
	clientID string
	mu       sync.Mutex

	onOffer      func(offer string, senderID string)
	onAnswer     func(answer string, senderID string)
	onCandidate  func(candidate string, senderID string)
	onRequest    func(senderID string)
	onDisconnect func(senderID string)
	onMessage    func(domain.Message)
}

func NewClient(cfg *config.Config, clientID string) (*Client, error) {
	conn, _, err := websocket.DefaultDialer.Dial(cfg.SignalingServer, nil)
	if err != nil {
		return nil, err
	}

	client := &Client{
		conn:     conn,
		roomID:   cfg.RoomID,
		clientID: clientID,
	}

	joinMsg := signalingMessage{
		Type:     "Join",
		RoomId:   cfg.RoomID,
		SenderId: clientID,
	}
	if err := client.sendMessage(joinMsg); err != nil {
		conn.Close()
		return nil, err
	}

	requestMsg := signalingMessage{
		Type:     "Request",
		RoomId:   cfg.RoomID,
		SenderId: clientID,
	}
	if err := client.sendMessage(requestMsg); err != nil {
		conn.Close()
		return nil, err
	}

	go client.readLoop()

	return client, nil
}

func (c *Client) SetCallbacks(
	onOffer func(offer string, senderID string),
	onAnswer func(answer string, senderID string),
	onCandidate func(candidate string, senderID string),
	onRequest func(senderID string),
	onDisconnect func(senderID string),
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onOffer = onOffer
	c.onAnswer = onAnswer
	c.onCandidate = onCandidate
	c.onRequest = onRequest
	c.onDisconnect = onDisconnect
}

func (c *Client) SendOffer(offer string, receiverID string) error {
	msg := signalingMessage{
		Type:       "Offer",
		Data:       offer,
		SenderId:   c.clientID,
		ReceiverId: receiverID,
		RoomId:     c.roomID,
	}
	return c.sendMessage(msg)
}

func (c *Client) SendAnswer(answer string, receiverID string) error {
	msg := signalingMessage{
		Type:       "Answer",
		Data:       answer,
		SenderId:   c.clientID,
		ReceiverId: receiverID,
		RoomId:     c.roomID,
	}
	return c.sendMessage(msg)
}

func (c *Client) SendCandidate(candidate string, receiverID string) error {
	msg := signalingMessage{
		Type:       "Candidate",
		Data:       candidate,
		SenderId:   c.clientID,
		ReceiverId: receiverID,
		RoomId:     c.roomID,
	}
	return c.sendMessage(msg)
}

func (c *Client) SendRequest(receiverID string) error {
	msg := signalingMessage{
		Type:       "Request",
		Data:       "",
		SenderId:   c.clientID,
		ReceiverId: receiverID,
		RoomId:     c.roomID,
	}
	return c.sendMessage(msg)
}

func (c *Client) sendMessage(msg signalingMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(msg)
}

func (c *Client) readLoop() {
	for {
		var msg signalingMessage
		if err := c.conn.ReadJSON(&msg); err != nil {
			logger.Errorf("signaling", "signaling read error: %v", err)
			return
		}

		c.mu.Lock()

		if c.onMessage != nil {
			messageType := msg.Type
			if messageType == "Offer" {
				messageType = "offer"
			} else if messageType == "Answer" {
				messageType = "answer"
			} else if messageType == "Candidate" {
				messageType = "candidate"
			}
			go c.onMessage(domain.Message{
				Type:       messageType,
				Data:       msg.Data,
				SenderId:   msg.SenderId,
				ReceiverId: msg.ReceiverId,
				RoomId:     msg.RoomId,
			})
		}

		switch msg.Type {
		case "Offer":
			if c.onOffer != nil {
				go c.onOffer(msg.Data, msg.SenderId)
			}
		case "Answer":
			if c.onAnswer != nil {
				go c.onAnswer(msg.Data, msg.SenderId)
			}
		case "Candidate":
			if c.onCandidate != nil {
				go c.onCandidate(msg.Data, msg.SenderId)
			}
		case "Request":
			if c.onRequest != nil {
				go c.onRequest(msg.SenderId)
			}
		case "Disconnect":
			if c.onDisconnect != nil {
				go c.onDisconnect(msg.SenderId)
			}
		}
		c.mu.Unlock()
	}
}

func (c *Client) OnMessage(handler func(domain.Message)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onMessage = handler
}

func (c *Client) Send(msg domain.Message) error {
	sigMsg := signalingMessage{
		Type:       msg.Type,
		Data:       msg.Data,
		SenderId:   msg.SenderId,
		ReceiverId: msg.ReceiverId,
		RoomId:     msg.RoomId,
	}
	return c.sendMessage(sigMsg)
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
