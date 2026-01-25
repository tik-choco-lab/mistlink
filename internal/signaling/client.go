package signaling

import (
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/logger"
)

type Client struct {
	conn     *websocket.Conn
	roomID   string
	clientID string
	config   *config.Config
	done     chan struct{}
	wg       sync.WaitGroup
	isClosed bool
	sendChan chan Message
	mu       sync.Mutex
	onOffer      func(string, string)
	onAnswer     func(string, string)
	onCandidate  func(string, string)
	onRequest    func(string)
	onRedirect   func(string, string)
	onDisconnect func(string)
	onMessage    func(Message)
}

func NewClient(cfg *config.Config, clientID string) (*Client, error) {
	c := &Client{
		config:   cfg,
		roomID:   cfg.RoomID,
		clientID: clientID,
		sendChan: make(chan Message, 1024),
		done:     make(chan struct{}),
	}

	if err := c.connect(); err != nil {
		logger.Warnf("signaling", "Initial connection failed: %v. Starting reconnect loop...", err)
	}
	
	go c.reconnectLoop()

	return c, nil
}

func (c *Client) connect() error {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	conn, _, err := websocket.DefaultDialer.Dial(c.config.SignalingServer, nil)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	joinMsg := Message{Type: "Join", RoomId: c.roomID, SenderId: c.clientID}
	if err := c.sendImmediate(conn, joinMsg); err != nil {
		conn.Close()
		return err
	}

	requestMsg := Message{Type: "Request", RoomId: c.roomID, SenderId: c.clientID}
	if err := c.sendImmediate(conn, requestMsg); err != nil {
		conn.Close()
		return err
	}

	go c.writePump(conn)
	go c.readLoop(conn)
	
	logger.Debugf("signaling", "Connected to signaling server")
	return nil
}

func (c *Client) reconnectLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.mu.Lock()
			if c.conn != nil || c.isClosed {
				c.mu.Unlock()
				continue
			}
			c.mu.Unlock()

			logger.Debugf("signaling", "Attempting to reconnect...")
			if err := c.connect(); err != nil {
				logger.Warnf("signaling", "Reconnect failed: %v", err)
			}
		}
	}
}

func (c *Client) SetCallbacks(
	onOffer func(offer string, senderID string),
	onAnswer func(answer string, senderID string),
	onCandidate func(candidate string, senderID string),
	onRequest func(senderID string),
	onRedirect func(targetID string, senderID string),
	onDisconnect func(senderID string),
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onOffer = onOffer
	c.onAnswer = onAnswer
	c.onCandidate = onCandidate
	c.onRequest = onRequest
	c.onRedirect = onRedirect
	c.onDisconnect = onDisconnect
}

func (c *Client) SendOffer(offer string, receiverID string) error {
	return c.sendMessage(Message{Type: "Offer", Data: offer, SenderId: c.clientID, ReceiverId: receiverID, RoomId: c.roomID, RootId: c.clientID})
}

func (c *Client) SendAnswer(answer string, receiverID string) error {
	return c.sendMessage(Message{Type: "Answer", Data: answer, SenderId: c.clientID, ReceiverId: receiverID, RoomId: c.roomID})
}

func (c *Client) SendCandidate(candidate string, receiverID string) error {
	return c.sendMessage(Message{Type: "Candidate", Data: candidate, SenderId: c.clientID, ReceiverId: receiverID, RoomId: c.roomID})
}

func (c *Client) SendRequest(receiverID string) error {
	return c.sendMessage(Message{Type: "Request", Data: "", SenderId: c.clientID, ReceiverId: receiverID, RoomId: c.roomID})
}

func (c *Client) SendRedirect(targetID string, receiverID string) error {
	return c.sendMessage(Message{Type: "Redirect", Data: targetID, SenderId: c.clientID, ReceiverId: receiverID, RoomId: c.roomID, RootId: c.clientID})
}

func (c *Client) sendMessage(msg Message) error {
	select {
	case c.sendChan <- msg:
		return nil
	default:
		logger.Errorf("signaling", "Send buffer full, dropping message")
		return fmt.Errorf("send buffer full")
	}
}

func (c *Client) sendImmediate(conn *websocket.Conn, msg Message) error {
	return conn.WriteJSON(msg)
}

func (c *Client) writePump(conn *websocket.Conn) {
	defer func() {
	}()
	
	for {
		select {
		case msg := <-c.sendChan:
			c.mu.Lock()
			currentConn := c.conn
			c.mu.Unlock()
			
			if currentConn != conn {
				return 
			}
			
			if err := conn.WriteJSON(msg); err != nil {
				logger.Errorf("signaling", "write error: %v", err)
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *Client) readLoop(conn *websocket.Conn) {
	defer func() {
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
			conn.Close()
		}
		c.mu.Unlock()
	}()

	for {
		var msg Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				logger.Warnf("signaling", "Read error: %v. Disconnecting.", err)
			}
			return
		}

		c.dispatchMessage(msg)
	}
}

func (c *Client) dispatchMessage(msg Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.onMessage != nil {
		messageType := msg.Type
		switch messageType {
		case "Offer":
			messageType = "offer"
		case "Answer":
			messageType = "answer"
		case "Candidate":
			messageType = "candidate"
		case "Redirect":
			messageType = "redirect"
		}
		
		go c.onMessage(Message{
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
			c.onOffer(msg.Data, msg.SenderId)
		}
	case "Answer":
		if c.onAnswer != nil {
			c.onAnswer(msg.Data, msg.SenderId)
		}
	case "Candidate":
		if c.onCandidate != nil {
			c.onCandidate(msg.Data, msg.SenderId)
		}
	case "Request":
		if c.onRequest != nil {
			c.onRequest(msg.SenderId)
		}
	case "Redirect":
		if c.onRedirect != nil {
			c.onRedirect(msg.Data, msg.SenderId)
		}
	case "Disconnect":
		if c.onDisconnect != nil {
			c.onDisconnect(msg.SenderId)
		}
	}
}

func (c *Client) OnMessage(handler func(Message)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onMessage = handler
}

func (c *Client) Send(msg Message) error {
	return c.sendMessage(msg)
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return nil
	}
	c.isClosed = true
	close(c.done)
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	
	if conn != nil {
		return conn.Close()
	}
	return nil
}
