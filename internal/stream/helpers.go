package stream

func (m *StreamManager) GetForwardReceiverCount() int {
	m.fwdMu.RLock()
	defer m.fwdMu.RUnlock()
	return len(m.forwardedReceivers)
}

func (m *StreamManager) GetRandomForwardReceiver() string {
	m.fwdMu.RLock()
	defer m.fwdMu.RUnlock()
	for id := range m.forwardedReceivers {
		return id
	}
	return ""
}
