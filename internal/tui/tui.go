package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/tik-choco-lab/mistlink/internal/capture"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/stream"
)

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FAFAFA")).
			Background(lipgloss.Color("#7D56F4")).
			Padding(0, 1).
			MarginBottom(1)

	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#9B9B9B")).
			MarginBottom(1)

	statsStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575")).
			Bold(true)

	logLevelStyle = map[string]lipgloss.Style{
		"DEBUG": lipgloss.NewStyle().Foreground(lipgloss.Color("#7F7F7F")),
		"INFO":  lipgloss.NewStyle().Foreground(lipgloss.Color("#5BC0EB")),
		"WARN":  lipgloss.NewStyle().Foreground(lipgloss.Color("#FDE74C")),
		"ERROR": lipgloss.NewStyle().Foreground(lipgloss.Color("#E55934")),
	}
)

type State int

const (
	StateSelecting State = iota
	StateRunning
)

type model struct {
	cfg      *config.Config
	manager  *stream.StreamManager
	state    State
	viewport viewport.Model
	logs     []string
	width    int
	height   int

	// Selection state
	choices     []string
	cursor      int
	windowTitle string

	// Signaling
	OnSelect func(inputType string, target string)
}

type logMsg logger.LogEntry
type tickMsg time.Time

func NewModel(cfg *config.Config, manager *stream.StreamManager) model {
	vp := viewport.New(0, 0)
	vp.Style = lipgloss.NewStyle().
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62"))

	state := StateRunning
	if !cfg.ScreenCapture && cfg.InputURL == "udp://0.0.0.0:1234" {
		state = StateSelecting
	}

	return model{
		cfg:      cfg,
		manager:  manager,
		state:    state,
		viewport: vp,
		logs:     make([]string, 0),
		choices:  []string{"UDP Stream (Default)", "Full Screen Capture", "Specific Window Capture..."},
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		tea.Tick(time.Second, func(t time.Time) tea.Msg {
			return tickMsg(t)
		}),
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.state == StateSelecting {
			switch msg.String() {
			case "up", "j":
				if m.cursor > 0 {
					m.cursor--
				}
			case "down", "k":
				if m.cursor < len(m.choices)-1 {
					m.cursor++
				}
			case "enter":
				choice := m.choices[m.cursor]
				if choice == "Specific Window Capture..." {
					wins, err := capture.ListWindows()
					if err == nil && len(wins) > 0 {
						m.choices = []string{"Back"}
						for _, w := range wins {
							m.choices = append(m.choices, w.Title)
						}
						m.cursor = 0
						return m, nil
					}
				}
				if choice == "Back" {
					m.choices = []string{"UDP Stream (Default)", "Full Screen Capture", "Specific Window Capture..."}
					m.cursor = 0
					return m, nil
				}

				inputType := "udp"
				target := ""
				if choice == "Full Screen Capture" {
					inputType = "screen"
					target = "entire"
				} else if strings.Contains(choice, "UDP Stream") {
					inputType = "udp"
				} else {
					inputType = "screen"
					target = choice
				}

				if m.OnSelect != nil {
					m.OnSelect(inputType, target)
				}
				m.state = StateRunning
			}
		}

		if msg.String() == "ctrl+c" || msg.String() == "q" {
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

		m.viewport.Width = msg.Width
		m.viewport.Height = msg.Height - 12
		if m.viewport.Height < 5 {
			m.viewport.Height = 5
		}

	case logMsg:
		entry := logger.LogEntry(msg)
		timeStr := entry.Time.Format("15:04:05")
		level := entry.Level
		style, ok := logLevelStyle[level]
		if !ok {
			style = lipgloss.NewStyle()
		}

		line := fmt.Sprintf("%s %s %s",
			lipgloss.NewStyle().Foreground(lipgloss.Color("#5A5A5A")).Render(timeStr),
			style.Render(fmt.Sprintf("[%s]", level)),
			entry.Message,
		)
		m.logs = append(m.logs, line)
		if len(m.logs) > 500 {
			m.logs = m.logs[1:]
		}
		m.viewport.SetContent(strings.Join(m.logs, "\n"))
		m.viewport.GotoBottom()

	case tickMsg:
		cmds = append(cmds, tea.Tick(time.Second, func(t time.Time) tea.Msg {
			return tickMsg(t)
		}))
	}

	m.viewport, cmd = m.viewport.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	var b strings.Builder

	if m.state == StateSelecting {
		b.WriteString(titleStyle.Render("Welcome to MistLink! Select Input Source:"))
		b.WriteString("\n\n")

		for i, choice := range m.choices {
			cursor := "  "
			if m.cursor == i {
				cursor = statsStyle.Render("> ")
			}
			b.WriteString(fmt.Sprintf("%s %s\n", cursor, choice))
		}
		b.WriteString("\n" + lipgloss.NewStyle().Foreground(lipgloss.Color("#5A5A5A")).Render("arrows/j/k: move | enter: select | q: exit"))
		return b.String()
	}

	b.WriteString(titleStyle.Render("MistLink P2P Relay"))
	b.WriteString("\n")

	b.WriteString(infoStyle.Render(fmt.Sprintf("Room ID:  %s", m.cfg.RoomID)))
	b.WriteString("\n")
	b.WriteString(infoStyle.Render(fmt.Sprintf("RTSP URL: %s", m.cfg.RTSPURL)))
	b.WriteString("\n")
	b.WriteString(infoStyle.Render(fmt.Sprintf("WHIP URL: %s", m.cfg.WHIPURL)))
	b.WriteString("\n\n")

	peerCount := 0
	if m.manager != nil {
		peerCount = m.manager.GetPeerCount()
	}

	b.WriteString(statsStyle.Render(fmt.Sprintf("Active Peers: %d", peerCount)))
	b.WriteString("\n\n")

	b.WriteString("Logs:\n")
	b.WriteString(m.viewport.View())
	b.WriteString("\n")
	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#5A5A5A")).Render("q/ctrl+c: quit | arrows/pgup/pgdown: scroll logs"))

	return b.String()
}

func Run(p *tea.Program) error {
	_, err := p.Run()
	return err
}

func Start(cfg *config.Config, manager *stream.StreamManager, onSelect func(string, string)) (*tea.Program, error) {
	m := NewModel(cfg, manager)
	m.OnSelect = onSelect
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())

	logger.AddLogHandler(func(entry logger.LogEntry) {
		p.Send(logMsg(entry))
	})

	return p, nil
}
