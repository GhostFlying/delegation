package hostkind

import "fmt"

type Kind string

const (
	Codex Kind = "codex"
	TraeX Kind = "traex"
)

func (k Kind) Validate() error {
	switch k {
	case Codex, TraeX:
		return nil
	default:
		return fmt.Errorf("unsupported host kind %q", k)
	}
}
