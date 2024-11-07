package command

import (
	"encoding/json"
	"fmt"
)

type Handler func(req json.RawMessage) (ok bool, resp interface{}, err error)

type Commander struct {
	handlers map[string]map[string][]Handler
}

func NewCommander() *Commander {
	return &Commander{
		handlers: make(map[string]map[string][]Handler),
	}
}

func (c *Commander) RegisterHandler(module, cmd string, handler Handler) {
	if _, ok := c.handlers[module]; !ok {
		c.handlers[module] = make(map[string][]Handler)
	}
	c.handlers[module][cmd] = append(c.handlers[module][cmd], handler)
}

func (c *Commander) OnHandler(module, cmd string, req json.RawMessage) (interface{}, error) {
	moduleHandlers, moduleExists := c.handlers[module]
	if !moduleExists {
		return nil, fmt.Errorf("module %s not found", module)
	}
	cmdHandlers, cmdExists := moduleHandlers[cmd]
	if !cmdExists {
		return nil, fmt.Errorf("command %s not found in module %s", cmd, module)
	}

	for _, handler := range cmdHandlers {
		if ok, resp, err := handler(req); ok {
			return resp, err
		}
	}
	return nil, fmt.Errorf("no handler succeeded for command %s in module %s", cmd, module)
}
