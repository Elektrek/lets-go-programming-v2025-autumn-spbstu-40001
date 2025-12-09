package conveyer

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Conveyer interface {
	RegisterDecorator(
		fn func(ctx context.Context, input chan string, output chan string) error,
		input string,
		output string,
	)
	RegisterMultiplexer(
		fn func(ctx context.Context, inputs []chan string, output chan string) error,
		inputs []string,
		output string,
	)
	RegisterSeparator(
		fn func(ctx context.Context, input chan string, outputs []chan string) error,
		input string,
		outputs []string,
	)
	Run(ctx context.Context) error
	Send(input string, data string) error
	Recv(output string) (string, error)
}

type conveyerImpl struct {
	mu           sync.RWMutex
	channels     map[string]chan string
	size         int
	decorators   []decoratorConfig
	multiplexers []multiplexerConfig
	separators   []separatorConfig
	wg           sync.WaitGroup
	errChan      chan error
	ctx          context.Context
	cancel       context.CancelFunc
	started      bool
}

type decoratorConfig struct {
	fn            func(ctx context.Context, input chan string, output chan string) error
	inputChannel  string
	outputChannel string
}

type multiplexerConfig struct {
	fn            func(ctx context.Context, inputs []chan string, output chan string) error
	inputChannels []string
	outputChannel string
}

type separatorConfig struct {
	fn             func(ctx context.Context, input chan string, outputs []chan string) error
	inputChannel   string
	outputChannels []string
}

func New(size int) Conveyer {
	return &conveyerImpl{
		channels: make(map[string]chan string),
		size:     size,
		errChan:  make(chan error, 100),
	}
}

func (c *conveyerImpl) getOrCreateChannel(name string) chan string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ch, exists := c.channels[name]; exists {
		return ch
	}

	ch := make(chan string, c.size)
	c.channels[name] = ch
	return ch
}

func (c *conveyerImpl) getChannel(name string) (chan string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ch, exists := c.channels[name]
	return ch, exists
}

func (c *conveyerImpl) RegisterDecorator(
	fn func(ctx context.Context, input chan string, output chan string) error,
	input string,
	output string,
) {
	if c.started {
		return
	}

	c.decorators = append(c.decorators, decoratorConfig{
		fn:            fn,
		inputChannel:  input,
		outputChannel: output,
	})
}

func (c *conveyerImpl) RegisterMultiplexer(
	fn func(ctx context.Context, inputs []chan string, output chan string) error,
	inputs []string,
	output string,
) {
	if c.started {
		return
	}

	c.multiplexers = append(c.multiplexers, multiplexerConfig{
		fn:            fn,
		inputChannels: inputs,
		outputChannel: output,
	})
}

func (c *conveyerImpl) RegisterSeparator(
	fn func(ctx context.Context, input chan string, outputs []chan string) error,
	input string,
	outputs []string,
) {
	if c.started {
		return
	}

	c.separators = append(c.separators, separatorConfig{
		fn:             fn,
		inputChannel:   input,
		outputChannels: outputs,
	})
}

func (c *conveyerImpl) Run(ctx context.Context) error {
	if c.started {
		return errors.New("conveyer already started")
	}

	c.started = true
	c.ctx, c.cancel = context.WithCancel(ctx)

	c.createChannelsForHandlers()

	c.startHandlers()

	select {
	case <-c.ctx.Done():
		c.stop()
		return c.ctx.Err()
	case err := <-c.errChan:
		c.stop()
		return err
	}
}

func (c *conveyerImpl) createChannelsForHandlers() {
	for _, d := range c.decorators {
		c.getOrCreateChannel(d.inputChannel)
		c.getOrCreateChannel(d.outputChannel)
	}

	for _, m := range c.multiplexers {
		for _, input := range m.inputChannels {
			c.getOrCreateChannel(input)
		}
		c.getOrCreateChannel(m.outputChannel)
	}

	for _, s := range c.separators {
		c.getOrCreateChannel(s.inputChannel)
		for _, output := range s.outputChannels {
			c.getOrCreateChannel(output)
		}
	}
}

func (c *conveyerImpl) startHandlers() {
	for _, d := range c.decorators {
		c.wg.Add(1)
		go func(d decoratorConfig) {
			defer c.wg.Done()

			inputCh, _ := c.getChannel(d.inputChannel)
			outputCh, _ := c.getChannel(d.outputChannel)

			if err := d.fn(c.ctx, inputCh, outputCh); err != nil {
				select {
				case c.errChan <- fmt.Errorf("decorator error: %w", err):
				default:
				}
				c.cancel()
			}
		}(d)
	}

	for _, m := range c.multiplexers {
		c.wg.Add(1)
		go func(m multiplexerConfig) {
			defer c.wg.Done()

			inputs := make([]chan string, len(m.inputChannels))
			for i, name := range m.inputChannels {
				ch, _ := c.getChannel(name)
				inputs[i] = ch
			}
			outputCh, _ := c.getChannel(m.outputChannel)

			if err := m.fn(c.ctx, inputs, outputCh); err != nil {
				select {
				case c.errChan <- fmt.Errorf("multiplexer error: %w", err):
				default:
				}
				c.cancel()
			}
		}(m)
	}

	for _, s := range c.separators {
		c.wg.Add(1)
		go func(s separatorConfig) {
			defer c.wg.Done()

			inputCh, _ := c.getChannel(s.inputChannel)
			outputs := make([]chan string, len(s.outputChannels))
			for i, name := range s.outputChannels {
				ch, _ := c.getChannel(name)
				outputs[i] = ch
			}

			if err := s.fn(c.ctx, inputCh, outputs); err != nil {
				select {
				case c.errChan <- fmt.Errorf("separator error: %w", err):
				default:
				}
				c.cancel()
			}
		}(s)
	}

	go func() {
		c.wg.Wait()
		c.stop()
	}()
}

func (c *conveyerImpl) stop() {
	if c.cancel != nil {
		c.cancel()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for name, ch := range c.channels {
		select {
		case _, ok := <-ch:
			if ok {
				close(ch)
			}
		default:
			close(ch)
		}
		delete(c.channels, name)
	}
}

func (c *conveyerImpl) Send(input string, data string) error {
	if !c.started {
		return errors.New("conveyer not started")
	}

	ch, exists := c.getChannel(input)
	if !exists {
		return errors.New("chan not found")
	}

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case ch <- data:
		return nil
	default:
		return errors.New("channel buffer full")
	}
}

func (c *conveyerImpl) Recv(output string) (string, error) {
	if !c.started {
		return "", errors.New("conveyer not started")
	}

	ch, exists := c.getChannel(output)
	if !exists {
		return "", errors.New("chan not found")
	}

	select {
	case <-c.ctx.Done():
		return "", c.ctx.Err()
	case data, ok := <-ch:
		if !ok {
			return "undefined", nil
		}
		return data, nil
	}
}
