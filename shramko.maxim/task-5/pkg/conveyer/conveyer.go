package conveyer

import (
	"context"
	"errors"
	"sync"
)

var ErrChannelNotFound = errors.New("chan not found")

type Pipeline struct {
	capacity    int
	chanStorage map[string]chan string
	tasks       []func(context.Context) error
	lock        sync.RWMutex
}

func New(capacity int) *Pipeline {
	return &Pipeline{
		capacity:    capacity,
		chanStorage: make(map[string]chan string),
		tasks:       make([]func(context.Context) error, 0),
		lock:        sync.RWMutex{},
	}
}

func (p *Pipeline) getChannel(id string) chan string {
	p.lock.Lock()
	defer p.lock.Unlock()

	if ch, found := p.chanStorage[id]; found {
		return ch
	}

	ch := make(chan string, p.capacity)
	p.chanStorage[id] = ch
	return ch
}

func (p *Pipeline) RegisterDecorator(
	decorator func(context.Context, chan string, chan string) error,
	inputID, outputID string,
) {
	p.lock.Lock()
	defer p.lock.Unlock()

	inCh := p.getChannel(inputID)
	outCh := p.getChannel(outputID)

	p.tasks = append(p.tasks, func(ctx context.Context) error {
		return decorator(ctx, inCh, outCh)
	})
}

func (p *Pipeline) RegisterMultiplexer(
	multiplexer func(context.Context, []chan string, chan string) error,
	inputIDs []string, outputID string,
) {
	p.lock.Lock()
	defer p.lock.Unlock()

	inputs := make([]chan string, len(inputIDs))
	for i, id := range inputIDs {
		inputs[i] = p.getChannel(id)
	}

	outputCh := p.getChannel(outputID)

	p.tasks = append(p.tasks, func(ctx context.Context) error {
		return multiplexer(ctx, inputs, outputCh)
	})
}

func (p *Pipeline) RegisterSeparator(
	separator func(context.Context, chan string, []chan string) error,
	inputID string, outputIDs []string,
) {
	p.lock.Lock()
	defer p.lock.Unlock()

	inputCh := p.getChannel(inputID)
	outputs := make([]chan string, len(outputIDs))

	for i, id := range outputIDs {
		outputs[i] = p.getChannel(id)
	}

	p.tasks = append(p.tasks, func(ctx context.Context) error {
		return separator(ctx, inputCh, outputs)
	})
}

func (p *Pipeline) Run(ctx context.Context) error {
	errChan := make(chan error, len(p.tasks))
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, task := range p.tasks {
		wg.Add(1)
		go func(t func(context.Context) error) {
			defer wg.Done()
			if err := t(ctx); err != nil {
				select {
				case errChan <- err:
				default:
				}
			}
		}(task)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			cancel()
			return err
		}
	}

	return nil
}

func (p *Pipeline) Send(channelID string, data string) error {
	p.lock.RLock()
	ch, exists := p.chanStorage[channelID]
	p.lock.RUnlock()

	if !exists {
		return ErrChannelNotFound
	}

	ch <- data
	return nil
}

func (p *Pipeline) Recv(channelID string) (string, error) {
	p.lock.RLock()
	ch, exists := p.chanStorage[channelID]
	p.lock.RUnlock()

	if !exists {
		return "", ErrChannelNotFound
	}

	value, ok := <-ch
	if !ok {
		return "undefined", nil
	}

	return value, nil
}

type conveyer interface {
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
