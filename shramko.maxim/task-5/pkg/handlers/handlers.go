package handlers

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrCannotDecorate = errors.New("can't be decorated")
)

func PrefixDecoratorFunc(ctx context.Context, input chan string, output chan string) error {
	const prefix = "decorated: "

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-input:
			if !ok {
				return nil
			}

			if strings.Contains(data, "no decorator") {
				return ErrCannotDecorate
			}

			result := data
			if !strings.HasPrefix(data, prefix) {
				result = prefix + data
			}

			select {
			case output <- result:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func SeparatorFunc(ctx context.Context, input chan string, outputs []chan string) error {
	var counter int64 = 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-input:
			if !ok {
				return nil
			}

			idx := atomic.AddInt64(&counter, 1) - 1
			channelIdx := int(idx) % len(outputs)

			select {
			case outputs[channelIdx] <- data:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func MultiplexerFunc(ctx context.Context, inputs []chan string, output chan string) error {
	results := make(chan string, 100)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, input := range inputs {
		wg.Add(1)
		go func(in chan string) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case data, ok := <-in:
					if !ok {
						return
					}

					if strings.Contains(data, "no multiplexer") {
						continue
					}

					select {
					case results <- data:
					case <-ctx.Done():
						return
					}
				}
			}
		}(input)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-results:
			if !ok {
				return nil
			}

			select {
			case output <- data:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
