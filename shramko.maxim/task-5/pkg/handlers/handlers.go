package handlers

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
)

var (
	ErrCannotDecorate = errors.New("can't be decorated")
)

func PrefixDecoratorFunc(ctx context.Context, input chan string, output chan string) error {
	defer func() {
		close(output)
	}()

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
	defer func() {
		for _, out := range outputs {
			close(out)
		}
	}()

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
	defer func() {
		close(output)
	}()

	results := make(chan string, 100)
	errChan := make(chan error, 1)

	for i, input := range inputs {
		go func(idx int, in chan string) {
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
		}(i, input)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-results:
			if !ok {
				allClosed := true
				for _, in := range inputs {
					select {
					case _, stillOpen := <-in:
						if stillOpen {
							allClosed = false
						}
					default:
						allClosed = false
					}
				}

				if allClosed {
					return nil
				}
			}

			select {
			case output <- data:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
